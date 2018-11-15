/*
 * Copyright (C) 2009 Swedish Institute of Computer Science (SICS) Copyright (C)
 * 2009 Royal Institute of Technology (KTH)
 *
 * GVoD is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
 */
package se.sics.nstream.torrent.tracking;

import com.google.gson.Gson;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.Optional;
import java.util.TreeMap;
import javax.ws.rs.ProcessingException;
import javax.ws.rs.core.MediaType;
import org.javatuples.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.Handler;
import se.sics.kompics.Negative;
import se.sics.kompics.Positive;
import se.sics.kompics.Start;
import se.sics.kompics.timer.Timer;
import se.sics.kompics.util.Identifier;
import se.sics.ktoolbox.util.identifiable.BasicIdentifiers;
import se.sics.ktoolbox.util.identifiable.IdentifierFactory;
import se.sics.ktoolbox.util.identifiable.IdentifierRegistryV2;
import se.sics.ktoolbox.webclient.AsyncWebResponse;
import se.sics.ktoolbox.webclient.WebClient;
import se.sics.ktoolbox.util.identifiable.overlay.OverlayId;
import se.sics.ktoolbox.util.network.KAddress;
import se.sics.nstream.FileId;
import se.sics.nstream.library.util.TorrentState;
import se.sics.nstream.torrent.core.DataReport;
import se.sics.nstream.torrent.status.event.DownloadSummaryEvent;
import se.sics.nstream.torrent.status.event.TorrentStatus;
import se.sics.nstream.torrent.tracking.event.StatusSummaryEvent;
import se.sics.nstream.torrent.tracking.event.TorrentTracking;
import se.sics.nstream.torrent.tracking.tracker.DelaReportDTO;
import se.sics.nstream.torrent.tracking.tracker.Hopssite;
import se.sics.nstream.torrent.tracking.tracker.ReportDTO;
import se.sics.nstream.torrent.transfer.tracking.DownloadReport;
import se.sics.nstream.util.TorrentExtendedStatus;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class TorrentTrackingComp extends ComponentDefinition {

  private static final Logger LOG = LoggerFactory.getLogger(TorrentTrackingComp.class);
  private String logPrefix = "";

  //**************************************************************************
  Positive<Timer> timerPort = requires(Timer.class);
  Positive<TorrentTrackingPort> trackingPort = requires(TorrentTrackingPort.class);
  Negative<TorrentStatusPort> statusPort = provides(TorrentStatusPort.class);
  //**************************************************************************
  private final KAddress selfAdr;
  private final OverlayId torrentId;
  private int reportLine = 0;
  private final long reportDelay;

  private TorrentState status = TorrentState.UPLOADING;
  private long startingTime;
  private DataReport dataReport;
  private DownloadReport downloadReport;
  //**************************************************************************
  private final TorrentTrackingConfig reportConfig;
  private BufferedWriter dataFile;
  private BufferedWriter downloadFile;
  private boolean writeToFile = false;
  private boolean writeToTracker = false;
  private Tracker tracker;
  //**************************************************************************
  private TorrentTracking.DownloadedManifest pendingAdvance;
  private final TreeMap<FileId, String> reportOrder = new TreeMap<>();

  private final IdentifierFactory eventIds;
  
  public TorrentTrackingComp(Init init) {
    selfAdr = init.selfAdr;
    torrentId = init.torrentId;
    logPrefix = "<tid:" + torrentId.toString() + ">";
    LOG.debug("{}initiating...", logPrefix);

    reportDelay = init.reportDelay;

    reportConfig = new TorrentTrackingConfig(config());
    LOG.info("reporting in:{} and/or to:{}", new Object[]{reportConfig.reportDir, reportConfig.reportTracker});
    this.eventIds = IdentifierRegistryV2.instance(BasicIdentifiers.Values.EVENT, Optional.empty());
    subscribe(handleStart, control);
    subscribe(handleDownloadedManifest, trackingPort);
    subscribe(handleDownloadStarting, trackingPort);
    subscribe(handleDownloadDone, trackingPort);
    subscribe(handleTorrentTrackingIndication, trackingPort);

    subscribe(handleStatusSummaryRequest, statusPort);
  }

  Handler handleStart = new Handler<Start>() {
    @Override
    public void handle(Start event) {
      LOG.debug("{}starting...", logPrefix);
    }
  };

  @Override
  public void tearDown() {
    LOG.warn("{}tearing down", logPrefix);
    if (dataFile != null && downloadFile != null) {
      try {
        dataFile.close();
        dataFile = null;
        downloadFile.close();
        downloadFile = null;
      } catch (IOException ex) {
        throw new RuntimeException("file report error");
      }
    }
  }

  Handler handleDownloadedManifest = new Handler<TorrentTracking.DownloadedManifest>() {
    @Override
    public void handle(TorrentTracking.DownloadedManifest req) {
      LOG.info("{}download - got manifest", logPrefix);
      pendingAdvance = req;
      trigger(new TorrentStatus.DownloadedManifest(eventIds.randomId(), req), statusPort);
    }
  };

  Handler handleDownloadStarting = new Handler<TorrentTracking.TransferSetUp>() {
    @Override
    public void handle(TorrentTracking.TransferSetUp event) {
      LOG.info("{}download:{} starting", logPrefix, torrentId);
      startingTime = System.currentTimeMillis();
      status = TorrentState.DOWNLOADING;
      dataReport = event.dataReport;
      reportOrder(event.dataReport);
      prepareReportAction();
      writeHeaders();
    }
  };

  Handler handleDownloadDone = new Handler<TorrentTracking.DownloadDone>() {
    @Override
    public void handle(TorrentTracking.DownloadDone event) {
      LOG.info("{}download:{} done", logPrefix, event.overlayId);
      long transferTime = (System.currentTimeMillis() - startingTime) / 1000;
      dataReport = event.dataReport;
      LOG.info("{}download completed in:{} avg dwnl speed:{} B/s", new Object[]{logPrefix, transferTime,
        (double) dataReport.totalSize.getValue1() / transferTime});
      trigger(new DownloadSummaryEvent(eventIds.randomId(), torrentId, dataReport.totalSize.getValue1(), transferTime), statusPort);
      status = TorrentState.UPLOADING;
      finalizeReportAction(transferTime);
    }
  };

  Handler handleTorrentTrackingIndication = new Handler<TorrentTracking.Indication>() {
    @Override
    public void handle(TorrentTracking.Indication resp) {
      dataReport = resp.dataReport;
      downloadReport = resp.downloadReport;
      if (TorrentState.DOWNLOADING.equals(status)) {
        reportLine++;
        writeValues();
      }
    }
  };

  private void prepareReportAction() {
    if (reportConfig.reportDir != null) {
      try {
        File tf = new File(reportConfig.reportDir + File.separator + torrentId.toString() + ".data.csv");
        if (tf.exists()) {
          tf.delete();
          tf.createNewFile();
        }
        dataFile = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(tf)));
        File lf = new File(reportConfig.reportDir + File.separator + torrentId.toString() + ".download.csv");
        if (lf.exists()) {
          lf.delete();
          lf.createNewFile();
        }
        downloadFile = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(lf)));
        writeToFile = true;
      } catch (FileNotFoundException ex) {
        throw new RuntimeException("file report error");
      } catch (IOException ex) {
        throw new RuntimeException("file report error");
      }
    }
    if (reportConfig.reportTracker != null) {
      writeToTracker = true;
      tracker = new Tracker(selfAdr.getId(), torrentId, reportConfig);
    }
  }

  private void finalizeReportAction(long transferTime) {
    ReportDTO dataValues = dataValues(dataReport);
    if (writeToFile) {
      try {
        fileWrite(dataFile, dataValues.toString());

        dataFile.close();
        dataFile = null;
        downloadFile.close();
        downloadFile = null;
      } catch (IOException ex) {
        throw new RuntimeException("file report error");
      }
    }
    if (writeToTracker) {
      ReportDTO transferValues = transferValues(transferTime, dataReport);
      tracker.trackerDataValues(dataValues.toString());
      tracker.trackerTransferValues(transferValues.toString());
    }
    writeToFile = false;
    writeToTracker = false;
  }

  private void writeHeaders() {
    ReportDTO dataHeader = dataHeader();
    ReportDTO downloadHeader = downloadHeader();
    if (writeToFile) {
      fileWrite(dataFile, dataHeader.toString());
      fileWrite(downloadFile, downloadHeader.toString());
    }
    if (writeToTracker) {
      tracker.trackerDataValues(dataHeader.toString());
      tracker.trackerDownloadValues(downloadHeader.toString());
    }
  }

  private void writeValues() {
    ReportDTO dataValues = dataValues(dataReport);
    ReportDTO downloadValues = downloadValues(downloadReport);
    if (writeToFile) {
      fileWrite(dataFile, dataValues.toString());
      fileWrite(downloadFile, downloadValues.toString());
    }
    if (writeToTracker) {
      tracker.trackerDataValues(dataValues.toString());
      tracker.trackerDownloadValues(downloadValues.toString());
    }
  }

  private void fileWrite(BufferedWriter writer, String report) {
    try {
      writer.write(report);
      writer.write("\n");
    } catch (IOException ex) {
      throw new RuntimeException("file report error");
    }
  }

  private void reportOrder(DataReport report) {
    report.torrent.nameToId.entrySet().stream().forEach((e) -> {
      reportOrder.put(e.getValue(), e.getKey());
    });
  }

  private static class Tracker {

    private final Identifier selfId;
    private final OverlayId torrentId;
    private final long reportId;
    private final TorrentTrackingConfig reportConfig;
    //SSLHandshakeException - thrown the very first time a new version of dela starts;
    private boolean sslHandshakeFixed = false;

    public Tracker(Identifier selfId, OverlayId torrentId, TorrentTrackingConfig reportConfig) {
      this.selfId = selfId;
      this.torrentId = torrentId;
      this.reportId = System.currentTimeMillis();
      this.reportConfig = reportConfig;
    }

    public void trackerDataValues(String reportVal) {
      DelaReportDTO report = new DelaReportDTO(selfId.toString(), torrentId.baseId.toString(),
        reportId, reportVal);
      LOG.debug("reporting to:{}/{} with data:{}", 
        new Object[]{reportConfig.reportTracker, Hopssite.dataValues(), new Gson().toJson(report)});
      try (WebClient client = WebClient.httpsInstance()) {
        AsyncWebResponse webResp = client
          .setTarget(reportConfig.reportTracker)
          .setPath(Hopssite.dataValues())
          .setPayload(report, MediaType.APPLICATION_JSON_TYPE)
          .doAsyncPost();
        //eventually maybe retry failures - check future
      } catch (ProcessingException ex) {
        if (!sslHandshakeFixed) {
          sslHandshakeFixed = true;
          trackerDataValues(reportVal);
        } else {
          LOG.warn("problem reporting to tracker:{}", ex);
        }
      } catch (Exception ex) {
        //not much to do now
        LOG.warn("problem reporting to tracker:{}", ex);
      }
    }

    public void trackerDownloadValues(String reportVal) {

      DelaReportDTO report = new DelaReportDTO(selfId.toString(), torrentId.baseId.toString(),
        reportId, reportVal);
      LOG.debug("reporting to:{}/{} with download:{}", 
        new Object[]{reportConfig.reportTracker, Hopssite.dataValues(), new Gson().toJson(report)});
      try (WebClient client = WebClient.httpsInstance()) {
        AsyncWebResponse webResp = client
          .setTarget(reportConfig.reportTracker)
          .setPath(Hopssite.downloadValues())
          .setPayload(report, MediaType.APPLICATION_JSON_TYPE)
          .doAsyncPost();
        //eventually maybe retry failures - check future
      } catch (ProcessingException ex) {
        if (!sslHandshakeFixed) {
          sslHandshakeFixed = true;
          trackerDownloadValues(reportVal);
        }
      } catch (Exception ex) {
        //not much to do now
      }
    }

    public void trackerTransferValues(String reportVal) {

      DelaReportDTO report = new DelaReportDTO(selfId.toString(), torrentId.baseId.toString(),
        reportId, reportVal);
      LOG.debug("reporting to:{}/{} with transfer:{}", 
        new Object[]{reportConfig.reportTracker, Hopssite.dataValues(), new Gson().toJson(report)});
      try (WebClient client = WebClient.httpsInstance()) {
        AsyncWebResponse webResp = client
          .setTarget(reportConfig.reportTracker)
          .setPath(Hopssite.transferValues())
          .setPayload(report, MediaType.APPLICATION_JSON_TYPE)
          .doAsyncPost();
        //eventually maybe retry failures - check future
      } catch (ProcessingException ex) {
        if (!sslHandshakeFixed) {
          sslHandshakeFixed = true;
          trackerTransferValues(reportVal);
        }
      } catch (Exception ex) {
        //not much to do now
      }
    }
  }

  private ReportDTO dataHeader() {
    ReportDTO r = new ReportDTO();
    r.addValue("lineNr");
    r.addValue("dataset_percentage");
    r.addValue("dataset_currentSize");
    reportOrder.keySet().stream().forEach((e) -> {
      r.addValue(reportOrder.get(e) + "_percentage");
      r.addValue(reportOrder.get(e) + "_currentSize");
    });
    return r;
  }

  private ReportDTO dataValues(DataReport report) {
    ReportDTO r = new ReportDTO();
    r.addValue("" + reportLine);
    r.addValue("" + 100 * ((double) report.totalSize.getValue1()) / report.totalSize.getValue0());
    r.addValue("" + report.totalSize.getValue1());
    reportOrder.keySet().stream().forEach((fileId) -> {
      if (report.pending.contains(fileId)) {
        r.addValue("0");
        r.addValue("0");
      } else if (report.completed.containsKey(fileId)) {
        r.addValue("100");
        r.addValue("" + report.completed.get(fileId));
      } else {
        Pair<Long, Long> progress = report.ongoing.get(fileId);
        r.addValue("" + 100 * ((double) progress.getValue1()) / progress.getValue0());
        r.addValue("" + progress.getValue1());
      }
    });
    return r;
  }

  private ReportDTO downloadHeader() {
    ReportDTO r = new ReportDTO();
    r.addValue("lineNr");
    r.addValue("inTimeThroughput");
    r.addValue("timedOutThroughput");
    r.addValue("ongoingBlocks");
    r.addValue("cwnd");
    return r;
  }

  private ReportDTO downloadValues(DownloadReport report) {
    ReportDTO r = new ReportDTO();
    r.addValue("" + reportLine);
    r.addValue("" + report.total.throughput.inTimeThroughput / 1024);
    r.addValue("" + report.total.throughput.timedOutThroughput / 1024);
    r.addValue("" + report.total.ongoingBlocks);
    r.addValue("" + report.total.cwnd);
    return r;
  }

  private ReportDTO transferValues(long transferTime, DataReport report) {
    ReportDTO r = new ReportDTO();
    if (transferTime > 1) {
      r.addValue("time:" + transferTime);
      r.addValue("size:" + dataReport.totalSize.getValue1());
      r.addValue("avg:" + (double) dataReport.totalSize.getValue1() / transferTime);
    } else {
      r.addValue("time:-");
      r.addValue("size:" + dataReport.totalSize.getValue0());
      r.addValue("avg:-");
    }
    return r;
  }

  Handler handleStatusSummaryRequest = new Handler<StatusSummaryEvent.Request>() {
    @Override
    public void handle(StatusSummaryEvent.Request req) {
      LOG.trace("{}received:{}", logPrefix, req);

      if (downloadReport == null || dataReport == null) {
        answer(req, req.success(new TorrentExtendedStatus(torrentId, status, 0, 0)));
      } else {
        double percentageCompleted = 100 * ((double) dataReport.totalSize.getValue1()) / dataReport.totalSize.
          getValue0();
        answer(req, req.success(new TorrentExtendedStatus(torrentId, status,
          downloadReport.total.throughput.inTimeThroughput, percentageCompleted)));
      }
    }
  };

  public static class Init extends se.sics.kompics.Init<TorrentTrackingComp> {

    public final KAddress selfAdr;
    public final OverlayId torrentId;
    public final long reportDelay;

    public Init(KAddress selfAdr, OverlayId torrentId, long reportDelay) {
      this.selfAdr = selfAdr;
      this.torrentId = torrentId;
      this.reportDelay = reportDelay;
    }
  }
}
