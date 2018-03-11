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

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.TreeMap;
import org.javatuples.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.Handler;
import se.sics.kompics.Negative;
import se.sics.kompics.Positive;
import se.sics.kompics.Start;
import se.sics.kompics.timer.Timer;
import se.sics.ktoolbox.util.identifiable.overlay.OverlayId;
import se.sics.ktoolbox.util.network.KAddress;
import se.sics.nstream.FileId;
import se.sics.nstream.library.util.TorrentState;
import se.sics.nstream.torrent.core.DataReport;
import se.sics.nstream.torrent.status.event.DownloadSummaryEvent;
import se.sics.nstream.torrent.status.event.TorrentStatus;
import se.sics.nstream.torrent.tracking.event.StatusSummaryEvent;
import se.sics.nstream.torrent.tracking.event.TorrentTracking;
import se.sics.nstream.torrent.tracking.tracker.ClientWrapper;
import se.sics.nstream.torrent.tracking.tracker.DataReportDTO;
import se.sics.nstream.torrent.tracking.tracker.DelaReportDTO;
import se.sics.nstream.torrent.tracking.tracker.DownloadReportDTO;
import se.sics.nstream.torrent.tracking.tracker.Hopssite;
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
  private final Long reportTId;
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
  //**************************************************************************
  private TorrentTracking.DownloadedManifest pendingAdvance;

  public TorrentTrackingComp(Init init) {
    selfAdr = init.selfAdr;
    torrentId = init.torrentId;
    reportTId = System.currentTimeMillis();
    logPrefix = "<tid:" + torrentId.toString() + ">";
    LOG.debug("{}initiating...", logPrefix);

    reportDelay = init.reportDelay;

    reportConfig = new TorrentTrackingConfig(config());
    LOG.info("reporting in:{} and to:{}", new Object[]{reportConfig.reportDir, reportConfig.reportTracker});
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
    }

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
      trigger(new TorrentStatus.DownloadedManifest(req), statusPort);
    }
  };

  Handler handleDownloadStarting = new Handler<TorrentTracking.TransferSetUp>() {
    @Override
    public void handle(TorrentTracking.TransferSetUp event) {
      LOG.info("{}download:{} starting", logPrefix, torrentId);
      startingTime = System.currentTimeMillis();
      status = TorrentState.DOWNLOADING;
      dataReport = event.dataReport;
//            dataHeader(dataReport.fileNames);
    }
  };

  Handler handleDownloadDone = new Handler<TorrentTracking.DownloadDone>() {
    @Override
    public void handle(TorrentTracking.DownloadDone event) {
      LOG.info("{}download:{} done", logPrefix, event.overlayId);
      long transferTime = System.currentTimeMillis() - startingTime;
      dataReport = event.dataReport;
      LOG.info("{}download completed in:{} avg dwnl speed:{} B/s", new Object[]{logPrefix, transferTime,
        (double) dataReport.totalSize.getValue1() / transferTime});
      trigger(new DownloadSummaryEvent(torrentId, dataReport.totalSize.getValue1(), transferTime), statusPort);
      status = TorrentState.UPLOADING;
      if (writeToFile) {
        try {
          dataFile.close();
          dataFile = null;
          downloadFile.close();
          downloadFile = null;
        } catch (IOException ex) {
          throw new RuntimeException("file report error");
        }
        writeToFile = false;
      }
      writeToTracker = false;
    }
  };

  Handler handleTorrentTrackingIndication = new Handler<TorrentTracking.Indication>() {
    @Override
    public void handle(TorrentTracking.Indication resp) {
      LOG.debug("{}reporting", logPrefix);
      dataReport = resp.dataReport;
      downloadReport = resp.downloadReport;
      reportLine++;
      DataReportDTO dataValues = dataValues(dataReport);
      DownloadReportDTO downloadValues = downloadValues(downloadReport);
      if (writeToFile) {
        fileWrite(dataFile, dataValues.toString());
        fileWrite(downloadFile, downloadValues.toString());
      }
      if (writeToTracker) {
        trackerDataValues(dataValues);
        trackerDownloadValues(downloadValues);
      }
    }
  };

  private void fileWrite(BufferedWriter writer, String report) {
    try {
      writer.write(report);
      writer.write("\n");
    } catch (IOException ex) {
      throw new RuntimeException("file report error");
    }
  }

  private String trackerDataValues(DataReportDTO reportVal) {
    ClientWrapper client = ClientWrapper.httpInstance(String.class)
      .setTarget(reportConfig.reportTracker)
      .setPath(Hopssite.dataValues());
    DelaReportDTO report = new DelaReportDTO(selfAdr.getId().toString(), torrentId.baseId.toString(),
      reportTId.toString(), reportVal.toJson());
    client.setPayload(report);
    try {
      return (String) client.doPost();
    } catch (IllegalStateException ex) {
      return "fail";
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  private String trackerDownloadValues(DownloadReportDTO reportVal) {
    ClientWrapper client = ClientWrapper.httpInstance(String.class)
      .setTarget(reportConfig.reportTracker)
      .setPath(Hopssite.downloadValues());
    DelaReportDTO report = new DelaReportDTO(selfAdr.getId().toString(), torrentId.baseId.toString(),
      reportTId.toString(), reportVal.toJson());
    client.setPayload(report);
    try {
      return (String) client.doPost();
    } catch (IllegalStateException ex) {
      return "fail";
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  private void dataHeader(TreeMap<Integer, String> files) {
    try {
      dataFile.write("total");
      for (String file : files.values()) {
        dataFile.write("," + file);
      }
      dataFile.write("\n");
    } catch (IOException ex) {
      throw new RuntimeException("file report error");
    }
  }

  private DataReportDTO dataValues(DataReport report) {
    DataReportDTO r = new DataReportDTO();
    r.addValue("" + reportLine);
    r.addValue("" + 100 * ((double) report.totalSize.getValue1()) / report.totalSize.getValue0());
    for (FileId fileId : report.torrent.base.keySet()) {
      if (report.pending.contains(fileId)) {
        r.addValue("0");
        continue;
      }
      if (report.completed.contains(fileId)) {
        r.addValue(",100");
        continue;
      }
      Pair<Long, Long> progress = report.ongoing.get(fileId);
      r.addValue("" + 100 * ((double) progress.getValue1()) / progress.getValue0());
    }
    return r;
  }

  private DownloadReportDTO downloadValues(DownloadReport report) {
    DownloadReportDTO r = new DownloadReportDTO();
    r.addValue("" + reportLine);
    r.addValue("" + report.total.throughput.inTimeThroughput / 1024);
    r.addValue("" + report.total.throughput.timedOutThroughput / 1024);
    r.addValue("" + report.total.ongoingBlocks);
    r.addValue("" + report.total.cwnd);
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
