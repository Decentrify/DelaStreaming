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
package se.sics.nstream.report;

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
import se.sics.gvod.mngr.util.TorrentExtendedStatus;
import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.Handler;
import se.sics.kompics.Negative;
import se.sics.kompics.Positive;
import se.sics.kompics.Start;
import se.sics.kompics.timer.Timer;
import se.sics.ktoolbox.util.identifiable.overlay.OverlayId;
import se.sics.nstream.library.util.TorrentStatus;
import se.sics.nstream.report.event.DownloadSummaryEvent;
import se.sics.nstream.report.event.StatusSummaryEvent;
import se.sics.nstream.report.event.TorrentTracking;
import se.sics.nstream.torrent.DataReport;
import se.sics.nstream.torrent.transferTracking.DownloadReport;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class ReportComp extends ComponentDefinition {

    private static final Logger LOG = LoggerFactory.getLogger(ReportComp.class);
    private String logPrefix = "";

    Positive<Timer> timerPort = requires(Timer.class);
    Positive<TransferStatusPort> torrentPort = requires(TransferStatusPort.class);
    Negative<ReportPort> reportPort = provides(ReportPort.class);

    private final OverlayId torrentId;
    private final long reportDelay;
    private static final int pieceSize = 1024;

    private TorrentStatus status = TorrentStatus.UPLOADING;
    private long startingTime;
    private DataReport dataReport;
    private DownloadReport downloadReport;
    //**************************************************************************
    private final ReportConfig reportConfig;
    private BufferedWriter dataFile;
    private BufferedWriter downloadFile;
    private int fileCounter = 0;

    public ReportComp(Init init) {
        LOG.info("{}initiating...", logPrefix);

        torrentId = init.torrentId;
        reportDelay = init.reportDelay;

        reportConfig = new ReportConfig(config());
        if (reportConfig.reportDir == null) {
            fileCounter = -1;
        } else {
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
                fileCounter = 1000 * 1000;
            } catch (FileNotFoundException ex) {
                throw new RuntimeException("file report error");
            } catch (IOException ex) {
                throw new RuntimeException("file report error");
            }
        }

        subscribe(handleStart, control);
        subscribe(handleDownloadStarting, torrentPort);
        subscribe(handleDownloadDone, torrentPort);
        subscribe(handleTorrentTrackingIndication, torrentPort);
        subscribe(handleStatusSummaryRequest, reportPort);
    }

    Handler handleStart = new Handler<Start>() {
        @Override
        public void handle(Start event) {
            LOG.info("{}starting...", logPrefix);
        }
    };

    @Override
    public void tearDown() {
        LOG.warn("{}tearing down", logPrefix);
        if (reportConfig.reportDir != null) {
            try {
                dataFile.close();
                downloadFile.close();
            } catch (IOException ex) {
                throw new RuntimeException("file report error");
            }
        }
    }

    Handler handleDownloadStarting = new Handler<TorrentTracking.DownloadStarting>() {
        @Override
        public void handle(TorrentTracking.DownloadStarting event) {
            LOG.info("{}download:{} starting", logPrefix, event.overlayId);
            startingTime = System.currentTimeMillis();
            status = TorrentStatus.DOWNLOADING;
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
            LOG.info("{}download completed in:{} avg dwnl speed:{} B/s", new Object[]{logPrefix, transferTime, (double) dataReport.totalSize.getValue1() / transferTime});
            trigger(new DownloadSummaryEvent(torrentId, dataReport.totalSize.getValue1(), transferTime), reportPort);
            status = TorrentStatus.UPLOADING;
            fileCounter = 0;
        }
    };

    Handler handleTorrentTrackingIndication = new Handler<TorrentTracking.Indication>() {
        @Override
        public void handle(TorrentTracking.Indication resp) {
            LOG.info("{}reporting", logPrefix);
            dataReport = resp.dataReport;
            downloadReport = resp.downloadReport;
            if (fileCounter > 0) {
                dataValues(resp.dataReport);
                downloadValues(resp.downloadReport);
                fileCounter--;
            }
            if (fileCounter == 0) {
                try {
                    dataFile.close();
                    downloadFile.close();
                } catch (IOException ex) {
                    throw new RuntimeException("file report error");
                }
                fileCounter--;
            }
        }
    };

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

    private void dataValues(DataReport report) {
        try {
            dataFile.write("" + 100 * ((double) report.totalSize.getValue1()) / report.totalSize.getValue0());
            for (Integer fileId : report.fileNames.keySet()) {
                if (report.pending.contains(fileId)) {
                    dataFile.write(",0");
                    continue;
                }
                if (report.completed.contains(fileId)) {
                    dataFile.write(",100");
                    continue;
                }
                Pair<Long, Long> progress = report.ongoing.get(fileId);
                dataFile.write("," + 100 * ((double) progress.getValue1()) / progress.getValue0());
            }
            dataFile.write("\n");
        } catch (IOException ex) {
            throw new RuntimeException("file report error");
        }
    }

    private void downloadValues(DownloadReport report) {
        try {
            downloadFile.write(report.total.throughput.inTimeThroughput / 1024 + "," + report.total.throughput.timedOutThroughput / 1024 + ","
                    + report.total.ongoingBlocks + "," + report.total.cwnd +  "\n");
        } catch (IOException ex) {
            throw new RuntimeException("file report error");
        }
    }

    Handler handleStatusSummaryRequest = new Handler<StatusSummaryEvent.Request>() {
        @Override
        public void handle(StatusSummaryEvent.Request req) {
            LOG.trace("{}received:{}", logPrefix, req);
            double percentageCompleted = 100 * ((double) dataReport.totalSize.getValue1()) / dataReport.totalSize.getValue0();
            answer(req, req.success(new TorrentExtendedStatus(torrentId, status, downloadReport.total.throughput.inTimeThroughput, percentageCompleted)));
        }
    };

    public static class Init extends se.sics.kompics.Init<ReportComp> {

        public final OverlayId torrentId;
        public final long reportDelay;

        public Init(OverlayId torrentId, long reportDelay) {
            this.torrentId = torrentId;
            this.reportDelay = reportDelay;
        }
    }
}
