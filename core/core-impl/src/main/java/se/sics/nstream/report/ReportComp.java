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

import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.gvod.mngr.util.TorrentExtendedStatus;
import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.Handler;
import se.sics.kompics.Negative;
import se.sics.kompics.Positive;
import se.sics.kompics.Start;
import se.sics.kompics.timer.CancelPeriodicTimeout;
import se.sics.kompics.timer.SchedulePeriodicTimeout;
import se.sics.kompics.timer.Timeout;
import se.sics.kompics.timer.Timer;
import se.sics.ktoolbox.util.identifiable.Identifier;
import se.sics.ktoolbox.util.identifiable.basic.UUIDIdentifier;
import se.sics.nstream.StreamEvent;
import se.sics.nstream.library.util.TorrentStatus;
import se.sics.nstream.report.event.DownloadSummaryEvent;
import se.sics.nstream.report.event.StatusSummaryEvent;
import se.sics.nstream.report.event.TransferStatus;
import se.sics.nstream.torrent.TransferSpeed;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class ReportComp extends ComponentDefinition {

    private static final Logger LOG = LoggerFactory.getLogger(ReportComp.class);
    private String logPrefix = "";

    Positive<Timer> timerPort = requires(Timer.class);
    Positive<TransferStatusPort> torrentPort = requires(TransferStatusPort.class);
    Negative<ReportPort> reportPort = provides(ReportPort.class);

    private final Identifier torrentId;
    private final long reportDelay;
    private static final int pieceSize = 1024;
    
    private TorrentStatus status = TorrentStatus.UPLOADING;
    private long startingTime;
    private long transferSize = 0;
    private TransferSpeed transferSpeed;
    private double percentageCompleted = 100;

    private UUID reportTId;

    public ReportComp(Init init) {
        LOG.info("{}initiating...", logPrefix);

        torrentId = init.torrentId;
        reportDelay = init.reportDelay;

        subscribe(handleStart, control);
        subscribe(handleDownloadStarting, torrentPort);
        subscribe(handleDownloadDone, torrentPort);
        subscribe(handleTransferStatusResponse, torrentPort);
        
        subscribe(handleReport, timerPort);
        
        subscribe(handleStatusSummaryRequest, reportPort);
    }

    Handler handleStart = new Handler<Start>() {
        @Override
        public void handle(Start event) {
            LOG.info("{}starting...", logPrefix);
            scheduleReport(torrentId);
        }
    };
    
    @Override
    public void tearDown() {
        LOG.warn("{}tearing down", logPrefix);
    }

    Handler handleDownloadStarting = new Handler<TransferStatus.DownloadStarting>() {
        @Override
        public void handle(TransferStatus.DownloadStarting event) {
            LOG.info("{}download:{} starting", logPrefix, event.overlayId);
            startingTime = System.currentTimeMillis();
            status = TorrentStatus.DOWNLOADING;
            percentageCompleted = 0;
        }
    };

    Handler handleDownloadDone = new Handler<TransferStatus.DownloadDone>() {
        @Override
        public void handle(TransferStatus.DownloadDone event) {
            LOG.info("{}download:{} done", logPrefix, event.overlayId);
            long transferTime = System.currentTimeMillis() - startingTime;
            LOG.info("{}download completed in:{} avg dwnl speed:{} KB/s", new Object[]{logPrefix, transferTime, (double)transferSize/transferTime});
            trigger(new DownloadSummaryEvent(torrentId, transferSize, transferTime), reportPort);
            transferSpeed = transferSpeed.resetDownloadSpeed();
            status = TorrentStatus.UPLOADING;
            percentageCompleted = 100;
        }
    };

    Handler handleTransferStatusResponse = new Handler<TransferStatus.Response>() {
        @Override
        public void handle(TransferStatus.Response resp) {
            transferSpeed = resp.transferSpeed;
            percentageCompleted = resp.percentageCompleted;
            LOG.info("{}report pc:{} ds:{} us:{}", new Object[]{logPrefix, percentageCompleted, transferSpeed.totalDownloadSpeed, transferSpeed.totalUploadSpeed});
        }
    };
    
    Handler handleReport = new Handler<ReportTimeout>() {
        @Override
        public void handle(ReportTimeout event) {
            trigger(new TransferStatus.Request(event.overlayId), torrentPort);
        }
    };
    
    Handler handleStatusSummaryRequest = new Handler<StatusSummaryEvent.Request>() {
        @Override
        public void handle(StatusSummaryEvent.Request req) {
            LOG.trace("{}received:{}", logPrefix, req);
            answer(req, req.success(new TorrentExtendedStatus(torrentId, status, transferSpeed, percentageCompleted)));
        }
    };

    public static class Init extends se.sics.kompics.Init<ReportComp> {
        public final Identifier torrentId;
        public final long reportDelay;

        public Init(Identifier torrentId, long reportDelay) {
            this.torrentId = torrentId;
            this.reportDelay = reportDelay;
        }
    }

    public void scheduleReport(Identifier overlayId) {
        SchedulePeriodicTimeout st = new SchedulePeriodicTimeout(reportDelay, reportDelay);
        Timeout t = new ReportTimeout(st, overlayId);
        st.setTimeoutEvent(t);
        trigger(st, timerPort);
        reportTId = t.getTimeoutId();
    }

    public void cancelReport() {
        CancelPeriodicTimeout ct = new CancelPeriodicTimeout(reportTId);
        trigger(ct, timerPort);
        reportTId = null;
    }

    public static class ReportTimeout extends Timeout implements StreamEvent {

        public final Identifier overlayId;

        public ReportTimeout(SchedulePeriodicTimeout st, Identifier overlayId) {
            super(st);
            this.overlayId = overlayId;
        }

        @Override
        public Identifier getId() {
            return new UUIDIdentifier(getTimeoutId());
        }

        @Override
        public String toString() {
            return "Report<" + overlayId + ">Timeout<" + getId() + ">";
        }
    }
}
