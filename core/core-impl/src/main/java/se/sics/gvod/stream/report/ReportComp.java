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
package se.sics.gvod.stream.report;

import java.util.Map;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.gvod.stream.StreamEvent;
import se.sics.gvod.stream.torrent.TorrentStatus;
import se.sics.gvod.stream.torrent.event.DownloadStatus;
import se.sics.gvod.stream.util.ConnectionStatus;
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

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class ReportComp extends ComponentDefinition {

    private static final Logger LOG = LoggerFactory.getLogger(ReportComp.class);
    private String logPrefix = "";

    Positive<Timer> timerPort = requires(Timer.class);
    Positive<TorrentStatus> torrentPort = requires(TorrentStatus.class);
    Negative<ReportPort> reportPort = provides(ReportPort.class);

    private final long reportDelay;
    
    private long startingTime;
    private long transferSize = 0;

    private UUID reportTId;

    public ReportComp(Init init) {
        LOG.info("{}initiating...", logPrefix);

        reportDelay = init.reportDelay;

        subscribe(handleStart, control);
        subscribe(handleDownloadStarting, torrentPort);
        subscribe(handleDownloadDone, torrentPort);
        subscribe(handleDownloadResponse, torrentPort);
        subscribe(handleReport, timerPort);
    }

    Handler handleStart = new Handler<Start>() {
        @Override
        public void handle(Start event) {
            LOG.info("{}starting...", logPrefix);
        }
    };

    Handler handleDownloadStarting = new Handler<DownloadStatus.Starting>() {
        @Override
        public void handle(DownloadStatus.Starting event) {
            LOG.info("{}download:{} starting", logPrefix, event.overlayId);
            scheduleReport(event.overlayId);
            startingTime = System.currentTimeMillis();
        }
    };

    Handler handleDownloadDone = new Handler<DownloadStatus.Done>() {
        @Override
        public void handle(DownloadStatus.Done event) {
            LOG.info("{}download:{} done", logPrefix, event.overlayId);
            report(event.overlayId, event.connectionStatus);
            cancelReport();
            long transferTime = System.currentTimeMillis() - startingTime;
            trigger(new SummaryEvent(transferSize, transferTime), reportPort);
        }
    };

    Handler handleDownloadResponse = new Handler<DownloadStatus.Response>() {
        @Override
        public void handle(DownloadStatus.Response event) {
            report(event.overlayId, event.connectionStatus);
        }
    };
    
    private void report(Identifier overlayId, Map<Identifier, ConnectionStatus> conn) {
        StringBuilder sb = new StringBuilder();
            sb.append("o:").append(overlayId).append("***********\n");
            int maxSlots = 0;
            int usedSlots = 0;
            int successSlots = 0;
            int failedSlots = 0;
            for (Map.Entry<Identifier, ConnectionStatus> cs : conn.entrySet()) {
                sb.append("o:").append(overlayId).append("n:").append(cs.getKey());
                maxSlots += cs.getValue().maxSlots;
                sb.append(" ms:").append(cs.getValue().maxSlots);
                usedSlots += cs.getValue().usedSlots;
                sb.append(" us:").append(cs.getValue().usedSlots);
                successSlots += cs.getValue().successSlots;
                sb.append(" s:").append(cs.getValue().successSlots);
                failedSlots += cs.getValue().failedSlots;
                sb.append(" f:").append(cs.getValue().failedSlots);
                sb.append("\n");
            }
            sb.append("o:").append(overlayId);
            sb.append(" tms:").append(maxSlots);
            sb.append(" tus:").append(usedSlots);
            sb.append(" ts:").append(successSlots);
            sb.append(" tf:").append(failedSlots);
            
            LOG.debug("{}", sb);
            
            transferSize += 1024 * successSlots;
    }

    Handler handleReport = new Handler<ReportTimeout>() {
        @Override
        public void handle(ReportTimeout event) {
            LOG.debug("{}report", logPrefix);
            trigger(new DownloadStatus.Request(event.overlayId), torrentPort);
        }
    };

    public static class Init extends se.sics.kompics.Init<ReportComp> {

        public final long reportDelay;

        public Init(long reportDelay) {
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
