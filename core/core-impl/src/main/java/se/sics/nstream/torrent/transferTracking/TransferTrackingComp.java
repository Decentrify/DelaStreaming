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
package se.sics.nstream.torrent.transferTracking;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
import se.sics.nstream.torrent.FileIdentifier;
import se.sics.nstream.torrent.util.TorrentConnId;

/**
 *
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class TransferTrackingComp extends ComponentDefinition {

    private static final Logger LOG = LoggerFactory.getLogger(TransferTrackingComp.class);
    private String logPrefix;

    private static final long TICK_PERIOD = 1000;
    //**************************************************************************
    private final Negative<TransferReportPort> reportPort = provides(TransferReportPort.class);
    private final Positive<TransferTrackingPort> trackingPort = requires(TransferTrackingPort.class);
    private final Positive<Timer> timerPort = requires(Timer.class);
    //**************************************************************************
    private final Map<TorrentConnId, ConnTracking> seederConnections = new HashMap<>();
    //**************************************************************************
    private UUID reportTId;

    public TransferTrackingComp(Init init) {
        logPrefix = "<" + init.torrentId + ">";

        subscribe(handleStart, control);
        subscribe(handleReport, timerPort);
        subscribe(handleDownloadTracking, trackingPort);
        subscribe(handleDownloadConnectionClosed, trackingPort);
    }

    Handler handleStart = new Handler<Start>() {
        @Override
        public void handle(Start event) {
            LOG.info("{}starting...", logPrefix);
            scheduleReport();
        }
    };

    @Override
    public void tearDown() {
        cancelReport();
    }

    Handler handleReport = new Handler<ReportTimeout>() {
        @Override
        public void handle(ReportTimeout event) {
            LOG.trace("{}reporting", logPrefix);
            DownloadTrackingTrace.CrossConnectionAccumulator totalAcc = new DownloadTrackingTrace.CrossConnectionAccumulator();
            Map<Identifier, DownloadTrackingTrace.CrossConnectionAccumulator> peerAccs = new HashMap<>();
            Map<FileIdentifier, DownloadTrackingTrace.CrossConnectionAccumulator> fileAccs = new HashMap<>();
            for (ConnTracking conn : seederConnections.values()) {
                conn.tick();
                DownloadTrackingTrace connReport = conn.report();
                totalAcc.accumulate(connReport);
                
                DownloadTrackingTrace.CrossConnectionAccumulator peerAcc = peerAccs.get(conn.connId.targetId);
                if(peerAcc == null) {
                    peerAcc = new DownloadTrackingTrace.CrossConnectionAccumulator();
                    peerAccs.put(conn.connId.targetId, peerAcc);
                }
                peerAcc.accumulate(connReport);
                
                DownloadTrackingTrace.CrossConnectionAccumulator fileAcc = peerAccs.get(conn.connId.fileId);
                if(fileAcc == null) {
                    fileAcc = new DownloadTrackingTrace.CrossConnectionAccumulator();
                    fileAccs.put(conn.connId.fileId, peerAcc);
                }
                fileAcc.accumulate(connReport);
            }
            
            Map<Identifier, DownloadTrackingTrace> peerReport = new HashMap<>();
            Map<FileIdentifier, DownloadTrackingTrace> fileReport = new HashMap<>();
            for(Map.Entry<Identifier, DownloadTrackingTrace.CrossConnectionAccumulator> acc : peerAccs.entrySet()) {
                peerReport.put(acc.getKey(), acc.getValue().build());
            } 
            for(Map.Entry<FileIdentifier, DownloadTrackingTrace.CrossConnectionAccumulator> acc : fileAccs.entrySet()) {
                fileReport.put(acc.getKey(), acc.getValue().build());
            } 
            trigger(new TransferTrackingReport(new DownloadReport(totalAcc.build(), fileReport, peerReport)), reportPort);
        }
    };

    Handler handleDownloadTracking = new Handler<DownloadTrackingReport>() {
        @Override
        public void handle(DownloadTrackingReport event) {
            LOG.trace("{}received:{}", logPrefix, event);
            ConnTracking conn = seederConnections.get(event.connId);
            if (conn == null) {
                conn = new ConnTracking(event.connId);
                seederConnections.put(event.connId, conn);
            }
            conn.handle(event.trace);
        }
    };
    
    Handler handleDownloadConnectionClosed = new Handler<DownloadConnectionClosed>() {
        @Override
        public void handle(DownloadConnectionClosed event) {
            LOG.debug("{}download conn:{} closed", logPrefix, event.connId);
            seederConnections.remove(event.connId);
        }
    };

    public static class Init extends se.sics.kompics.Init<TransferTrackingComp> {

        public final Identifier torrentId;

        public Init(Identifier torrentId) {
            this.torrentId = torrentId;
        }
    }

    public void scheduleReport() {
        SchedulePeriodicTimeout spt = new SchedulePeriodicTimeout(TICK_PERIOD, TICK_PERIOD);
        Timeout t = new ReportTimeout(spt);
        spt.setTimeoutEvent(t);
        trigger(spt, timerPort);
        reportTId = t.getTimeoutId();
    }

    public void cancelReport() {
        CancelPeriodicTimeout ct = new CancelPeriodicTimeout(reportTId);
        trigger(ct, timerPort);
        reportTId = null;
    }

    public static class ReportTimeout extends Timeout {

        public ReportTimeout(SchedulePeriodicTimeout spt) {
            super(spt);
        }
    }
}
