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
package se.sics.nstream.torrent.conn;

import se.sics.nstream.torrent.FileIdentifier;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.javatuples.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.kompics.ClassMatchedHandler;
import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.Handler;
import se.sics.kompics.KompicsEvent;
import se.sics.kompics.Negative;
import se.sics.kompics.Positive;
import se.sics.kompics.Start;
import se.sics.kompics.network.Network;
import se.sics.kompics.network.Transport;
import se.sics.kompics.timer.CancelPeriodicTimeout;
import se.sics.kompics.timer.CancelTimeout;
import se.sics.kompics.timer.SchedulePeriodicTimeout;
import se.sics.kompics.timer.ScheduleTimeout;
import se.sics.kompics.timer.Timeout;
import se.sics.kompics.timer.Timer;
import se.sics.ktoolbox.util.identifiable.Identifier;
import se.sics.ktoolbox.util.network.KAddress;
import se.sics.ktoolbox.util.network.KContentMsg;
import se.sics.ktoolbox.util.network.KHeader;
import se.sics.ktoolbox.util.network.basic.BasicContentMsg;
import se.sics.ktoolbox.util.network.basic.BasicHeader;
import se.sics.ktoolbox.util.network.basic.DecoratedHeader;
import se.sics.ktoolbox.util.tracking.load.NetworkQueueLoadProxy;
import se.sics.ktoolbox.util.tracking.load.QueueLoadConfig;
import se.sics.ledbat.core.AppCongestionWindow;
import se.sics.ledbat.core.LedbatConfig;
import se.sics.ledbat.ncore.msg.LedbatMsg;
import se.sics.nstream.torrent.TransferConfig;
import se.sics.nstream.torrent.conn.dwnl.event.BlocksCompleted;
import se.sics.nstream.torrent.conn.dwnl.event.DownloadBlocks;
import se.sics.nstream.torrent.conn.dwnl.event.DwnlConnReport;
import se.sics.nstream.torrent.conn.dwnl.event.FPDControl;
import se.sics.nstream.torrent.conn.msg.CacheHint;
import se.sics.nstream.torrent.conn.msg.DownloadPiece;
import se.sics.nstream.torrent.event.TorrentTimeout;

/**
 * A DwnlConnComp per torrent per peer per file per dwnl (there is an
 equivalent comp for upld)
 *
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class DwnlConnComp extends ComponentDefinition {

    private static final Logger LOG = LoggerFactory.getLogger(DwnlConnComp.class);
    private String logPrefix;

    private static final long ADVANCE_DOWNLOAD = 1000;
    private static final long CACHE_TIMEOUT = 2000;
    private static final long REPORT_PERIOD = 1000;
    //**************************************************************************
    Negative<DwnlConnPort> connPort = provides(DwnlConnPort.class);
    Positive<Network> networkPort = requires(Network.class);
    Positive<Timer> timerPort = requires(Timer.class);
    //**************************************************************************
    private final FileIdentifier fileId;
    private final KAddress self;
    private final KAddress target;
    //**************************************************************************
    private final NetworkQueueLoadProxy networkQueueLoad;
    private final AppCongestionWindow cwnd;
    private final DwnlConnWorkCtrl workController;
    //**************************************************************************
    private UUID advanceDownloadTid;
    private UUID cacheTid;
    private UUID reportTid;
    private final Map<Identifier, UUID> pendingPieces = new HashMap<>();
    //**************************************************************************
    private final LedbatConfig ledbatConfig;

    public DwnlConnComp(Init init) {
        fileId = init.fileId;
        self = init.self;
        target = init.target;
        logPrefix = "<nid:" + self.getId() + ",cid:" + fileId + ">";

        ledbatConfig = new LedbatConfig(config());
        networkQueueLoad = new NetworkQueueLoadProxy(logPrefix, proxy, new QueueLoadConfig(config()));
        cwnd = new AppCongestionWindow(ledbatConfig, fileId);
        workController = new DwnlConnWorkCtrl();

        subscribe(handleStart, control);
        subscribe(handleReport, timerPort);
        subscribe(handleAdvanceDownload, timerPort);
        subscribe(handleFPDControl, connPort);
        subscribe(handleNewBlocks, connPort);
        subscribe(handleCacheTimeout, timerPort);
        subscribe(handleCache, networkPort);
        subscribe(handlePieceTimeout, timerPort);
        subscribe(handlePiece, networkPort);
    }

    Handler handleStart = new Handler<Start>() {
        @Override
        public void handle(Start event) {
            LOG.info("{}starting conn to:{}", logPrefix, target);
            networkQueueLoad.start();
            scheduleAdvanceDownload();
            scheduleReport();
        }
    };

    @Override
    public void tearDown() {
        networkQueueLoad.tearDown();
        cancelAdvanceDownload();
        cancelReport();
    }
    
    Handler handleReport = new Handler<ReportTimeout>() {
        @Override
        public void handle(ReportTimeout event) {
            Pair<Integer, Integer> queueDelay = networkQueueLoad.queueDelay();
            trigger(new DwnlConnReport(fileId, target, queueDelay), connPort);
        }
    };

    Handler handleAdvanceDownload = new Handler<TorrentTimeout.AdvanceDownload>() {
        @Override
        public void handle(TorrentTimeout.AdvanceDownload event) {
            LOG.trace("{}advance download", logPrefix);
            tryDownload(System.currentTimeMillis());
        }
    };
    
    Handler handleFPDControl = new Handler<FPDControl>() {
        @Override
        public void handle(FPDControl event) {
            double adjustment = Math.min(event.appCwndAdjustment, networkQueueLoad.adjustment());
            cwnd.adjustState(adjustment);
        }
    };
    //**********************************FPD*************************************
    private Handler handleNewBlocks = new Handler<DownloadBlocks>() {
        @Override
        public void handle(DownloadBlocks event) {
            workController.add(event.blocks);
        }
    };

    //**************************************************************************
    Handler handleCacheTimeout = new Handler<CacheTimeout>() {
        @Override
        public void handle(CacheTimeout timeout) {
            if (cacheTid != null && cacheTid.equals(timeout.getTimeoutId())) {
                sendCacheReq(timeout.req);
                //TODO maybe give up at one point;
            }
        }
    };
    ClassMatchedHandler handleCache
            = new ClassMatchedHandler<CacheHint.Response, KContentMsg<KAddress, KHeader<KAddress>, CacheHint.Response>>() {

                @Override
                public void handle(CacheHint.Response content, KContentMsg<KAddress, KHeader<KAddress>, CacheHint.Response> context) {
                    workController.cacheConfirmed();
                    trigger(new CancelTimeout(cacheTid), timerPort);
                    cacheTid = null;
                }
            };

    private void sendCacheReq(CacheHint.Request req) {
        networkSend(req);
        trigger(scheduleCacheTimeout(req), timerPort);
    }
    //**************************************************************************
    Handler handlePieceTimeout = new Handler<PieceTimeout>() {
        @Override
        public void handle(PieceTimeout timeout) {
            UUID pieceTid = pendingPieces.remove(timeout.req.eventId);
            if (pieceTid != null) {
                long now = System.currentTimeMillis();
                workController.pieceTimeout(timeout.req.piece);
                cwnd.timeout(now, ledbatConfig.mss);
                tryDownload(now);
            }
        }
    };

    ClassMatchedHandler handlePiece
            = new ClassMatchedHandler<LedbatMsg.Response<DownloadPiece.Response>, KContentMsg<KAddress, KHeader<KAddress>, LedbatMsg.Response<DownloadPiece.Response>>>() {

                @Override
                public void handle(LedbatMsg.Response<DownloadPiece.Response> content, KContentMsg<KAddress, KHeader<KAddress>, LedbatMsg.Response<DownloadPiece.Response>> context) {
                    DownloadPiece.Response resp = content.payload;
                    UUID pieceTid = pendingPieces.remove(resp.eventId);
                    long now = System.currentTimeMillis();
                    if (pieceTid != null) {
                        workController.piece(resp.piece, resp.val.getRight());
                        cwnd.success(now, ledbatConfig.mss, content);
                        trigger(new CancelTimeout(pieceTid), timerPort);
                        tryDownload(now);
                    } else {
                        workController.latePiece(resp.piece, resp.val.getRight());
                        cwnd.late(now, ledbatConfig.mss, content);
                    }
                    if(workController.hasComplete()) {
                        trigger(new BlocksCompleted(fileId, target, workController.getCompleteBlocks()), connPort);
                    }
                }
            };

    private void sendPieceReq(DownloadPiece.Request req) {
        LedbatMsg.Request aux = new LedbatMsg.Request(req);
        networkSend(aux);
        trigger(schedulePieceTimeout(req), timerPort);
    }

    //**************************************************************************

    private void tryDownload(long now) {
        if (workController.hasNewHint()) {
            sendCacheReq(new CacheHint.Request(fileId, workController.newHint()));
        }
        while (workController.hasNextPiece() && cwnd.canSend()) {
            sendPieceReq(new DownloadPiece.Request(fileId, workController.next()));
            cwnd.request(now, ledbatConfig.mss);
        }
    }

    //**************************************************************************

    public void networkSend(KompicsEvent event) {
        KHeader header = new DecoratedHeader(new BasicHeader(self, target, Transport.UDP), fileId);
        KContentMsg msg = new BasicContentMsg<>(header, event);
        trigger(msg, networkPort);
    }

    public static class Init extends se.sics.kompics.Init<DwnlConnComp> {

        public final FileIdentifier fileId;
        public final KAddress self;
        public final KAddress target;

        public Init(FileIdentifier fileId, KAddress self, KAddress target) {
            this.fileId = fileId;
            this.self = self;
            this.target = target;
        }
    }
    
     private void scheduleAdvanceDownload() {
        SchedulePeriodicTimeout spt = new SchedulePeriodicTimeout(TransferConfig.advanceDownloadPeriod, TransferConfig.advanceDownloadPeriod);
        TorrentTimeout.AdvanceDownload tt = new TorrentTimeout.AdvanceDownload(spt);
        spt.setTimeoutEvent(tt);
        advanceDownloadTid = tt.getTimeoutId();
        trigger(spt, timerPort);
     }


    private void cancelAdvanceDownload() {
        CancelPeriodicTimeout cpd = new CancelPeriodicTimeout(advanceDownloadTid);
        trigger(cpd, timerPort);
        advanceDownloadTid = null;
    }
    
    private ScheduleTimeout scheduleCacheTimeout(CacheHint.Request req) {
        ScheduleTimeout st = new ScheduleTimeout(CACHE_TIMEOUT);
        CacheTimeout ct = new CacheTimeout(st, req);
        st.setTimeoutEvent(ct);
        cacheTid = ct.getTimeoutId();
        return st;
    }

    public static class CacheTimeout extends Timeout {

        public final CacheHint.Request req;

        public CacheTimeout(ScheduleTimeout st, CacheHint.Request req) {
            super(st);
            this.req = req;
        }
    }

     private ScheduleTimeout schedulePieceTimeout(DownloadPiece.Request req) {
        ScheduleTimeout st = new ScheduleTimeout(cwnd.getRTO());
        PieceTimeout ct = new PieceTimeout(st, req);
        st.setTimeoutEvent(ct);
        pendingPieces.put(req.eventId, ct.getTimeoutId());
        return st;
    }
     
    public static class PieceTimeout extends Timeout {

        public final DownloadPiece.Request req;

        public PieceTimeout(ScheduleTimeout st, DownloadPiece.Request req) {
            super(st);
            this.req = req;
        }
    }
    
    private void scheduleReport() {
        SchedulePeriodicTimeout spt = new SchedulePeriodicTimeout(REPORT_PERIOD, REPORT_PERIOD);
        ReportTimeout rt = new ReportTimeout(spt);
        spt.setTimeoutEvent(rt);
        reportTid = rt.getTimeoutId();
        trigger(spt, timerPort);
    }

    private void cancelReport() {
        CancelPeriodicTimeout cpd = new CancelPeriodicTimeout(reportTid);
        trigger(cpd, timerPort);
        reportTid = null;
    }
    
    public static class ReportTimeout extends Timeout {
        public ReportTimeout(SchedulePeriodicTimeout spt) {
            super(spt);
        }
    }
};
