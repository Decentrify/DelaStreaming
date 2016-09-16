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
package se.sics.nstream.torrent.transfer;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.javatuples.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.kompics.ClassMatchedHandler;
import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.Handler;
import se.sics.kompics.Negative;
import se.sics.kompics.Positive;
import se.sics.kompics.Start;
import se.sics.kompics.network.Network;
import se.sics.kompics.network.Transport;
import se.sics.kompics.timer.CancelPeriodicTimeout;
import se.sics.kompics.timer.SchedulePeriodicTimeout;
import se.sics.kompics.timer.Timeout;
import se.sics.kompics.timer.Timer;
import se.sics.ktoolbox.util.identifiable.Identifiable;
import se.sics.ktoolbox.util.identifiable.Identifier;
import se.sics.ktoolbox.util.network.KAddress;
import se.sics.ktoolbox.util.network.KContentMsg;
import se.sics.ktoolbox.util.network.KHeader;
import se.sics.ktoolbox.util.network.basic.BasicContentMsg;
import se.sics.ktoolbox.util.network.basic.BasicHeader;
import se.sics.ledbat.core.AppCongestionWindow;
import se.sics.ledbat.core.LedbatConfig;
import se.sics.ledbat.ncore.msg.LedbatMsg;
import se.sics.nstream.torrent.event.TorrentTimeout;
import se.sics.nstream.torrent.old.TransferConfig;
import se.sics.nstream.torrent.transfer.dwnl.event.CompletedBlocks;
import se.sics.nstream.torrent.transfer.dwnl.event.DownloadBlocks;
import se.sics.nstream.torrent.transfer.dwnl.event.DwnlConnReport;
import se.sics.nstream.torrent.transfer.dwnl.event.FPDControl;
import se.sics.nstream.torrent.transfer.msg.CacheHint;
import se.sics.nstream.torrent.transfer.msg.DownloadHash;
import se.sics.nstream.torrent.transfer.msg.DownloadPiece;
import se.sics.nstream.torrent.util.TorrentConnId;
import se.sics.nstream.util.BlockDetails;
import se.sics.nutil.ContentWrapperHelper;
import se.sics.nutil.network.bestEffort.event.BestEffortMsg;
import se.sics.nutil.tracking.load.NetworkQueueLoadProxy;
import se.sics.nutil.tracking.load.QueueLoadConfig;

/**
 * A DwnlConnComp per torrent per peer per file per dwnl (there is an equivalent
 * comp for upld)
 *
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class DwnlConnComp extends ComponentDefinition {

    private static final Logger LOG = LoggerFactory.getLogger(DwnlConnComp.class);
    private String logPrefix;

    private static final long ADVANCE_DOWNLOAD = 1000;
    private static final long CACHE_TIMEOUT = 10000;
    private static final long REPORT_PERIOD = 1000;
    //**************************************************************************
    Negative<DwnlConnPort> connPort = provides(DwnlConnPort.class);
    Positive<Network> networkPort = requires(Network.class);
    Positive<Timer> timerPort = requires(Timer.class);
    //**************************************************************************
    private final TorrentConnId connId;
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
    private final Set<Identifier> pendingMsgs = new HashSet<>();
    //**************************************************************************
    private final LedbatConfig ledbatConfig;

    public DwnlConnComp(Init init) {
        connId = init.connId;
        self = init.self;
        target = init.target;
        logPrefix = connId.toString();

        ledbatConfig = new LedbatConfig(config());
        networkQueueLoad = new NetworkQueueLoadProxy(logPrefix, proxy, new QueueLoadConfig(config()));
        cwnd = new AppCongestionWindow(ledbatConfig, connId);
        workController = new DwnlConnWorkCtrl(init.defaultBlockDetails, init.withHashes);

        subscribe(handleStart, control);
        subscribe(handleReport, timerPort);
        subscribe(handleAdvanceDownload, timerPort);
        subscribe(handleFPDControl, connPort);
        subscribe(handleNewBlocks, connPort);
        subscribe(handleNetworkTimeouts, networkPort);
        subscribe(handleCache, networkPort);
        subscribe(handleLedbat, networkPort);
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
            trigger(new DwnlConnReport(connId, queueDelay), connPort);
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
            workController.add(event.blocks, event.irregularBlocks);
        }
    };
    //**************************************************************************
    ClassMatchedHandler handleNetworkTimeouts
            = new ClassMatchedHandler<BestEffortMsg.Timeout, KContentMsg<KAddress, KHeader<KAddress>, BestEffortMsg.Timeout>>() {
                @Override
                public void handle(BestEffortMsg.Timeout wrappedContent, KContentMsg<KAddress, KHeader<KAddress>, BestEffortMsg.Timeout> context) {
                    Object content = ContentWrapperHelper.getBaseContent(wrappedContent, Object.class);
                    if (content instanceof CacheHint.Request) {
                        handleCacheTimeout((CacheHint.Request) content);
                    } else if (content instanceof DownloadPiece.Request) {
                        handlePieceTimeout((DownloadPiece.Request) content);
                    } else if (content instanceof DownloadHash.Request) {
                        handleHashTimeout((DownloadHash.Request) content);
                    } else {
                        LOG.warn("{}!!!possible performance issue - fix:{}", logPrefix, content);
                    }
                }
            };

    public void handleCacheTimeout(CacheHint.Request req) {
        throw new RuntimeException("ups");
    }

    public void handlePieceTimeout(DownloadPiece.Request req) {
        if (pendingMsgs.remove(req.eventId)) {
            long now = System.currentTimeMillis();
            workController.pieceTimeout(req.piece);
            cwnd.timeout(now, ledbatConfig.mss);
            tryDownload(now);
        }
    }
    
    public void handleHashTimeout(DownloadHash.Request req) {
        if (pendingMsgs.remove(req.eventId)) {
            long now = System.currentTimeMillis();
            workController.hashTimeout(req.hashes);
            cwnd.timeout(now, ledbatConfig.mss);
            tryDownload(now);
        }
    }

    //**************************************************************************
    private void sendSimpleUDP(Identifiable content) {
        BestEffortMsg.Request wrappedContent = new BestEffortMsg.Request<>(content, 1, cwnd.getRTO());
        KHeader header = new BasicHeader(self, target, Transport.UDP);
        KContentMsg msg = new BasicContentMsg<>(header, wrappedContent);
        LOG.trace("{}sending:{}", logPrefix, content);
        trigger(msg, networkPort);
    }

    private void sendSimpleLedbat(Identifiable content) {
        LedbatMsg.Request ledbatContent = new LedbatMsg.Request(content);
        sendSimpleUDP(ledbatContent);
    }

    ClassMatchedHandler handleCache
            = new ClassMatchedHandler<CacheHint.Response, KContentMsg<KAddress, KHeader<KAddress>, CacheHint.Response>>() {

                @Override
                public void handle(CacheHint.Response content, KContentMsg<KAddress, KHeader<KAddress>, CacheHint.Response> context) {
                    if (pendingMsgs.remove(content.getId())) {
                        LOG.debug("{}received cache confirm", logPrefix);
                        workController.cacheConfirmed();
                        tryDownload(System.currentTimeMillis());
                    }
                }
            };

    ClassMatchedHandler handleLedbat
            = new ClassMatchedHandler<LedbatMsg.Response, KContentMsg<KAddress, KHeader<KAddress>, LedbatMsg.Response>>() {

                @Override
                public void handle(LedbatMsg.Response content, KContentMsg<KAddress, KHeader<KAddress>, LedbatMsg.Response> context) {
                    Object baseContent = content.getWrappedContent();
                    if (baseContent instanceof DownloadPiece.Response) {
                        handlePiece(content);
                    } else if (baseContent instanceof DownloadHash.Response) {
                        handleHash(content);
                    } else {
                        throw new RuntimeException("ups");
                    }
                }
            };

    private void handlePiece(LedbatMsg.Response<DownloadPiece.Response> content) {
        LOG.trace("{}received:{}", logPrefix, content);
        DownloadPiece.Response resp = content.getWrappedContent();
        long now = System.currentTimeMillis();
        if (pendingMsgs.remove(resp.eventId)) {
            workController.piece(resp.piece, resp.val.getRight());
            cwnd.success(now, ledbatConfig.mss, content);
            tryDownload(now);
        } else {
            workController.latePiece(resp.piece, resp.val.getRight());
            cwnd.late(now, ledbatConfig.mss, content);
        }
        if (workController.hasComplete()) {
            Pair<Map<Integer, byte[]>, Map<Integer, byte[]>> completed = workController.getComplete();
            LOG.info("{}completed hashes:{} blocks:{}", new Object[]{logPrefix, completed.getValue0().keySet(), completed.getValue1().keySet()});
            trigger(new CompletedBlocks(connId, completed.getValue0(), completed.getValue1()), connPort);
        }
    }

    private void handleHash(LedbatMsg.Response<DownloadHash.Response> content) {
        LOG.trace("{}received:{}", logPrefix, content);
        DownloadHash.Response resp = content.getWrappedContent();
        long now = System.currentTimeMillis();
        if (pendingMsgs.remove(resp.eventId)) {
            workController.hashes(resp.hashValues);
            cwnd.success(now, ledbatConfig.mss, content);
            tryDownload(now);
        } else {
            workController.lateHashes(resp.hashValues);
            cwnd.late(now, ledbatConfig.mss, content);
        }
        if (workController.hasComplete()) {
            Pair<Map<Integer, byte[]>, Map<Integer, byte[]>> completed = workController.getComplete();
            LOG.info("{}completed hashes:{} blocks:{}", new Object[]{logPrefix, completed.getValue0().keySet(), completed.getValue1().keySet()});
            trigger(new CompletedBlocks(connId, completed.getValue0(), completed.getValue1()), connPort);
        }
    }

    //**************************************************************************
    private void tryDownload(long now) {
        if (workController.hasNewHint()) {
            CacheHint.Request req = new CacheHint.Request(connId.fileId, workController.newHint());
            sendSimpleUDP(req);
            pendingMsgs.add(req.getId());
        }
        while (workController.hasHashes() && cwnd.canSend()) {
            DownloadHash.Request req = new DownloadHash.Request(connId.fileId, workController.nextHashes());
            sendSimpleLedbat(req);
            pendingMsgs.add(req.getId());
            cwnd.request(now, ledbatConfig.mss);
        }
        while (workController.hasPiece() && cwnd.canSend()) {
            DownloadPiece.Request req = new DownloadPiece.Request(connId.fileId, workController.nextPiece());
            sendSimpleLedbat(req);
            pendingMsgs.add(req.getId());
            cwnd.request(now, ledbatConfig.mss);
        }
    }

    //**************************************************************************
    public static class Init extends se.sics.kompics.Init<DwnlConnComp> {

        public final TorrentConnId connId;
        public final KAddress self;
        public final KAddress target;
        public final BlockDetails defaultBlockDetails;
        public final boolean withHashes;

        public Init(TorrentConnId connId, KAddress self, KAddress target, BlockDetails defaultBlockDetails, boolean withHashes) {
            this.connId = connId;
            this.self = self;
            this.target = target;
            this.defaultBlockDetails = defaultBlockDetails;
            this.withHashes = withHashes;
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
