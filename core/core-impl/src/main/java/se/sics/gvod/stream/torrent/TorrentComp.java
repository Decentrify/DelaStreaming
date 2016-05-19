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
package se.sics.gvod.stream.torrent;

import se.sics.gvod.stream.torrent.event.Download;
import com.google.common.base.Optional;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import org.javatuples.Pair;
import org.javatuples.Triplet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.gvod.common.util.VodDescriptor;
import se.sics.gvod.core.util.TorrentDetails;
import se.sics.gvod.stream.StreamEvent;
import se.sics.gvod.stream.congestion.PLedbatPort;
import se.sics.gvod.stream.congestion.event.external.PLedbatConnection;
import se.sics.gvod.stream.connection.ConnMngrPort;
import se.sics.gvod.stream.connection.event.Connection;
import se.sics.gvod.stream.torrent.event.DownloadStatus;
import se.sics.gvod.stream.torrent.event.TorrentGet;
import se.sics.kompics.ClassMatchedHandler;
import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.Handler;
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
import se.sics.ktoolbox.util.config.impl.SystemKCWrapper;
import se.sics.ktoolbox.util.identifiable.Identifier;
import se.sics.ktoolbox.util.identifiable.basic.UUIDIdentifier;
import se.sics.ktoolbox.util.managedStore.core.FileMngr;
import se.sics.ktoolbox.util.managedStore.core.HashMngr;
import se.sics.ktoolbox.util.managedStore.core.ManagedStoreHelper;
import se.sics.ktoolbox.util.managedStore.core.impl.TransferMngr;
import se.sics.ktoolbox.util.managedStore.core.impl.util.PrepDwnlInfo;
import se.sics.ktoolbox.util.managedStore.core.util.Torrent;
import se.sics.ktoolbox.util.network.KAddress;
import se.sics.ktoolbox.util.network.KContentMsg;
import se.sics.ktoolbox.util.network.KHeader;
import se.sics.ktoolbox.util.network.basic.BasicContentMsg;
import se.sics.ktoolbox.util.network.basic.BasicHeader;
import se.sics.ktoolbox.util.network.basic.DecoratedHeader;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class TorrentComp extends ComponentDefinition {

    private static final Logger LOG = LoggerFactory.getLogger(TorrentComp.class);
    private String logPrefix;

    //****************************CONNECTIONS***********************************
    //***********************EXTERNAL_CONNECT_TO********************************
    Positive<Timer> timerPort = requires(Timer.class);
    Positive<Network> networkPort = requires(Network.class);
    Positive<ConnMngrPort> connectionPort = requires(ConnMngrPort.class);
    Positive<PLedbatPort> congestionPort = requires(PLedbatPort.class);
    Negative<TorrentStatus> statusPort = provides(TorrentStatus.class);
    //**************************EXTERNAL_STATE**********************************
    private final LoadModifiersKCWrapper loadModifiersConfig;
    private final KAddress selfAdr;
    private final Identifier overlayId;
    private final long defaultMsgTimeout;
    private final long checkPeriod = 1000;
    //**************************INTERNAL_STATE**********************************
    private TransferFSM transferFSM;
    private LoadTracker loadTracker;
    private Random rand;
    private UUID periodicCheckTId;

    public TorrentComp(Init init) {
        selfAdr = init.selfAdr;
        overlayId = init.torrentDetails.getOverlayId();
        logPrefix = "<nid:" + selfAdr.getId() + ", oid:" + overlayId + ">";
        LOG.info("{}initiating...", logPrefix);

        SystemKCWrapper systemConfig = new SystemKCWrapper(config());
        rand = new Random(systemConfig.seed);
        loadModifiersConfig = new LoadModifiersKCWrapper(config());
        defaultMsgTimeout = loadModifiersConfig.maxLinkRTT;
        loadTracker = new LoadTracker(this.proxy, loadModifiersConfig.targetQueueingDelay, loadModifiersConfig.maxQueueingDelay, logPrefix + "[TorrentComp]");
        transferFSM = new TransferInit(init.torrentDetails);
        transferFSM.setup();

        subscribe(handleStart, control);
        subscribe(handleCheck, timerPort);
    }

    Handler handleStart = new Handler<Start>() {
        @Override
        public void handle(Start event) {
            LOG.info("{}starting...", logPrefix);
            schedulePeriodicCheck();
        }
    };
    
    @Override
    public void tearDown() {
        LOG.warn("{}tearing down", logPrefix);
        cancelPeriodicTimer();
        transferFSM.cleanup();
    }

    Handler handleCheck = new Handler<StatusCheck>() {
        @Override
        public void handle(StatusCheck event) {
            transferFSM.report();
        }
    };

    public void schedulePeriodicCheck() {
        SchedulePeriodicTimeout spt = new SchedulePeriodicTimeout(checkPeriod, checkPeriod);
        Timeout t = new StatusCheck(spt);
        spt.setTimeoutEvent(t);
        trigger(spt, timerPort);
        periodicCheckTId = t.getTimeoutId();
    }
    
    public void cancelPeriodicTimer() {
        CancelPeriodicTimeout cpt = new CancelPeriodicTimeout(periodicCheckTId);
        trigger(cpt, timerPort);
    }

    private void sendNetwork(KAddress destination, Object content) {
        KHeader msgHeader = new DecoratedHeader(new BasicHeader(selfAdr, destination, Transport.UDP), overlayId);
        KContentMsg msg = new BasicContentMsg(msgHeader, content);
        LOG.trace("{}sending:{}", logPrefix, msg);
        trigger(msg, networkPort);
    }

    private void answerNetwork(KContentMsg msg, Object content) {
        KContentMsg resp = msg.answer(content);
        LOG.trace("{}sending:{}", logPrefix, resp);
        trigger(resp, networkPort);
    }

    interface TransferFSM {

        public void setup();

        public void cleanup();

        public void next();

        public void report();
    }

    class TransferInit implements TransferFSM {

        TorrentDetails torrentDetails;

        public TransferInit(TorrentDetails torrentDetails) {
            this.torrentDetails = torrentDetails;
        }

        @Override
        public void setup() {
            LOG.debug("{}TransferFSM setup InitState", logPrefix);
            subscribe(handleFSMStart, control);
        }

        @Override
        public void cleanup() {
            LOG.debug("{}TransferFSM cleanup InitState", logPrefix);
            unsubscribe(handleFSMStart, control);
        }

        @Override
        public void next() {
            //I only do something on start event
        }

        @Override
        public void report() {
            LOG.info("{}TransferFSM - InitState", logPrefix);
        }

        Handler handleFSMStart = new Handler<Start>() {
            @Override
            public void handle(Start event) {
                if (torrentDetails.download()) {
                    moveToGetTorrentState();
                } else {
                    moveToUploadTorrentState();
                }
            }
        };

        private void moveToGetTorrentState() {
            GetTorrent nextState = new GetTorrent(torrentDetails);
            nextState.setup();
            cleanup();
            nextState.next();
            transferFSM = nextState;
        }

        private void moveToUploadTorrentState() {
            Torrent torrent = torrentDetails.getTorrent();
            Triplet<FileMngr, HashMngr, TransferMngr> torrentMngrs = torrentDetails.torrentMngrs(torrent);
            UploadTorrent nextState = new UploadTorrent(torrent, torrentMngrs.getValue0(), torrentMngrs.getValue1());
            nextState.setup();
            cleanup();
            transferFSM = nextState;
        }

    }

    class GetTorrent implements TransferFSM {

        final TorrentDetails torrentDetails;

        final DwnlConnAux dwnlConn;

        UUID torrentGetTId;

        public GetTorrent(TorrentDetails torrentDetails) {
            this.torrentDetails = torrentDetails;
            dwnlConn = new DwnlConnAux();
        }

        @Override
        public void setup() {
            LOG.debug("{}TransferFSM setup GetTorrentState", logPrefix);
            subscribe(handlePublishConn, connectionPort);
            subscribe(handleTorrentResponse, networkPort);
            subscribe(handleTorrentGetTimeout, timerPort);
        }

        @Override
        public void cleanup() {
            LOG.debug("{}TransferFSM cleanup GetTorrentState", logPrefix);
            unsubscribe(handlePublishConn, connectionPort);
            unsubscribe(handleTorrentResponse, networkPort);
            unsubscribe(handleTorrentGetTimeout, timerPort);
        }

        @Override
        public void next() {
            trigger(new Connection.Request(), connectionPort);
        }

        @Override
        public void report() {
            LOG.info("{}TransferFSM - GetTorrentState", logPrefix);
        }

        Handler handlePublishConn = new Handler<Connection.Indication>() {
            @Override
            public void handle(Connection.Indication event) {
                LOG.debug("{}new connections to:{}", new Object[]{logPrefix, event.connections.keySet()});
                dwnlConn.addConnections(event.connections, event.descriptors);
                if (torrentGetTId == null) {
                    sendTorrentRequest();
                }
            }
        };

        ClassMatchedHandler handleTorrentResponse
                = new ClassMatchedHandler<TorrentGet.Response, BasicContentMsg<KAddress, DecoratedHeader<KAddress>, TorrentGet.Response>>() {

                    @Override
                    public void handle(TorrentGet.Response content, BasicContentMsg<KAddress, DecoratedHeader<KAddress>, TorrentGet.Response> container) {
                        KAddress target = container.getHeader().getSource();
                        LOG.debug("{}received torrent response from:{}", new Object[]{logPrefix, target.getId()});
                        cancelTorrentTimeout();
                        moveToDownloadState(content.torrent);
                    }
                };

        Handler handleTorrentGetTimeout = new Handler<TorrentGetTimeout>() {
            @Override
            public void handle(TorrentGetTimeout timeout) {
                if (timeout.getTimeoutId().equals(torrentGetTId)) {
                    LOG.debug("{}target:{} for torrent timed out", logPrefix, timeout.target.getId());
                    dwnlConn.mngr.timedOut(timeout.target);
                    torrentGetTId = null;
                    sendTorrentRequest();
                } else {
                    LOG.trace("{}late timeout:{}", logPrefix, timeout);
                }
            }
        };

        private void moveToDownloadState(Torrent torrent) {
            LOG.info("{}setting up for download", logPrefix);
            DownloadTorrent nextPhase = new DownloadTorrent(torrent, torrentDetails, dwnlConn);
            nextPhase.setup();
            cleanup();
            nextPhase.next();
            transferFSM = nextPhase;
        }

        private void sendTorrentRequest() {
            KAddress target = dwnlConn.mngr.getRandomConnection();
            LOG.debug("{}request torrent from:{}", logPrefix, target.getId());
            scheduleTorrentTimeout(target);
            sendNetwork(target, new TorrentGet.Request());
        }

        public void scheduleTorrentTimeout(KAddress target) {
            ScheduleTimeout st = new ScheduleTimeout(defaultMsgTimeout);
            Timeout t = new TorrentGetTimeout(st, target);
            st.setTimeoutEvent(t);
            trigger(st, timerPort);
            torrentGetTId = t.getTimeoutId();
        }

        public void cancelTorrentTimeout() {
            CancelTimeout ct = new CancelTimeout(torrentGetTId);
            trigger(ct, timerPort);
            torrentGetTId = null;
        }
    }

    class UploadTorrent implements TransferFSM {

        final Torrent torrent;
        final FileMngr fileMngr;
        final HashMngr hashMngr;

        public UploadTorrent(Torrent torrent, FileMngr fileMngr, HashMngr hashMngr) {
            this.torrent = torrent;
            this.fileMngr = fileMngr;
            this.hashMngr = hashMngr;
        }

        @Override
        public void setup() {
            subscribe(handleTorrentRequest, networkPort);
            subscribe(handleHashRequest, networkPort);
            subscribe(handlePieceRequest, networkPort);
        }

        @Override
        public void cleanup() {
            unsubscribe(handleTorrentRequest, networkPort);
            unsubscribe(handleHashRequest, networkPort);
            unsubscribe(handlePieceRequest, networkPort);
        }

        @Override
        public void next() {
            throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        }

        @Override
        public void report() {
            LOG.info("{}TransferFSM Upload", logPrefix);
        }

        ClassMatchedHandler handleTorrentRequest
                = new ClassMatchedHandler<TorrentGet.Request, BasicContentMsg<KAddress, DecoratedHeader<KAddress>, TorrentGet.Request>>() {

                    @Override
                    public void handle(TorrentGet.Request content, BasicContentMsg<KAddress, DecoratedHeader<KAddress>, TorrentGet.Request> container) {
                        KAddress target = container.getHeader().getSource();
                        LOG.debug("{}received torrent request from:{}", new Object[]{logPrefix, target.getId()});
                        answerNetwork(container, content.success(torrent));
                    }
                };

        ClassMatchedHandler handleHashRequest
                = new ClassMatchedHandler<Download.HashRequest, BasicContentMsg<KAddress, DecoratedHeader<KAddress>, Download.HashRequest>>() {

                    @Override
                    public void handle(Download.HashRequest content, BasicContentMsg<KAddress, DecoratedHeader<KAddress>, Download.HashRequest> container) {
                        KAddress target = container.getHeader().getSource();
                        LOG.debug("{}received hashPos:{} request from:{}", new Object[]{logPrefix, content.targetPos, target.getId()});
                        Pair<Map<Integer, ByteBuffer>, Set<Integer>> result = hashMngr.readHashes(content.hashes);
                        answerNetwork(container, content.success(result.getValue0(), result.getValue1()));
                    }
                };

        ClassMatchedHandler handlePieceRequest
                = new ClassMatchedHandler<Download.DataRequest, BasicContentMsg<KAddress, DecoratedHeader<KAddress>, Download.DataRequest>>() {

                    @Override
                    public void handle(Download.DataRequest content, BasicContentMsg<KAddress, DecoratedHeader<KAddress>, Download.DataRequest> container) {
                        KAddress target = container.getHeader().getSource();
                        LOG.debug("{}received data:{} request from:{}", new Object[]{logPrefix, content.pieceId, target.getId()});
                        if (fileMngr.hasPiece(content.pieceId)) {
                            ByteBuffer piece = fileMngr.readPiece(content.pieceId);
                            answerNetwork(container, content.success(piece));
                        } else {
                            answerNetwork(container, content.missingPiece());
                        }
                    }
                };
    }

    class DownloadTorrent implements TransferFSM {

        static final int hashsesPerMsg = 50;
        static final int hashMsgPerRound = 1;
        static final int minBlockPlayBuffer = 1;
        static final int minAheadHashes = 10;
        static final long downloadCheckPeriod = 1000;

        final TorrentDetails torrentDetails; //in case I want to reshape the upload phase once download is completed;
        final Torrent torrent;

        final UploadTorrent upload;

        final DwnlConnAux dwnlConn;
        final TransferMngr transferMngr;

        private Map<Identifier, UUID> pendingPieces = new HashMap<>();
        private Map<Identifier, UUID> pendingHashes = new HashMap<>();

        public DownloadTorrent(Torrent torrent, TorrentDetails torrentDetails, DwnlConnAux dwnlConn) {
            this.torrentDetails = torrentDetails;
            this.torrent = torrent;
            Triplet<FileMngr, HashMngr, TransferMngr> torrentMngrs = torrentDetails.torrentMngrs(torrent);
            upload = new UploadTorrent(torrent, torrentMngrs.getValue0(), torrentMngrs.getValue1());
            transferMngr = torrentMngrs.getValue2();
            this.dwnlConn = dwnlConn;
        }

        @Override
        public void setup() {
            upload.setup();
            subscribe(handlePublishConn, connectionPort);
            subscribe(handleStatusRequest, statusPort);
            subscribe(handleHashResponse, networkPort);
            subscribe(handlePieceResponse, networkPort);
            subscribe(handleHashTimeout, timerPort);
            subscribe(handlePieceTimeout, timerPort);
        }

        @Override
        public void cleanup() {
            upload.cleanup();
            internalCleanup();
        }

        private void internalCleanup() {
            unsubscribe(handlePublishConn, connectionPort);
            unsubscribe(handleHashResponse, networkPort);
            unsubscribe(handlePieceResponse, networkPort);
            unsubscribe(handleHashTimeout, timerPort);
            unsubscribe(handlePieceTimeout, timerPort);
        }

        @Override
        public void next() {
            trigger(new DownloadStatus.Starting(overlayId), statusPort);
            download();
        }

        @Override
        public void report() {
            int pendingMsg = pendingPieces.size() + pendingHashes.size();
            Pair<Integer, Integer> dwnlReport = dwnlConn.mngr.getLoad();
            Triplet<Long, Long, Long> loadTimes = loadTracker.times();
            LOG.info("{}load:{} (r:{},h:{},q:{})",
                    new Object[]{logPrefix, loadTracker.getLoad(), loadTimes.getValue0(), loadTimes.getValue1(), loadTimes.getValue2()});
            LOG.info("{}TransferFSM Download[p:{},u:{}/m:{}] TransferMngr:{}",
                    new Object[]{logPrefix, pendingMsg, dwnlReport.getValue0(), dwnlReport.getValue1(), transferMngr});
            upload.report();
        }

        Handler handleStatusRequest = new Handler<DownloadStatus.Request>() {
            @Override
            public void handle(DownloadStatus.Request req) {
                LOG.trace("{}received:{}", logPrefix, req);
                answer(req, req.answer(dwnlConn.mngr.reportNReset()));
            }
        };

        Handler handlePublishConn = new Handler<Connection.Indication>() {
            @Override
            public void handle(Connection.Indication event) {
                LOG.debug("{}new connections to:{}", new Object[]{logPrefix, event.connections.keySet()});
                dwnlConn.addConnections(event.connections, event.descriptors);
                download();
            }
        };

        ClassMatchedHandler handleHashResponse
                = new ClassMatchedHandler<Download.HashResponse, BasicContentMsg<KAddress, DecoratedHeader<KAddress>, Download.HashResponse>>() {

                    @Override
                    public void handle(Download.HashResponse content, BasicContentMsg<KAddress, DecoratedHeader<KAddress>, Download.HashResponse> container) {
                        KAddress target = container.getHeader().getSource();
                        LOG.debug("{}received hashPos:{} response from:{}", new Object[]{logPrefix, content.targetPos, target.getId()});

                        if (!cancelPendingHashTimeout(content.getId())) {
                            LOG.debug("{}hash from:{} - posibly late", logPrefix, target.getId());
                            return;
                        }
                        switch (content.status) {
                            case SUCCESS:
                                LOG.trace("{}SUCCESS hashes:{} missing hashes:{}", new Object[]{logPrefix, content.hashes.keySet(), content.missingHashes});
                                transferMngr.writeHashes(content.hashes, content.missingHashes);
                                dwnlConn.mngr.completed(target, content.getStatus());
                                loadTracker.trackLoad(content);
                                download();
                                return;
                            case TIMEOUT:
                            case BUSY:
                                LOG.debug("{}BUSY/TIMEOUT hashes:{}", logPrefix, content.missingHashes);
                                transferMngr.writeHashes(content.hashes, content.missingHashes);
                                dwnlConn.mngr.timedOut(target);
                                download();
                                return;
                            default:
                                LOG.warn("{}illegal status:{}, ignoring", new Object[]{logPrefix, content.status});
                        }
                    }
                };

        ClassMatchedHandler handlePieceResponse
                = new ClassMatchedHandler<Download.DataResponse, BasicContentMsg<KAddress, DecoratedHeader<KAddress>, Download.DataResponse>>() {

                    @Override
                    public void handle(Download.DataResponse content, BasicContentMsg<KAddress, DecoratedHeader<KAddress>, Download.DataResponse> container) {
                        KAddress target = container.getHeader().getSource();
                        LOG.debug("{}received data:{} response from:{}", new Object[]{logPrefix, content.pieceId, target.getId()});

                        if (!cancelPendingPieceTimeout(content.getId())) {
                            LOG.debug("{}piece:{} from:{} - posibly late", new Object[]{logPrefix, content.pieceId, target.getId()});
                            return;
                        }
                        switch (content.status) {
                            case SUCCESS:
                                LOG.trace("{}SUCCESS piece:{}", new Object[]{logPrefix, content.pieceId});
                                transferMngr.writePiece(content.pieceId, content.piece);
                                dwnlConn.mngr.completed(target, content.getStatus());
                                loadTracker.trackLoad(content);
                                download();
                                return;
                            case TIMEOUT:
                            case BUSY:
                                LOG.debug("{}BUSY/TIMEOUT piece:{}", logPrefix, content.pieceId);
                                transferMngr.resetPiece(content.pieceId);
                                dwnlConn.mngr.timedOut(target);
                                download();
                                return;
                            default:
                                LOG.warn("{} illegal status:{}, ignoring", new Object[]{logPrefix, content.status});
                        }
                    }
                };

        Handler handleHashTimeout = new Handler<PendingHashTimeout>() {
            @Override
            public void handle(PendingHashTimeout timeout) {
                if (pendingHashes.remove(timeout.request.getId()) != null) {
                    LOG.debug("{}timeout hash:{} from:{}", new Object[]{logPrefix, timeout.request.targetPos, timeout.target.getId()});
                    transferMngr.writeHashes(new HashMap<Integer, ByteBuffer>(), timeout.request.hashes);
                    dwnlConn.mngr.timedOut(timeout.target);
                    download();
                } else {
                    LOG.trace("{}late timeout:{}", logPrefix, timeout);
                }
            }
        };

        Handler handlePieceTimeout = new Handler<PendingPieceTimeout>() {
            @Override
            public void handle(PendingPieceTimeout timeout) {
                if (pendingPieces.remove(timeout.request.getId()) != null) {
                    LOG.debug("{}timeout piece:{} from:{}", new Object[]{logPrefix, timeout.request.pieceId, timeout.target.getId()});
                    transferMngr.resetPiece(timeout.request.pieceId);
                    dwnlConn.mngr.timedOut(timeout.target);
                    download();
                } else {
                    LOG.trace("{}late timeout:{}", logPrefix, timeout);
                }
            }
        };

        private void download() {
            double load = loadTracker.getLoad();
            if (dwnlConn.mngr.getLoad().getValue0() > 10) {
                if (load == 1) {
                    if (rand.nextInt(10) != 0) {
                        LOG.info("{}too loaded to download", logPrefix);
                        if (loadTracker.shouldSlowDown()) {
                            int onTheWire = dwnlConn.mngr.localOverloaded();
                            loadTracker.slowedDown(onTheWire);
                        }
                        report();
                        return;
                    }
                } else if (0 < load) {
                    if (rand.nextDouble() < load) {
                        LOG.info("{}too loaded to download", logPrefix);
                        if (loadTracker.shouldSlowDown()) {
                            int onTheWire = dwnlConn.mngr.localOverloaded();
                            loadTracker.slowedDown(onTheWire);
                        }
                        report();
                        return;
                    }
                } else {
                    if (loadTracker.canSpeedUp()) {
                        int onTheWire = dwnlConn.mngr.localUnderloaded();
                        loadTracker.spedUp(onTheWire);
                    }
                }
            }
            while (true) {
                Optional<Set<Integer>> hashes = transferMngr.downloadHash(hashsesPerMsg);
                if (hashes.isPresent()) {
                    int hashPos = Collections.min(hashes.get());
                    Optional<KAddress> dwnlSrc = dwnlConn.mngr.download(hashPos);
                    if (dwnlSrc.isPresent()) {
                        Download.HashRequest req = new Download.HashRequest(overlayId, hashPos, hashes.get());
                        schedulePendingHashTimeout(req, dwnlSrc.get());
                        sendNetwork(dwnlSrc.get(), req);
                        continue;
                    } else {
                        //no source - put back
                        transferMngr.writeHashes(new HashMap<Integer, ByteBuffer>(), hashes.get());
                    }
                }
                Optional<Integer> piece = transferMngr.downloadData();
                if (piece.isPresent()) {
                    Pair<Integer, Integer> blockDetails = ManagedStoreHelper.componentDetails(piece.get(), torrent.torrentInfo.piecesPerBlock);
                    Optional<KAddress> dwnlSrc = dwnlConn.mngr.download(blockDetails.getValue0());
                    if (dwnlSrc.isPresent()) {
                        Download.DataRequest req = new Download.DataRequest(overlayId, piece.get());
                        schedulePendingPieceTimeout(req, dwnlSrc.get());
                        sendNetwork(dwnlSrc.get(), req);
                        continue;
                    } else {
                        //no source - put back
                        transferMngr.resetPiece(piece.get());
                    }
                }
                if (hashes.isPresent() || piece.isPresent()) {
                    //have something to download next, but no one to download from atm - wait
                    return;
                } else {
                    transferMngr.checkCompleteBlocks();
                    if (transferMngr.isComplete()) {
                        moveToUploadState();
                        return;
                    }
                    int prep = transferMngr.prepareDownload(0, new PrepDwnlInfo(hashsesPerMsg, hashMsgPerRound, minBlockPlayBuffer, minAheadHashes));
                    if (prep == -1) {
                        return;
                    }
                }
            }
        }

        private void moveToUploadState() {
            LOG.info("{}cleaning up download state", logPrefix);
            for (UUID tId : pendingHashes.values()) {
                cancelTimeout(tId);
            }
            pendingHashes.clear();
            for (UUID tId : pendingPieces.values()) {
                cancelTimeout(tId);
            }
            pendingPieces.clear();
            internalCleanup();
            transferFSM = upload;

            trigger(new DownloadStatus.Done(overlayId, dwnlConn.mngr.reportNReset()), statusPort);
        }

        private void cancelTimeout(UUID tId) {
            CancelTimeout ct = new CancelTimeout(tId);
            trigger(ct, timerPort);
        }

        private void schedulePendingHashTimeout(Download.HashRequest request, KAddress target) {
            ScheduleTimeout st = new ScheduleTimeout(defaultMsgTimeout);
            Timeout t = new PendingHashTimeout(st, request, target);
            st.setTimeoutEvent(t);
            trigger(st, timerPort);
            pendingHashes.put(request.getId(), t.getTimeoutId());
        }

        private boolean cancelPendingHashTimeout(Identifier pieceId) {
            UUID tId = pendingHashes.remove(pieceId);
            if (tId == null) {
                LOG.trace("{}late timeout for hash:{}", logPrefix, pieceId);
                return false;
            }
            CancelTimeout ct = new CancelTimeout(tId);
            trigger(ct, timerPort);
            return true;
        }

        private void schedulePendingPieceTimeout(Download.DataRequest request, KAddress target) {
            ScheduleTimeout st = new ScheduleTimeout(defaultMsgTimeout);
            Timeout t = new PendingPieceTimeout(st, request, target);
            st.setTimeoutEvent(t);
            trigger(st, timerPort);
            pendingPieces.put(request.getId(), t.getTimeoutId());
        }

        private boolean cancelPendingPieceTimeout(Identifier pieceId) {
            UUID tId = pendingPieces.remove(pieceId);
            if (tId == null) {
                LOG.debug("{}late:{} timeout occured", logPrefix, pieceId);
                return false;
            }
            CancelTimeout ct = new CancelTimeout(tId);
            trigger(ct, timerPort);
            return true;
        }
    }

    public class DwnlConnAux {

        public final DwnlConnMngr mngr;
        final Map<Identifier, PLedbatConnection.TrackRequest> trackingReqs = new HashMap<>();
        

        public DwnlConnAux() {
            mngr = new DwnlConnMngr(loadModifiersConfig.timeoutSlowDownModifier, loadModifiersConfig.normalSlowDownModifier, loadModifiersConfig.speedUpModifier);
        }

        public void addConnections(Map<Identifier, KAddress> connections, Map<Identifier, VodDescriptor> descriptors) {
            for (Identifier partnerId : connections.keySet()) {
                KAddress partnerAdr = connections.get(partnerId);
                VodDescriptor partnerDesc = descriptors.get(partnerId);
                mngr.addConnection(partnerAdr, partnerDesc);

                PLedbatConnection.TrackRequest trackingReq = new PLedbatConnection.TrackRequest(partnerAdr);
                trigger(trackingReq, congestionPort);
                trackingReqs.put(trackingReq.getId(), trackingReq);
            }
        }
    }

    public static class Init extends se.sics.kompics.Init<TorrentComp> {

        public final KAddress selfAdr;
        public final TorrentDetails torrentDetails;

        public Init(KAddress selfAdr, TorrentDetails torrentDetails) {
            this.selfAdr = selfAdr;
            this.torrentDetails = torrentDetails;
        }
    }

    class StatusCheck extends Timeout implements StreamEvent {

        public StatusCheck(SchedulePeriodicTimeout spt) {
            super(spt);
        }

        @Override
        public Identifier getId() {
            return new UUIDIdentifier(getTimeoutId());
        }

        @Override
        public String toString() {
            return "StatusCheck<" + getId() + ">";
        }
    }

    class TorrentGetTimeout extends Timeout implements StreamEvent {

        public final KAddress target;

        public TorrentGetTimeout(ScheduleTimeout st, KAddress target) {
            super(st);
            this.target = target;
        }

        @Override
        public Identifier getId() {
            return new UUIDIdentifier(getTimeoutId());
        }

        @Override
        public String toString() {
            return "TorrentGetTimeout<" + getId() + ">";
        }
    }

    class PendingHashTimeout extends Timeout implements StreamEvent {

        public final Download.HashRequest request;
        public final KAddress target;

        public PendingHashTimeout(ScheduleTimeout st, Download.HashRequest request, KAddress target) {
            super(st);
            this.request = request;
            this.target = target;
        }

        @Override
        public Identifier getId() {
            return new UUIDIdentifier(getTimeoutId());
        }

        @Override
        public String toString() {
            return "PendingHashTimeout<" + getId() + ">";
        }
    }

    public class PendingPieceTimeout extends Timeout implements StreamEvent {

        public final Download.DataRequest request;
        public final KAddress target;

        public PendingPieceTimeout(ScheduleTimeout st, Download.DataRequest request, KAddress target) {
            super(st);
            this.request = request;
            this.target = target;
        }

        @Override
        public Identifier getId() {
            return new UUIDIdentifier(getTimeoutId());
        }

        @Override
        public String toString() {
            return "PendingPieceTimeout<" + getId() + ">";
        }
    }
}
