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
package se.sics.nstream.torrent;

import com.google.common.base.Optional;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;
import org.javatuples.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.kompics.ComponentProxy;
import se.sics.kompics.KompicsEvent;
import se.sics.kompics.Positive;
import se.sics.kompics.config.Config;
import se.sics.kompics.network.Network;
import se.sics.kompics.network.Transport;
import se.sics.kompics.timer.CancelTimeout;
import se.sics.kompics.timer.SchedulePeriodicTimeout;
import se.sics.kompics.timer.ScheduleTimeout;
import se.sics.kompics.timer.Timer;
import se.sics.ktoolbox.util.identifiable.Identifier;
import se.sics.ktoolbox.util.network.KAddress;
import se.sics.ktoolbox.util.network.KContentMsg;
import se.sics.ktoolbox.util.network.KHeader;
import se.sics.ktoolbox.util.network.basic.BasicContentMsg;
import se.sics.ktoolbox.util.network.basic.BasicHeader;
import se.sics.ktoolbox.util.reference.KReference;
import se.sics.ktoolbox.util.result.DelayedExceptionSyncHandler;
import se.sics.ktoolbox.util.result.Result;
import se.sics.nstream.storage.cache.KHint;
import se.sics.nstream.torrent.event.HashGet;
import se.sics.nstream.torrent.event.PieceGet;
import se.sics.nstream.torrent.event.TorrentGet;
import se.sics.nstream.torrent.event.TorrentTimeout;
import se.sics.nstream.transfer.MultiFileTransfer;
import se.sics.nstream.transfer.Transfer;
import se.sics.nstream.transfer.TransferMngr;
import se.sics.nstream.transfer.TransferMngrPort;
import se.sics.nstream.util.FileBaseDetails;
import se.sics.nstream.util.TransferDetails;
import se.sics.nstream.util.result.HashReadCallback;
import se.sics.nstream.util.result.NopHashWC;
import se.sics.nstream.util.result.NopPieceWC;
import se.sics.nstream.util.result.PieceReadCallback;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public abstract class TransferFSM {

    private final static Logger LOG = LoggerFactory.getLogger(TransferFSM.class);

    protected final CommonState cs;

    public TransferFSM(CommonState cs) {
        this.cs = cs;
    }

    public abstract void start();

    public abstract TransferFSM next();

    public abstract void report();

    public abstract void close();

    public void handleTorrentReq(KContentMsg msg, TorrentGet.Request req) {
        LOG.trace("{}received:{}", cs.logPrefix, msg);
        answer(msg, req.busy());
    }

    public void handleTorrentTimeout(TorrentTimeout.Metadata timeout) {
    }

    public void handleTorrentResp(KContentMsg msg, TorrentGet.Response resp) {
    }

    public void handleExtendedTorrentResp(Transfer.DownloadResponse resp) {
    }

    public void handleAdvanceDownload(TorrentTimeout.AdvanceDownload timeout) {
    }

    public void handleHashReq(KContentMsg<KAddress, KHeader<KAddress>, HashGet.Request> msg, HashGet.Request req) {
    }

    public void handleHashResp(KContentMsg<KAddress, KHeader<KAddress>, HashGet.Response> msg, HashGet.Response resp) {
    }

    public void handleHashTimeout(TorrentTimeout.Hash timeout) {
    }

    public void handlePieceReq(KContentMsg<KAddress, KHeader<KAddress>, PieceGet.Request> msg, PieceGet.Request req) {
    }

    public void handlePieceResp(KContentMsg<KAddress, KHeader<KAddress>, PieceGet.Response> msg, PieceGet.Response resp) {
    }

    public void handlePieceTimeout(TorrentTimeout.Piece timeout) {
    }

    protected void request(KAddress partner, KompicsEvent content, ScheduleTimeout timeout) {
        KHeader header = new BasicHeader(cs.selfAdr, partner, Transport.UDP);
        KContentMsg msg = new BasicContentMsg(header, content);
        LOG.trace("{}sending:{}", cs.logPrefix, msg);
        cs.proxy.trigger(msg, cs.networkPort);
        cs.proxy.trigger(timeout, cs.timerPort);
    }

    protected void answer(Pair content) {
        answer((KContentMsg) content.getValue0(), (KompicsEvent) content.getValue1());
    }

    protected void answer(KContentMsg msg, KompicsEvent content) {
        LOG.trace("{}answering:{}", cs.logPrefix, msg);
        cs.proxy.trigger(msg.answer(content), cs.networkPort);
    }

    protected void cancelTimeout(UUID tid) {
        LOG.trace("{}canceling:{}", cs.logPrefix, tid);
        cs.proxy.trigger(new CancelTimeout(tid), cs.timerPort);
    }

    public static class InitState extends TransferFSM {

        private byte[] torrentByte;
        private TransferDetails transferDetails = null;
        private Pair<UUID, Identifier> tid;

        public InitState(CommonState cs) {
            super(cs);
        }

        @Override
        public void start() {
            requestTorrentDetails();
        }

        @Override
        public TransferFSM next() {
            if (transferDetails != null) {
                TransferFSM nextState = new DownloadState(cs, torrentByte, transferDetails);
                nextState.start();
                return nextState;
            }
            return this;
        }

        @Override
        public void report() {
            LOG.info("{}init state", cs.logPrefix);
        }

        @Override
        public void close() {
            cancelTimeout(tid.getValue0());
        }

        @Override
        public void handleTorrentTimeout(TorrentTimeout.Metadata timeout) {
            LOG.trace("{}received:{}", cs.logPrefix, timeout);
            if (tid.getValue0().equals(timeout.getTimeoutId())) {
                requestTorrentDetails();
            }
        }

        @Override
        public void handleTorrentResp(KContentMsg msg, TorrentGet.Response resp) {
            LOG.trace("{}received:{}", cs.logPrefix, msg);
            if (tid.getValue1().equals(resp.getId())) {
                cancelTimeout(tid.getValue0());
                if (Result.Status.isSuccess(resp.status)) {
                    torrentByte = resp.torrent;
                    cs.proxy.trigger(new Transfer.DownloadRequest(cs.overlayId, Result.success(torrentByte)), cs.transferMngrPort);
                } else {
                    requestTorrentDetails();
                }
            }
        }

        @Override
        public void handleExtendedTorrentResp(Transfer.DownloadResponse resp) {
            transferDetails = resp.transferDetails;
        }

        private void requestTorrentDetails() {
            KAddress target = cs.router.randomPartner();
            TorrentGet.Request req = new TorrentGet.Request(cs.overlayId);
            request(target, req, scheduleTorrentTimeout(req.eventId, target));
        }

        private ScheduleTimeout scheduleTorrentTimeout(Identifier eventId, KAddress target) {
            ScheduleTimeout st = new ScheduleTimeout(cs.torrentConfig.netDelay);
            TorrentTimeout.Metadata tt = new TorrentTimeout.Metadata(st, target);
            st.setTimeoutEvent(tt);
            tid = Pair.with(tt.getTimeoutId(), eventId);
            return st;
        }
    }

    public static abstract class TransferState extends TransferFSM {

        protected final byte[] torrentByte;
        protected final TransferDetails transferDetails;
        protected final MultiFileTransfer transferMngr;
        protected final Map<Identifier, PendingHashReq> pendingHashes = new HashMap<>();
        protected final Map<Identifier, PieceGet.Request> pendingPieces = new HashMap<>();

        public TransferState(CommonState cs, byte[] torrentByte, TransferDetails transferDetails, MultiFileTransfer transferMngr) {
            super(cs);
            this.torrentByte = torrentByte;
            this.transferDetails = transferDetails;
            this.transferMngr = transferMngr;
        }

        @Override
        public void start() {
            transferMngr.start();
        }

        @Override
        public void close() {
            transferMngr.close();
        }

        @Override
        public void handleTorrentReq(KContentMsg msg, TorrentGet.Request req) {
            answer(msg, req.success(torrentByte));
        }

        private void updateCacheHint(Identifier readerId, Map<String, KHint.Summary> cacheHints) {
            for (Map.Entry<String, KHint.Summary> e : cacheHints.entrySet()) {
                TransferMngr.Reader reader = transferMngr.readFrom(e.getKey());
                FileBaseDetails baseDetails = transferDetails.base.baseDetails.get(e.getKey());
                reader.setFutureReads(readerId, e.getValue().expand(baseDetails));
            }
        }

        @Override
        public void handleHashReq(KContentMsg<KAddress, KHeader<KAddress>, HashGet.Request> msg, final HashGet.Request req) {
            updateCacheHint(msg.getHeader().getSource().getId(), req.cacheHints);
            TransferMngr.Reader reader = transferMngr.readFrom(req.fileName);
            final PendingHashReq phr = new PendingHashReq(msg);
            pendingHashes.put(req.eventId, phr);
            for (final Integer hash : req.hashes) {
                if (reader.hasHash(hash)) {
                    HashReadCallback hashRC = new HashReadCallback() {
                        @Override
                        public boolean fail(Result<KReference<byte[]>> result) {
                            //TODO Alex - exception
                            throw new RuntimeException("ups...");
                        }

                        @Override
                        public boolean success(Result<KReference<byte[]>> result) {
                            phr.hash(hash, result.getValue());
                            if (phr.canAnswer()) {
                                pendingHashes.remove(req.eventId);
                                answer(phr.answer());
                            }
                            return true;
                        }
                    };
                    reader.readHash(hash, hashRC);
                } else {
                    phr.missingHash(hash);
                    if (phr.canAnswer()) {
                        pendingHashes.remove(req.eventId);
                        answer(phr.answer());
                    }
                }
            }
        }

        @Override
        public void handlePieceReq(final KContentMsg<KAddress, KHeader<KAddress>, PieceGet.Request> msg, final PieceGet.Request req) {
            if (req.pieceNr.getValue1() == 1023) {
                int x = 1;
            }
            LOG.trace("{}received req for b:{},pb:{}", new Object[]{cs.logPrefix, req.pieceNr.getValue0(), req.pieceNr.getValue1()});
            updateCacheHint(msg.getHeader().getSource().getId(), req.cacheHints);
            TransferMngr.Reader reader = transferMngr.readFrom(req.fileName);
            if (reader.hasBlock(req.pieceNr.getValue0())) {
                pendingPieces.put(req.eventId, req);
                PieceReadCallback pieceRC = new PieceReadCallback() {

                    @Override
                    public boolean fail(Result<KReference<byte[]>> result) {
                        throw new RuntimeException(result.getException());
                    }

                    @Override
                    public boolean success(Result<KReference<byte[]>> result) {
                        pendingPieces.remove(req.eventId);
                        answer(msg, req.success(result.getValue()));
                        return true;
                    }
                };
                reader.readPiece(req.pieceNr, pieceRC);
            } else {
                answer(msg, req.missingBlock());
            }
        }

        public static class PendingHashReq {

            private final KContentMsg<KAddress, KHeader<KAddress>, HashGet.Request> req;
            private final Map<Integer, ByteBuffer> hashes = new TreeMap<>();
            private final Set<Integer> missingHashes = new TreeSet<>();

            public PendingHashReq(KContentMsg<KAddress, KHeader<KAddress>, HashGet.Request> req) {
                this.req = req;
            }

            public void hash(int hashNr, KReference<byte[]> hash) {
                hashes.put(hashNr, ByteBuffer.wrap(hash.getValue().get()));
            }

            public void missingHash(int hashNr) {
                missingHashes.add(hashNr);
            }

            public boolean canAnswer() {
                return req.getContent().hashes.size() == hashes.size() + missingHashes.size();
            }

            public Pair answer() {
                HashGet.Response resp = req.getContent().success(hashes, missingHashes);
                return Pair.with(req, resp);
            }
        }
    }

    public static class DownloadState extends TransferState {

        private final Map<Identifier, UUID> pendingHashes = new HashMap<>();
        private final Map<Identifier, UUID> pendingPieces = new HashMap<>();
        private UUID advanceDownload;

        public DownloadState(CommonState cs, byte[] torrentByte, TransferDetails transferDetails) {
            super(cs, torrentByte, transferDetails, new MultiFileTransfer(cs.config, cs.proxy, cs.exSyncHandler, transferDetails, false));
        }

        @Override
        public void start() {
            super.start();
            tryDownload();
            cs.proxy.trigger(scheduleAdvanceDownload(), cs.timerPort);
        }

        @Override
        public void close() {
            super.close();
            cancelTimeout(advanceDownload);
        }

        @Override
        public TransferFSM next() {
            return this;
        }

        @Override
        public void handleAdvanceDownload(TorrentTimeout.AdvanceDownload timeout) {
            tryDownload();
        }

        @Override
        public void handleHashResp(KContentMsg msg, HashGet.Response resp) {
            UUID tId = pendingHashes.remove(resp.eventId);
            if (tId == null) {
                LOG.trace("{}late response:{}", cs.logPrefix, resp);
                return;
            }
            cancelTimeout(tId);
            cs.router.releaseSlot();
            TransferMngr.Writer writer = transferMngr.writeTo(resp.fileName);
            if (resp.status.isSuccess()) {
                for (Map.Entry<Integer, ByteBuffer> hash : resp.hashes.entrySet()) {
                    LOG.debug("{}received hash:{}", cs.logPrefix, hash.getKey());
                    writer.writeHash(hash.getKey(), hash.getValue().array(), new NopHashWC());
                }
                writer.resetHashes(resp.missingHashes);
            } else {
                writer.resetHashes(resp.missingHashes);
            }
            tryDownload();
        }

        @Override
        public void handleHashTimeout(TorrentTimeout.Hash timeout) {
            LOG.trace("{}timeout:{}", cs.logPrefix, timeout);
            UUID tId = pendingHashes.remove(timeout.req.getId());
            if (tId == null) {
                LOG.trace("{}late timeout:{}", cs.logPrefix, timeout);
                return;
            }
            cs.router.timeoutSlot(timeout.target);
            TransferMngr.Writer writer = transferMngr.writeTo(timeout.req.fileName);
            writer.resetHashes(timeout.req.hashes);
        }

        @Override
        public void handlePieceResp(KContentMsg<KAddress, KHeader<KAddress>, PieceGet.Response> msg, final PieceGet.Response resp) {
            if (resp.pieceNr.getValue1() == 1023) {
                int x = 1;
            }
            UUID tId = pendingPieces.remove(resp.eventId);
            if (tId == null) {
                LOG.trace("{}late response:{}", cs.logPrefix, resp);
                return;
            }
            cancelTimeout(tId);
            cs.router.releaseSlot();
            TransferMngr.Writer writer = transferMngr.writeTo(resp.fileName);

            LOG.trace("{}received resp for b:{},pb:{}", new Object[]{cs.logPrefix, resp.pieceNr.getValue0(), resp.pieceNr.getValue1()});
            writer.writePiece(resp.pieceNr, resp.piece.array(), new NopPieceWC());
            tryDownload();
        }

        @Override
        public void handlePieceTimeout(TorrentTimeout.Piece timeout) {
            UUID tId = pendingPieces.remove(timeout.req.eventId);
            if (tId == null) {
                LOG.trace("{}late timeout:{}", cs.logPrefix, timeout);
                return;
            }
            cs.router.timeoutSlot(timeout.target);
            TransferMngr.Writer writer = transferMngr.writeTo(timeout.req.fileName);
            writer.resetPiece(timeout.req.pieceNr);
        }

        @Override
        public void report() {
            LOG.info("{}transfer report:\n{}", new Object[]{cs.logPrefix, transferMngr.report()});
        }

        private void tryDownload() {
            LOG.trace("{}downloading...", cs.logPrefix);

            if (!cs.router.hasSlot()) {
                return;
            }
            if (!transferMngr.hasOngoing()) {
                LOG.info("{}waiting to complete...", cs.logPrefix);
                LOG.info("{}completed", cs.logPrefix);
                return;
            }

            Optional<MultiFileTransfer.NextDownload> nextDownload = transferMngr.nextDownload();
            if (!nextDownload.isPresent()) {
                if (transferMngr.complete()) {
                    LOG.info("{}transfer complete", cs.logPrefix);
                } else {
                    LOG.info("{}waiting to complete...", cs.logPrefix);
                }
                return;
            }
            if (nextDownload.get() instanceof MultiFileTransfer.NextHash) {
                MultiFileTransfer.NextHash nextHash = (MultiFileTransfer.NextHash) nextDownload.get();
                HashGet.Request hashReq = new HashGet.Request(cs.overlayId, nextHash.cacheHints, nextHash.fileName, nextHash.hashes.getValue0(), nextHash.hashes.getValue1());
                LOG.trace("{}download hashes:{}", cs.logPrefix, hashReq.hashes);
                KAddress partner = cs.router.randomPartner();
                cs.router.retainSlot();
                request(partner, hashReq, scheduleHashTimeout(hashReq, partner));
            } else if (nextDownload.get() instanceof MultiFileTransfer.NextPiece) {
                MultiFileTransfer.NextPiece nextPiece = (MultiFileTransfer.NextPiece) nextDownload.get();
                PieceGet.Request pieceReq = new PieceGet.Request(cs.overlayId, nextPiece.cacheHints, nextPiece.fileName, nextPiece.piece);
                LOG.trace("{}download block:{} pieceBlock:{}", new Object[]{cs.logPrefix, pieceReq.pieceNr.getValue0(), pieceReq.pieceNr.getValue1()});
                KAddress partner = cs.router.randomPartner();
                cs.router.retainSlot();
                request(partner, pieceReq, schedulePieceTimeout(pieceReq, partner));
            }
        }

        private SchedulePeriodicTimeout scheduleAdvanceDownload() {
            SchedulePeriodicTimeout spt = new SchedulePeriodicTimeout(TransferConfig.advanceDownloadPeriod, TransferConfig.advanceDownloadPeriod);
            TorrentTimeout.AdvanceDownload tt = new TorrentTimeout.AdvanceDownload(spt);
            spt.setTimeoutEvent(tt);
            advanceDownload = tt.getTimeoutId();
            return spt;
        }

        private ScheduleTimeout scheduleHashTimeout(HashGet.Request req, KAddress target) {
            ScheduleTimeout st = new ScheduleTimeout(TransferConfig.hashTimeout);
            TorrentTimeout.Hash tt = new TorrentTimeout.Hash(st, req, target);
            st.setTimeoutEvent(tt);
            pendingHashes.put(req.getId(), tt.getTimeoutId());
            return st;
        }

        private ScheduleTimeout schedulePieceTimeout(PieceGet.Request req, KAddress target) {
            ScheduleTimeout st = new ScheduleTimeout(TransferConfig.pieceTimeout);
            TorrentTimeout.Piece tt = new TorrentTimeout.Piece(st, req, target);
            st.setTimeoutEvent(tt);
            pendingPieces.put(req.getId(), tt.getTimeoutId());
            return st;

        }
    }

    public static class UploadState extends TransferState {

        public UploadState(CommonState cs, byte[] torrentByte, TransferDetails transferDetails) {
            super(cs, torrentByte, transferDetails, new MultiFileTransfer(cs.config, cs.proxy, cs.exSyncHandler, transferDetails, true));
        }

        @Override
        public void start() {
        }

        @Override
        public TransferFSM next() {
            return this;
        }

        @Override
        public void report() {
            LOG.info("{}uploading", cs.logPrefix);
        }
    }

    public static class CommonState {

        public String logPrefix = "";

        public final ComponentProxy proxy;
        public final DelayedExceptionSyncHandler exSyncHandler = null; //TODO 
        //**********************************************************************
        Positive<Timer> timerPort;
        Positive<Network> networkPort;
        Positive<TransferMngrPort> transferMngrPort;
        //**********************************************************************
        public final Config config;
        public final TorrentConfig torrentConfig;
        public final KAddress selfAdr;
        public final Identifier overlayId;
        public final Router router;

        public CommonState(Config config, TorrentConfig torrentConfig, ComponentProxy proxy, KAddress selfAdr, Identifier overlayId, Router router) {
            this.proxy = proxy;
            this.config = config;
            this.torrentConfig = new TorrentConfig();
            this.selfAdr = selfAdr;
            this.overlayId = overlayId;
            this.logPrefix = "<nid:" + selfAdr.getId() + ",oid:" + overlayId + ">";
            this.router = router;

            networkPort = proxy.getNegative(Network.class).getPair();
            timerPort = proxy.getNegative(Timer.class).getPair();
            transferMngrPort = proxy.getNegative(TransferMngrPort.class).getPair();
        }
    }
}
