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
import se.sics.nstream.storage.buffer.WriteResult;
import se.sics.nstream.storage.cache.KHint;
import se.sics.nstream.torrent.event.HashGet;
import se.sics.nstream.torrent.event.TorrentGet;
import se.sics.nstream.torrent.event.timeout.TorrentTimeout;
import se.sics.nstream.transfer.MultiFileTransfer;
import se.sics.nstream.transfer.Transfer;
import se.sics.nstream.transfer.TransferMngr;
import se.sics.nstream.transfer.TransferMngrPort;
import se.sics.nstream.util.FileBaseDetails;
import se.sics.nstream.util.TransferDetails;
import se.sics.nstream.util.result.HashReadCallback;
import se.sics.nstream.util.result.HashWriteCallback;

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

    public void handleHashReq(KContentMsg<KAddress, KHeader<KAddress>, HashGet.Request> msg, HashGet.Request req) {
    }

    public void handleHashResp(KContentMsg<KAddress, KHeader<KAddress>, HashGet.Response> msg, HashGet.Response resp) {
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

        @Override
        public void handleHashReq(KContentMsg<KAddress, KHeader<KAddress>, HashGet.Request> msg, HashGet.Request req) {
            for (Map.Entry<String, KHint.Summary> e : req.cacheHints.entrySet()) {
                TransferMngr.Reader reader = transferMngr.readFrom(e.getKey());
                FileBaseDetails baseDetails = transferDetails.base.baseDetails.get(e.getKey());
                reader.setFutureReads(msg.getHeader().getSource().getId(), e.getValue().expand(baseDetails));
            }
            TransferMngr.Reader hashReader = transferMngr.readFrom(req.fileName);
            final PendingHashReq phr = new PendingHashReq(msg);
            pendingHashes.put(req.eventId, phr);
            for (final Integer hash : req.hashes) {
                if (hashReader.hasHash(hash)) {
                    hashReader.readHash(hash, new HashReadCallback() {
                        @Override
                        public boolean fail(Result<KReference<byte[]>> result) {
                            //TODO Alex - exception
                            throw new RuntimeException("ups...");
                        }

                        @Override
                        public boolean success(Result<KReference<byte[]>> result) {
                            phr.hash(hash, result.getValue());
                            if (phr.canAnswer()) {
                                answer(phr.answer());
                            }
                            return true;
                        }
                    });
                } else {
                    phr.missingHash(hash);
                    if (phr.canAnswer()) {
                        answer(phr.answer());
                    }
                }
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

        public DownloadState(CommonState cs, byte[] torrentByte, TransferDetails transferDetails) {
            super(cs, torrentByte, transferDetails, new MultiFileTransfer(cs.config, cs.proxy, cs.exSyncHandler, transferDetails, false));
        }

        @Override
        public void start() {
            super.start();
            download();
        }

        @Override
        public TransferFSM next() {
            return this;
        }

        @Override
        public void handleHashResp(KContentMsg msg, HashGet.Response resp) {
            if (resp.status.isSuccess()) {
                TransferMngr.Writer hashWriter = transferMngr.writeTo(resp.fileName);
                for (Map.Entry<Integer, ByteBuffer> hash : resp.hashes.entrySet()) {
                    LOG.debug("{}received hash:{}", cs.logPrefix, hash.getKey());
                    hashWriter.writeHash(hash.getKey(), hash.getValue().array(), new HashWriteCallback() {
                        @Override
                        public boolean fail(Result<WriteResult> result) {
                            //TODO Alex - exception
                            throw new RuntimeException("ups...");
                        }

                        @Override
                        public boolean success(Result<WriteResult> result) {
                            return true;
                        }
                    });
                }
                for (Integer missingHash : resp.missingHashes) {
                    //TODO Alex - exception
                    throw new RuntimeException("ups...");
                }
            } else {
                //TODO Alex - exception
                throw new RuntimeException("ups...");
            }
        }

        @Override
        public void report() {
            LOG.info("{}downloading/uploading", cs.logPrefix);
        }

        private void download() {
            LOG.trace("{}downloading...");
            Map<String, KHint.Summary> cacheHints = new HashMap<>();

            Pair<String, TransferMngr.Writer> file = transferMngr.nextOngoing();
            KHint.Summary hint = file.getValue1().getFutureReads(TransferConfig.hintCacheSize);
            cacheHints.put(file.getValue0(), hint);

            KAddress partner = cs.router.randomPartner();

            Set<Integer> hashes = file.getValue1().nextHashes();
            if (hashes.isEmpty()) {
                throw new RuntimeException("ups");
            }
            request(partner, new HashGet.Request(cs.overlayId, cacheHints, file.getValue0(), 0, hashes), scheduleHashTimeout(partner));
        }

        private ScheduleTimeout scheduleHashTimeout(KAddress target) {
            ScheduleTimeout st = new ScheduleTimeout(TransferConfig.hashTimeout);
            TorrentTimeout.Hash tt = new TorrentTimeout.Hash(st, target);
            st.setTimeoutEvent(tt);
            pendingHashes.put(tt.getId(), tt.getTimeoutId());
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

        private String logPrefix = "";

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
