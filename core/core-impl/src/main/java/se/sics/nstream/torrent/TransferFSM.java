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
import se.sics.ktoolbox.util.result.DelayedExceptionSyncHandler;
import se.sics.ktoolbox.util.result.Result;
import se.sics.nstream.transfer.MultiFileTransfer;
import se.sics.nstream.transfer.Transfer;
import se.sics.nstream.transfer.TransferMngrPort;
import se.sics.nstream.transfer.event.TransferTorrent;
import se.sics.nstream.util.TransferDetails;

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

    public void handleTorrentReq(KContentMsg msg, TransferTorrent.Request req) {
        LOG.trace("{}received:{}", cs.logPrefix, msg);
        answer(msg, req.busy());
    }

    public void handleTorrentTimeout(TorrentTimeout.Metadata timeout) {
    }

    public void handleTorrentResp(KContentMsg msg, TransferTorrent.Response resp) {
    }

    public void handleExtendedTorrentResp(Transfer.DownloadResponse resp) {
    }

    protected void request(KAddress partner, KompicsEvent content, ScheduleTimeout timeout) {
        KHeader header = new BasicHeader(cs.selfAdr, partner, Transport.UDP);
        KContentMsg msg = new BasicContentMsg(header, content);
        LOG.trace("{}sending:{}", cs.logPrefix, msg);
        cs.proxy.trigger(msg, cs.networkPort);
        cs.proxy.trigger(timeout, cs.timerPort);
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
        public void handleTorrentResp(KContentMsg msg, TransferTorrent.Response resp) {
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
            TransferTorrent.Request req = new TransferTorrent.Request(cs.overlayId);
            ScheduleTimeout timeout = scheduleTorrentTimeout(req.eventId, target);
            request(target, req, timeout);
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
        public void handleTorrentReq(KContentMsg msg, TransferTorrent.Request req) {
            answer(msg, req.success(torrentByte));
        }
    }

    public static class DownloadState extends TransferState {

        public DownloadState(CommonState cs, byte[] torrentByte, TransferDetails transferDetails) {
            super(cs, torrentByte, transferDetails, new MultiFileTransfer(cs.config, cs.proxy, cs.exSyncHandler, transferDetails, false));
        }

        @Override
        public TransferFSM next() {
            return this;
        }

        @Override
        public void report() {
            LOG.info("{}downloading/uploading", cs.logPrefix);
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
