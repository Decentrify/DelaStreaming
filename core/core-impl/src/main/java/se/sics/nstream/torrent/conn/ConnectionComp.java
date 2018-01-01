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
 * GNU General Public License for more manifestDef.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
 */
package se.sics.nstream.torrent.conn;

import com.google.common.base.Optional;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import javassist.NotFoundException;
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
import se.sics.kompics.timer.Timer;
import se.sics.kompics.util.Identifiable;
import se.sics.kompics.util.Identifier;
import se.sics.ktoolbox.util.identifiable.overlay.OverlayId;
import se.sics.ktoolbox.util.network.KAddress;
import se.sics.ktoolbox.util.network.KContentMsg;
import se.sics.ktoolbox.util.network.KHeader;
import se.sics.ktoolbox.util.network.basic.BasicContentMsg;
import se.sics.ktoolbox.util.network.basic.BasicHeader;
import se.sics.ktoolbox.util.result.Result;
import se.sics.nstream.ConnId;
import se.sics.nstream.FileId;
import se.sics.nstream.TorrentIds;
import se.sics.nstream.torrent.conn.event.CloseTransfer;
import se.sics.nstream.torrent.conn.event.DetailedState;
import se.sics.nstream.torrent.conn.event.OpenTransfer;
import se.sics.nstream.torrent.conn.event.Seeder;
import se.sics.nstream.torrent.conn.msg.NetCloseTransfer;
import se.sics.nstream.torrent.conn.msg.NetConnect;
import se.sics.nstream.torrent.conn.msg.NetDetailedState;
import se.sics.nstream.torrent.conn.msg.NetOpenTransfer;
import se.sics.nstream.transfer.MyTorrent.ManifestDef;
import se.sics.nutil.network.bestEffort.event.BestEffortMsg;
import se.sics.nutil.tracking.load.NetworkQueueLoadProxy;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class ConnectionComp extends ComponentDefinition {

    private static final Logger LOG = LoggerFactory.getLogger(ConnectionComp.class);
    private String logPrefix;

    private static final int DEFAULT_RETRIES = 5;
    //**************************************************************************
    Negative<ConnectionPort> connPort = provides(ConnectionPort.class);
    Positive<Network> networkPort = requires(Network.class);
    Positive<Timer> timerPort = requires(Timer.class);
    //**************************************************************************
    private final OverlayId torrentId;
    private final KAddress selfAdr;
    //**************************************************************************
    private final NetworkQueueLoadProxy networkQueueLoad;
    //**************************************************************************
    private LeecherConnectionState leecherConnState;
    private SeederConnectionState seederConnState;

    public ConnectionComp(Init init) {
        torrentId = init.torrentId;
        selfAdr = init.selfAdr;
        logPrefix = "<nid:" + selfAdr.getId() + ",oid:" + torrentId + "> ";

        networkQueueLoad = NetworkQueueLoadProxy.instance("load_conn" + logPrefix, proxy, config(), 
          Optional.fromNullable((String)null));
        seederConnState = new SeederConnectionState();

        subscribe(handleStart, control);
        subscribe(handlePeerConnect, connPort);
        subscribe(handleSetDetailedState, connPort);
        subscribe(handleOpenTransferLeecher, connPort);
        subscribe(handleOpenTransferSeeder, connPort);
        subscribe(handleCloseTransfer, connPort);
        subscribe(handleNetworkTimeouts, networkPort);

        subscribe(handleNetConnectionReq, networkPort);
        subscribe(handleNetConnectionResp, networkPort);
        subscribe(handleNetDetailedStateReq, networkPort);
        subscribe(handleNetDetailedStateResp, networkPort);
        subscribe(handleNetOpenTransferReq, networkPort);
        subscribe(handleNetOpenTransferResp, networkPort);
        subscribe(handleNetCloseTransfer, networkPort);
    }

    Handler handleStart = new Handler<Start>() {

        @Override
        public void handle(Start event) {
            LOG.info("{}starting", logPrefix);
            networkQueueLoad.start();
        }
    };

    @Override
    public void tearDown() {
        networkQueueLoad.tearDown();
    }
    //**************************************************************************
    Handler handlePeerConnect = new Handler<Seeder.Connect>() {
        @Override
        public void handle(Seeder.Connect req) {
            seederConnState.connect(req);
        }
    };

    Handler handleSetDetailedState = new Handler<DetailedState.Set>() {
        @Override
        public void handle(DetailedState.Set req) {
            leecherConnState = new LeecherConnectionState(req.manifestDef);
        }
    };

    Handler handleOpenTransferLeecher = new Handler<OpenTransfer.LeecherRequest>() {
        @Override
        public void handle(OpenTransfer.LeecherRequest req) {
            seederConnState.openTransfer(req);
        }
    };
    Handler handleOpenTransferSeeder = new Handler<OpenTransfer.SeederResponse>() {
        @Override
        public void handle(OpenTransfer.SeederResponse event) {
            leecherConnState.openTransferResp(event);
        }
    };

    Handler handleCloseTransfer = new Handler<CloseTransfer.Request>() {
        @Override
        public void handle(CloseTransfer.Request req) {
            LOG.info("{}closing conn:{}", logPrefix, req);
            if (req.connId.leecher) {
                seederConnState.localClose(req.connId);
            } else {
                leecherConnState.localClose(req.connId);
            }
        }
    };

    //**************************************************************************
    private void simpleUDPSend(Identifiable content, KAddress target) {
        LOG.trace("{}sending:{} to:{}", new Object[]{logPrefix, content, target});
        KHeader header = new BasicHeader(selfAdr, target, Transport.UDP);
        KContentMsg msg = new BasicContentMsg(header, content);
        trigger(msg, networkPort);
    }

    private void bestEffortUDPSend(Identifiable content, KAddress target, int retries) {
        simpleUDPSend(new BestEffortMsg.Request<>(content, retries, 1000), target);
    }

    private void answerNetwork(KContentMsg msg, KompicsEvent content) {
        LOG.trace("{}answering:{} to:{}", new Object[]{logPrefix, content, msg.getHeader().getSource()});
        KContentMsg resp = msg.answer(content);
        trigger(resp, networkPort);
    }

    ClassMatchedHandler handleNetworkTimeouts
            = new ClassMatchedHandler<BestEffortMsg.Timeout, KContentMsg<KAddress, KHeader<KAddress>, BestEffortMsg.Timeout>>() {
                @Override
                public void handle(BestEffortMsg.Timeout content, KContentMsg<KAddress, KHeader<KAddress>, BestEffortMsg.Timeout> context) {
                    LOG.trace("{}timed out:{}", logPrefix, content);
                    Object baseContent = content.extractValue();
                    Identifier netReqId = content.extractValue().getId();
                    KAddress source = context.getHeader().getSource();

                    if (baseContent instanceof NetConnect.Request) {
                        seederConnState.connectTimeout(netReqId);
                    } else if (baseContent instanceof NetDetailedState.Request) {
                        seederConnState.detailedStateTimeout(netReqId, source);
                    } else if (baseContent instanceof NetOpenTransfer.Request) {
                        seederConnState.openTransferTimeout(netReqId, source);
                    } else {
                        throw new RuntimeException("ups");
                    }
                }
            };
    //**************************************************************************
    ClassMatchedHandler handleNetConnectionReq
            = new ClassMatchedHandler<NetConnect.Request, KContentMsg<KAddress, KHeader<KAddress>, NetConnect.Request>>() {
                @Override
                public void handle(NetConnect.Request content, KContentMsg<KAddress, KHeader<KAddress>, NetConnect.Request> context) {
                    LOG.trace("{}received:{}", logPrefix, content);
                    if (leecherConnState != null) {
                        leecherConnState.connect(context, content);
                    }
                }
            };
    ClassMatchedHandler handleNetConnectionResp
            = new ClassMatchedHandler<NetConnect.Response, KContentMsg<KAddress, KHeader<KAddress>, NetConnect.Response>>() {
                @Override
                public void handle(NetConnect.Response content, KContentMsg<KAddress, KHeader<KAddress>, NetConnect.Response> context) {
                    LOG.trace("{}received:{}", logPrefix, content);
                    seederConnState.connectResp(content.getId(), content.result);
                }
            };
    ClassMatchedHandler handleNetDetailedStateReq
            = new ClassMatchedHandler<NetDetailedState.Request, KContentMsg<KAddress, KHeader<KAddress>, NetDetailedState.Request>>() {
                @Override
                public void handle(NetDetailedState.Request content, KContentMsg<KAddress, KHeader<KAddress>, NetDetailedState.Request> context) {
                    LOG.trace("{}received:{}", logPrefix, content);
                    LOG.trace("{}received:{}", logPrefix, content);
                    if (leecherConnState != null) {
                        leecherConnState.detailedState(context, content);
                    }
                }
            };

    ClassMatchedHandler handleNetDetailedStateResp
            = new ClassMatchedHandler<NetDetailedState.Response, KContentMsg<KAddress, KHeader<KAddress>, NetDetailedState.Response>>() {
                @Override
                public void handle(NetDetailedState.Response content, KContentMsg<KAddress, KHeader<KAddress>, NetDetailedState.Response> context) {
                    LOG.trace("{}received:{}", logPrefix, content);
                    if (leecherConnState == null) {
                        leecherConnState = new LeecherConnectionState(content.manifestDef);
                        seederConnState.detailedStateResp(content.getId(), content.manifestDef);
                    }
                }
            };

    ClassMatchedHandler handleNetOpenTransferReq
            = new ClassMatchedHandler<NetOpenTransfer.Request, KContentMsg<KAddress, KHeader<KAddress>, NetOpenTransfer.Request>>() {
                @Override
                public void handle(NetOpenTransfer.Request content, KContentMsg<KAddress, KHeader<KAddress>, NetOpenTransfer.Request> context) {
                    LOG.trace("{}received:{}", logPrefix, content);
                    if (leecherConnState != null) {
                        leecherConnState.openTransfer(context.getHeader().getSource(), content.fileId, context);
                    }
                }
            };

    ClassMatchedHandler handleNetOpenTransferResp
            = new ClassMatchedHandler<NetOpenTransfer.Response, KContentMsg<KAddress, KHeader<KAddress>, NetOpenTransfer.Response>>() {
                @Override
                public void handle(NetOpenTransfer.Response content, KContentMsg<KAddress, KHeader<KAddress>, NetOpenTransfer.Response> context) {
                    LOG.trace("{}received:{}", logPrefix, content);
                    seederConnState.openTransferResp(content.getId(), content.result);
                }
            };

    ClassMatchedHandler handleNetCloseTransfer
            = new ClassMatchedHandler<NetCloseTransfer, KContentMsg<KAddress, KHeader<KAddress>, NetCloseTransfer>>() {
                @Override
                public void handle(NetCloseTransfer content, KContentMsg<KAddress, KHeader<KAddress>, NetCloseTransfer> context) {
                    LOG.trace("{}received:{}", logPrefix, context);
                    KAddress peer = context.getHeader().getSource();
                    if (content.leecher) {
                        leecherConnState.remoteClose(content.fileId, peer.getId());
                    } else {
                        seederConnState.remoteClose(content.fileId, peer.getId());
                    }
                }
            };

    //**************************************************************************
    public static class Init extends se.sics.kompics.Init<ConnectionComp> {

        public final OverlayId torrentId;
        public final KAddress selfAdr;

        public Init(OverlayId torrentId, KAddress selfAdr) {
            this.torrentId = torrentId;
            this.selfAdr = selfAdr;
        }
    }

    public class LeecherConnectionState {

        private final ManifestDef manifestDef;
        //<peerId, peer>
        private final Map<Identifier, KAddress> connected = new HashMap<>();
        //<localReqId, netReq>
        private final Map<Identifier, KContentMsg> pendingOpenTransfer = new HashMap<>();

        public LeecherConnectionState(ManifestDef manifestDef) {
            this.manifestDef = manifestDef;
        }

        public void refreshConnections() {

        }

        public void connect(KContentMsg<KAddress, ?, ?> msg, NetConnect.Request req) {
            KAddress peer = msg.getHeader().getSource();
            LOG.info("{}connected to leecher:{}", logPrefix, peer);
            connected.put(peer.getId(), peer);
            answerNetwork(msg, req.answer(true));
        }

        public void detailedState(KContentMsg msg, NetDetailedState.Request req) {
            answerNetwork(msg, req.success(manifestDef));
        }

        public void openTransfer(KAddress peer, FileId fileId, KContentMsg msg) {
            ConnId connId = TorrentIds.connId(fileId, peer.getId(), false);
            OpenTransfer.SeederRequest localReq = new OpenTransfer.SeederRequest(peer, connId);
            pendingOpenTransfer.put(localReq.getId(), msg);
            trigger(localReq, connPort);
        }

        public void openTransferResp(OpenTransfer.SeederResponse resp) {
            KContentMsg<?, ?, NetOpenTransfer.Request> msg = pendingOpenTransfer.remove(resp.getId());
            answerNetwork(msg, msg.getContent().answer(resp.result));
        }

        public void localClose(ConnId connId) {
            KAddress peer = connected.get(connId.peerId);
            if (peer != null) {
                simpleUDPSend(new NetCloseTransfer(connId.fileId, !connId.leecher), peer);
            }
        }

        public void remoteClose(FileId fileId, Identifier peerId) {
            ConnId connId = TorrentIds.connId(fileId, peerId, true);
            trigger(new CloseTransfer.Indication(connId), connPort);
        }
    }

    public class SeederConnectionState {

        private final Map<Identifier, KAddress> suspected = new HashMap<>();
        private final TreeMap<Identifier, KAddress> connected = new TreeMap<>();
        private final Map<Identifier, Seeder.Connect> connectedReq = new HashMap<>();
        //**********************************************************************
        private final Map<Identifier, Seeder.Connect> pendingConnect = new HashMap<>();
        private final Map<Identifier, OpenTransfer.LeecherRequest> pendingOpenTransfer = new HashMap<>();

        public void refreshConnections(Optional<KAddress> connectionCandidate) {
            if (connectionCandidate.isPresent()) {
            }
        }

        //**********************************************************************
        public void connect(Seeder.Connect req) {
            LOG.debug("{}connecting to seeder:{}", logPrefix, req.peer);
            NetConnect.Request netReq = new NetConnect.Request(torrentId);
            pendingConnect.put(netReq.getId(), req);
            bestEffortUDPSend(netReq, req.peer, DEFAULT_RETRIES);
        }

        public void connectResp(Identifier reqId, boolean result) {
            Seeder.Connect req = pendingConnect.remove(reqId);
            if (req == null) {
                LOG.trace("{}late req:{}", logPrefix, reqId);
                return;
            }
            if (result) {
                LOG.info("{}connected to seeder:{}", logPrefix, req.peer);
                connected.put(req.peer.getId(), req.peer);
                connectedReq.put(req.peer.getId(), req);
                answer(req, req.success());
                if (leecherConnState == null) {
                    detailedState();
                }
            } else {
                throw new RuntimeException("ups");
            }
        }

        public void connectTimeout(Identifier reqId) {
            Seeder.Connect req = pendingConnect.remove(reqId);
            if (req == null) {
                LOG.trace("{}late timeout:{}", logPrefix, reqId);
                return;
            }
            LOG.info("{}connection to seeder:{} timed out", logPrefix, req.peer);
            answer(req, req.timeout());
        }

        //**********************************************************************
        public void detailedState() {
            if (connected.isEmpty()) {
                LOG.info("{}detailed state - no connection", logPrefix);
                trigger(new DetailedState.Deliver(Result.timeout(new NotFoundException("manifest def not found"))), connPort);
                return;
            }
            KAddress peer = connected.firstEntry().getValue();
            LOG.debug("{}detailed state - requesting from:{}", logPrefix, peer);
            NetDetailedState.Request netReq = new NetDetailedState.Request(torrentId);
            bestEffortUDPSend(netReq, peer, DEFAULT_RETRIES);
        }

        public void detailedStateResp(Identifier reqId, ManifestDef manifestDef) {
            LOG.info("{}detailed state - received", logPrefix);
            trigger(new DetailedState.Deliver(Result.success(manifestDef)), connPort);
        }

        public void detailedStateTimeout(Identifier reqId, KAddress peer) {
            Seeder.Connect connectReq = connectedReq.get(peer.getId());
            if (connectReq != null) {
                connected.remove(peer.getId());
                suspected.put(peer.getId(), peer);
                LOG.debug("{}suspect:{}", logPrefix, peer);
                answer(connectReq, connectReq.suspect());
            }
            detailedState();
        }

        //**********************************************************************
        public void openTransfer(OpenTransfer.LeecherRequest req) {
            LOG.debug("{}transfer leecher - requesting from:{}", logPrefix, req.peer);
            NetOpenTransfer.Request netReq = new NetOpenTransfer.Request(req.connId.fileId);
            pendingOpenTransfer.put(netReq.getId(), req);
            bestEffortUDPSend(netReq, req.peer, DEFAULT_RETRIES);
        }

        public void openTransferResp(Identifier reqId, boolean result) {
            OpenTransfer.LeecherRequest req = pendingOpenTransfer.remove(reqId);
            if (req == null) {
                LOG.trace("{}transfer leecher - late:{}", logPrefix, reqId);
                return;
            }
            LOG.info("{}transfer definition leecher - received from:{}", logPrefix, req.peer);
            answer(req, req.answer(result));
        }

        public void openTransferTimeout(Identifier reqId, KAddress peer) {
            OpenTransfer.LeecherRequest req = pendingOpenTransfer.remove(reqId);
            if (req == null) {
                LOG.trace("{}transfer definition leecher - late timeout:{}", logPrefix, reqId);
                return;
            }
            Seeder.Connect connectReq = connectedReq.get(peer.getId());
            if (connectReq != null) {
                connected.remove(peer.getId());
                suspected.put(peer.getId(), peer);
                LOG.debug("{}suspect:{}", logPrefix, peer);
                answer(connectReq, connectReq.suspect());
            }
            answer(req, req.timeout());
        }

        public void localClose(ConnId connId) {
            KAddress peer = connected.get(connId.peerId);
            if (peer != null) {
                simpleUDPSend(new NetCloseTransfer(connId.fileId, !connId.leecher), peer);
            }
        }

        public void remoteClose(FileId fileId, Identifier peerId) {
            ConnId connId = TorrentIds.connId(fileId, peerId, false);
            trigger(new CloseTransfer.Indication(connId), connPort);
        }
    }
}
