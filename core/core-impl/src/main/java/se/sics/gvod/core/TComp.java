///*
// * Copyright (C) 2009 Swedish Institute of Computer Science (SICS) Copyright (C)
// * 2009 Royal Institute of Technology (KTH)
// *
// * GVoD is free software; you can redistribute it and/or
// * modify it under the terms of the GNU General Public License
// * as published by the Free Software Foundation; either version 2
// * of the License, or (at your option) any later version.
// *
// * This program is distributed in the hope that it will be useful,
// * but WITHOUT ANY WARRANTY; without even the implied warranty of
// * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// * GNU General Public License for more details.
// *
// * You should have received a copy of the GNU General Public License
// * along with this program; if not, write to the Free Software
// * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
// */
//package se.sics.gvod.core;
//
//import java.nio.ByteBuffer;
//import java.util.HashMap;
//import java.util.List;
//import java.util.Map;
//import java.util.Set;
//import java.util.UUID;
//import java.util.concurrent.atomic.AtomicInteger;
//import org.javatuples.Pair;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//import se.sics.gvod.common.event.vod.Connection;
//import se.sics.gvod.common.event.vod.Download;
//import se.sics.gvod.common.util.VodDescriptor;
//import se.sics.gvod.core.util.TorrentDetails;
//import se.sics.kompics.ClassMatchedHandler;
//import se.sics.kompics.ComponentDefinition;
//import se.sics.kompics.Handler;
//import se.sics.kompics.Negative;
//import se.sics.kompics.Positive;
//import se.sics.kompics.Start;
//import se.sics.kompics.network.Network;
//import se.sics.kompics.network.Transport;
//import se.sics.kompics.timer.CancelPeriodicTimeout;
//import se.sics.kompics.timer.CancelTimeout;
//import se.sics.kompics.timer.SchedulePeriodicTimeout;
//import se.sics.kompics.timer.ScheduleTimeout;
//import se.sics.kompics.timer.Timeout;
//import se.sics.kompics.timer.Timer;
//import se.sics.ktoolbox.croupier.CroupierPort;
//import se.sics.ktoolbox.util.identifiable.Identifier;
//import se.sics.ktoolbox.util.managedStore.core.FileMngr;
//import se.sics.ktoolbox.util.managedStore.core.HashMngr;
//import se.sics.ktoolbox.util.managedStore.core.impl.TransferMngr;
//import se.sics.ktoolbox.util.network.KAddress;
//import se.sics.ktoolbox.util.network.KContentMsg;
//import se.sics.ktoolbox.util.network.KHeader;
//import se.sics.ktoolbox.util.network.basic.BasicContentMsg;
//import se.sics.ktoolbox.util.network.basic.BasicHeader;
//import se.sics.ktoolbox.util.network.basic.DecoratedHeader;
//import se.sics.ktoolbox.util.overlays.view.OverlayViewUpdatePort;
//
///**
// * @author Alex Ormenisan <aaor@kth.se>
// */
//public class TComp extends ComponentDefinition {
//
//    private static final Logger LOG = LoggerFactory.getLogger(TComp.class);
//    private String logPrefix;
//
//    //****************************CONNECTIONS***********************************
//    //***********************EXTERNAL_CONNECT_TO********************************
//    private Positive<Timer> timerPort = requires(Timer.class);
//    private Positive<Network> networkPort = requires(Network.class);
//    private Positive<CroupierPort> croupierPort = requires(CroupierPort.class);
//    private Negative<OverlayViewUpdatePort> viewUpdatePort = provides(OverlayViewUpdatePort.class);
//    //**************************EXTERNAL_STATE**********************************
//    private final KAddress selfAdr;
//    private final Identifier overlayId;
//    private final TKCWrapper torrentConfig;
//    private final AtomicInteger playPos;
//    //**************************INTERNAL_STATE**********************************
//    private VodDescriptor selfDesc;
//    private final CommunicationMngr commMngr;
//    private final FileMngr fileMngr;
//    private final HashMngr hashMngr;
//    private final TransferMngr transferMngr;
//    private final DownloadMngr dwnlMngr;
//    private final UploadMngr upldMngr;
//    private final ConnectionHandling connMngr;
//
//    public TComp(Init init) {
//        selfAdr = init.selfAdr;
//        overlayId = init.overlayId;
//        logPrefix = "<nid:" + selfAdr.getId() + ", oid:" + overlayId + ">";
//        LOG.info("{}initiating...", logPrefix);
//
//        torrentConfig = new TKCWrapper(config());
//        commMngr = new CommunicationMngr();
//
//        fileMngr = init.torrentDetails.fileMngr();
//        hashMngr = init.torrentDetails.hashMngr();
//        transferMngr = init.torrentDetails.transferMngr();
//
//        dwnlMngr = new DownloadMngr();
//        dwnlMngr.newPartners(init.partners);
//        upldMngr = new UploadMngr();
//        connMngr = new ConnectionHandling();
//
//        subscribe(handleStart, control);
//    }
//
//    Handler handleStart = new Handler<Start>() {
//        @Override
//        public void handle(Start event) {
//            LOG.info("{}starting...", logPrefix);
//        }
//    };
//
//    private void sendNetwork(KAddress destination, Object content) {
//        KHeader msgHeader = new DecoratedHeader(new BasicHeader(selfAdr, destination, Transport.UDP), overlayId);
//        KContentMsg msg = new BasicContentMsg(msgHeader, content);
//        LOG.trace("{}sending:{}", logPrefix, msg);
//        trigger(msg, networkPort);
//    }
//
//    private void answerNetwork(KContentMsg msg, Object content) {
//        KContentMsg resp = msg.answer(content);
//        LOG.trace("{}sending:{}", logPrefix, resp);
//        trigger(resp, networkPort);
//    }
//
//    public static class Init extends se.sics.kompics.Init<TComp> {
//
//        public final KAddress selfAdr;
//        public final Identifier overlayId;
//        public final List<KAddress> partners;
//        public final TorrentDetails torrentDetails;
//
//        public Init(KAddress selfAdr, Identifier overlayId, List<KAddress> partners, TorrentDetails torrentDetails) {
//            this.selfAdr = selfAdr;
//            this.overlayId = overlayId;
//            this.partners = partners;
//            this.torrentDetails = torrentDetails;
//        }
//    }
//
//    class TransferHandling {
//
//        ClassMatchedHandler handleNetHashRequest
//                = new ClassMatchedHandler<Download.HashRequest, BasicContentMsg<KAddress, DecoratedHeader<KAddress>, Download.HashRequest>>() {
//
//                    @Override
//                    public void handle(Download.HashRequest content, BasicContentMsg<KAddress, DecoratedHeader<KAddress>, Download.HashRequest> container) {
//                        KAddress target = container.getHeader().getSource();
//                        LOG.debug("{}received hash:{} request from:{}", new Object[]{logPrefix, content.targetPos, target.getId()});
//                        if (!upldMngr.hasConnection(target)) {
//                            LOG.warn("{}no upload connection tracked to:{}, dropping and closing", new Object[]{logPrefix, target.getId(), content.hashes});
//                            answerNetwork(container, new Connection.Close(overlayId, true));
//                            return;
//                        }
//                        Pair<Map<Integer, ByteBuffer>, Set<Integer>> result = hashMngr.readHashes(content.hashes);
//                        answerNetwork(container, content.success(result.getValue0(), result.getValue1()));
//                        upldMngr.useDataSlot(target);
//                    }
//                };
//
//        ClassMatchedHandler handleNetDataRequest
//                = new ClassMatchedHandler<Download.DataRequest, BasicContentMsg<KAddress, DecoratedHeader<KAddress>, Download.DataRequest>>() {
//
//                    @Override
//                    public void handle(Download.DataRequest content, BasicContentMsg<KAddress, DecoratedHeader<KAddress>, Download.DataRequest> container) {
//                        KAddress target = container.getHeader().getSource();
//                        LOG.debug("{}received data:{} request from:{}", new Object[]{logPrefix, content.pieceId, target.getId()});
//                        if (!upldMngr.hasConnection(target)) {
//                            LOG.warn("{}no upload connection tracked to:{}, dropping and closing", new Object[]{logPrefix, target.getId(), content.pieceId});
//                            answerNetwork(container, new Connection.Close(overlayId, true));
//                            return;
//                        }
//                        if (fileMngr.hasPiece(content.pieceId)) {
//                            ByteBuffer piece = fileMngr.readPiece(content.pieceId);
//                            answerNetwork(container, content.success(piece));
//                            upldMngr.useDataSlot(target);
//                        } else {
//                            answerNetwork(container, content.missingPiece());
//                        }
//                    }
//                };
//
//        ClassMatchedHandler handleHashResponse
//                = new ClassMatchedHandler<Download.HashResponse, BasicContentMsg<KAddress, DecoratedHeader<KAddress>, Download.HashResponse>>() {
//
//                    @Override
//                    public void handle(Download.HashResponse content, BasicContentMsg<KAddress, DecoratedHeader<KAddress>, Download.HashResponse> container) {
//                        KAddress target = container.getHeader().getSource();
//                        LOG.debug("{}received hash:{} response from:{}", new Object[]{logPrefix, content.targetPos, target.getId()});
//
//                        if (!commMngr.cancelPendingHashTimeout(content.getId())) {
//                            LOG.info("{}hash from:{} - posibly late", logPrefix, target.getId());
//                            return;
//                        }
//                        switch (content.status) {
//                            case SUCCESS:
//                                LOG.trace("{}SUCCESS hashes:{} missing hashes:{}", new Object[]{logPrefix, content.hashes.keySet(), content.missingHashes});
//                                transferMngr.writeHashes(content.hashes, content.missingHashes);
//                                dwnlMngr.releaseDataSlot(target, true);
//                                download();
//                                return;
//                            case TIMEOUT:
//                            case BUSY:
//                                LOG.debug("{}BUSY/TIMEOUT hashes:{}", logPrefix, content.missingHashes);
//                                transferMngr.writeHashes(content.hashes, content.missingHashes);
//                                dwnlMngr.releaseDataSlot(target, false);
//                                return;
//                            default:
//                                LOG.warn("{}illegal status:{}, ignoring", new Object[]{logPrefix, content.status});
//                        }
//                    }
//                };
//
//        ClassMatchedHandler handleNetDataResponse
//                = new ClassMatchedHandler<Download.DataResponse, BasicContentMsg<KAddress, DecoratedHeader<KAddress>, Download.DataResponse>>() {
//
//                    @Override
//                    public void handle(Download.DataResponse content, BasicContentMsg<KAddress, DecoratedHeader<KAddress>, Download.DataResponse> container) {
//                        KAddress target = container.getHeader().getSource();
//                        LOG.debug("{}received data:{} response from:{}", new Object[]{logPrefix, content.pieceId, target.getId()});
//                        
//                        if (!commMngr.cancelPendingPieceTimeout(content.getId())) {
//                            LOG.info("{}piece from:{} - posibly late", logPrefix, target.getId());
//                            return;
//                        }
//                        switch (content.status) {
//                            case SUCCESS:
//                                LOG.trace("{}SUCCESS piece:{}", new Object[]{logPrefix, content.pieceId});
//                                transferMngr.writePiece(content.pieceId, content.piece);
//                                dwnlMngr.releaseDataSlot(target, true);
//                                download();
//                                return;
//                            case TIMEOUT:
//                            case BUSY:
//                                LOG.debug("{}BUSY/TIMEOUT piece:{}", logPrefix, content.pieceId);
//                                transferMngr.resetPiece(content.pieceId);
//                                dwnlMngr.releaseDataSlot(target, false);
//                                return;
//                            default:
//                                LOG.warn("{} illegal status:{}, ignoring", new Object[]{logPrefix, content.status});
//                        }
//                    }
//                };
//
//        private void download() {
//            
//        }
//    }
//
//    class ConnectionHandling {
//
//        //fire and forget
//        Handler handleScheduledConnectionUpdate = new Handler<PeriodicConnUpdate>() {
//
//            @Override
//            public void handle(PeriodicConnUpdate event) {
//                LOG.debug("{}connection update", logPrefix);
//                for (KAddress partner : dwnlMngr.getPartners().values()) {
//                    sendNetwork(partner, new Connection.Update(overlayId, selfDesc, true));
//                }
//
//                for (KAddress partner : upldMngr.getPartners().values()) {
//                    sendNetwork(partner, new Connection.Update(overlayId, selfDesc, false));
//                }
//            }
//        };
//
//        ClassMatchedHandler handleConnectionRequest
//                = new ClassMatchedHandler<Connection.Request, BasicContentMsg<KAddress, DecoratedHeader<KAddress>, Connection.Request>>() {
//
//                    @Override
//                    public void handle(Connection.Request content, BasicContentMsg<KAddress, DecoratedHeader<KAddress>, Connection.Request> container) {
//                        KAddress partner = container.getHeader().getSource();
//                        LOG.info("{}received download connection request from:{}", new Object[]{logPrefix, partner.getId()});
//                        if (!upldMngr.hasConnection(partner) && !upldMngr.connect(partner, content.desc)) {
//                            LOG.info("{}reject upload connection to:{}", logPrefix, partner.getId());
//                            return;
//                        }
//                        LOG.info("{}accept upload connection to:{}", logPrefix, partner.getId());
//                        answerNetwork(container, content.accept(selfDesc));
//                    }
//                };
//
//        //TODO Alex - connection response status - currently a response means by default connection accepted
//        ClassMatchedHandler handleConnectionResponse
//                = new ClassMatchedHandler<Connection.Response, BasicContentMsg<KAddress, DecoratedHeader<KAddress>, Connection.Response>>() {
//
//                    @Override
//                    public void handle(Connection.Response content, BasicContentMsg<KAddress, DecoratedHeader<KAddress>, Connection.Response> container) {
//                        KAddress partner = container.getHeader().getSource();
//                        LOG.info("{}received download connection response from:{}", new Object[]{logPrefix, partner.getId()});
//                        if (dwnlMngr.hasConnection(partner) && !dwnlMngr.connect(partner, content.desc)) {
//                            LOG.info("{}close download connection from:{}", logPrefix, partner.getId());
//                            answerNetwork(container, new Connection.Close(overlayId, true));
//                            return;
//                        }
//                        LOG.info("{}ready download connection from:{}", logPrefix, partner.getId());
//                    }
//                };
//
//        ClassMatchedHandler handleConnectionClose
//                = new ClassMatchedHandler<Connection.Close, BasicContentMsg<KAddress, DecoratedHeader<KAddress>, Connection.Close>>() {
//
//                    @Override
//                    public void handle(Connection.Close content, BasicContentMsg<KAddress, DecoratedHeader<KAddress>, Connection.Close> container) {
//                        KAddress partner = container.getHeader().getSource();
//                        LOG.info("{}received close connection from:{}", new Object[]{logPrefix, partner.getId()});
//
//                        //from receiver perspective it is opposite
//                        if (!content.downloadConnection) {
//                            if (dwnlMngr.hasConnection(partner)) {
//                                dwnlMngr.close(partner);
//                            }
//                        } else {
//                            if (upldMngr.hasConnection(partner)) {
//                                upldMngr.close(partner);
//                            }
//                        }
//                    }
//                };
//
//        ClassMatchedHandler handleConnectionUpdate
//                = new ClassMatchedHandler<Connection.Update, BasicContentMsg<KAddress, DecoratedHeader<KAddress>, Connection.Update>>() {
//
//                    @Override
//                    public void handle(Connection.Update content, BasicContentMsg<KAddress, DecoratedHeader<KAddress>, Connection.Update> container) {
//                        KAddress partner = container.getHeader().getSource();
//
//                        //remember local view of connection is reverse to what the sender sees it 
//                        LOG.debug("{}received {} connection update from:{}", new Object[]{logPrefix, !content.downloadConnection ? "download" : "upload", partner.getId()});
//                        if (!content.downloadConnection) {
//                            if (upldMngr.hasConnection(partner)) {
//                                upldMngr.useControlSlot(partner);
//                            } else {
//                                LOG.info("{}no upload connection tracked - closing", logPrefix);
//                                answerNetwork(container, new Connection.Close(overlayId, true));
//                            }
//                        } else {
//                            if (dwnlMngr.hasConnection(partner)) {
//                                dwnlMngr.useControlSlot(partner);
//                            } else {
//                                LOG.info("{}no download connection tracked - closing", logPrefix);
//                                answerNetwork(container, new Connection.Close(overlayId, false));
//                            }
//                        }
//                    }
//                };
//    }
//
//    class DownloadMngr {
//
//        private Map<Identifier, KAddress> partners = new HashMap<>();
//        private Map<Identifier, ConnectionStatus> connectionStatus = new HashMap<>();
//        private Map<Identifier, VodDescriptor> partnerStatus = new HashMap<>();
//
//        private void addPartner(KAddress candidate, VodDescriptor candidateStatus) {
//            partners.put(candidate.getId(), candidate);
//            connectionStatus.put(candidate.getId(), new ConnectionStatus(torrentConfig.defaultConnectionThroughput));
//            partnerStatus.put(candidate.getId(), candidateStatus);
//        }
//
//        private void removePartner(KAddress partner) {
//            partners.remove(partner.getId());
//            connectionStatus.remove(partner.getId());
//            partnerStatus.remove(partner.getId());
//        }
//
//        public void newPartners(List<KAddress> newCandidates) {
//            for (KAddress candidate : newCandidates) {
//                addPartner(candidate, new VodDescriptor(0));
//            }
//        }
//
//        public void updateStatus(KAddress partner, VodDescriptor status) {
//            if (partners.containsKey(partner.getId())) {
//                partnerStatus.put(partner.getId(), status);
//            }
//        }
//
//        public Map<Identifier, KAddress> getPartners() {
//            return partners;
//        }
//
//        public boolean hasConnection(KAddress partner) {
//            return partners.containsKey(partner.getId());
//        }
//
//        public boolean connect(KAddress partner, VodDescriptor status) {
//            addPartner(partner, status);
//            return true;
//        }
//
//        public void close(KAddress partner) {
//            removePartner(partner);
//        }
//
//        public void useControlSlot(KAddress partner) {
//            ConnectionStatus cs = connectionStatus.get(partner.getId());
//            cs.useControlSlot();
//        }
//
//        public void useDataSlot(KAddress partner) {
//            ConnectionStatus cs = connectionStatus.get(partner.getId());
//            cs.useDataSlot();
//        }
//        
//        public void releaseDataSlot(KAddress partner, boolean used) {
//            ConnectionStatus cs = connectionStatus.get(partner.getId());
//            cs.releaseDataSlot(used);
//        }
//    }
//
//    class UploadMngr {
//
//        private Map<Identifier, KAddress> partners = new HashMap<>();
//        private Map<Identifier, ConnectionStatus> connectionStatus = new HashMap<>();
//        private Map<Identifier, VodDescriptor> partnerStatus = new HashMap<>();
//
//        private void addPartner(KAddress candidate, VodDescriptor candidateStatus) {
//            partners.put(candidate.getId(), candidate);
//            connectionStatus.put(candidate.getId(), new ConnectionStatus(torrentConfig.defaultConnectionThroughput));
//            partnerStatus.put(candidate.getId(), new VodDescriptor(0));
//        }
//
//        private void removePartner(KAddress partner) {
//            partners.remove(partner.getId());
//            connectionStatus.remove(partner.getId());
//            partnerStatus.remove(partner.getId());
//        }
//
//        public void updateStatus(KAddress partner, VodDescriptor status) {
//            if (partners.containsKey(partner.getId())) {
//                partnerStatus.put(partner.getId(), status);
//            }
//        }
//
//        public Map<Identifier, KAddress> getPartners() {
//            return partners;
//        }
//
//        public boolean hasConnection(KAddress partner) {
//            return partners.containsKey(partner.getId());
//        }
//
//        public boolean connect(KAddress partner, VodDescriptor status) {
//            addPartner(partner, status);
//            return true;
//        }
//
//        public void close(KAddress partner) {
//            removePartner(partner);
//        }
//
//        public void useControlSlot(KAddress partner) {
//            ConnectionStatus cs = connectionStatus.get(partner.getId());
//            cs.useControlSlot();
//        }
//
//        public void useDataSlot(KAddress partner) {
//            ConnectionStatus cs = connectionStatus.get(partner.getId());
//            cs.useDataSlot();
//            cs.releaseDataSlot(true);
//        }
//    }
//
//    class ConnectionStatus {
//
//        private int maxSlots;
//        private int usedSlots;
//        private int dataSlots;
//        private int controlSlots;
//
//        public ConnectionStatus(int maxSlots) {
//            this.maxSlots = maxSlots;
//            this.usedSlots = 0;
//        }
//
//        public boolean available() {
//            return usedSlots < maxSlots;
//        }
//
//        public void useDataSlot() {
//            usedSlots++;
//        }
//
//        public void releaseDataSlot(boolean used) {
//            usedSlots--;
//            if(used) {
//                dataSlots++;
//            }
//        }
//
//        public int resetDataSlots() {
//            int aux = dataSlots;
//            dataSlots = 0;
//            return aux;
//        }
//
//        public void useControlSlot() {
//            controlSlots++;
//        }
//
//        public int resetControlSlots() {
//            int aux = controlSlots;
//            controlSlots = 0;
//            return aux;
//        }
//    }
//
//    class CommunicationMngr {
//
//        private UUID torrentTId = null;
//        private UUID periodicConnUpdate = null;
//        private Map<Identifier, UUID> pendingPieces = new HashMap<>();
//        private Map<Identifier, UUID> pendingHashes = new HashMap<>();
//
//        public void scheduleTorrentTimeout() {
//            ScheduleTimeout st = new ScheduleTimeout(torrentConfig.defaultMsgTimeout);
//            Timeout t = new TorrentTimeout(st);
//            st.setTimeoutEvent(t);
//            trigger(st, timerPort);
//            torrentTId = t.getTimeoutId();
//        }
//
//        public void cancelTorrentTimeout() {
//            CancelTimeout ct = new CancelTimeout(torrentTId);
//            trigger(ct, timerPort);
//            torrentTId = null;
//        }
//
//        public void scheduleConnectionUpdateTimeout() {
//            SchedulePeriodicTimeout spt = new SchedulePeriodicTimeout(torrentConfig.defaultMsgTimeout, torrentConfig.defaultMsgTimeout);
//            Timeout t = new PeriodicConnUpdate(spt);
//            spt.setTimeoutEvent(t);
//            trigger(spt, timerPort);
//            periodicConnUpdate = t.getTimeoutId();
//        }
//
//        public void cancelConnectionUpdateTimeout() {
//            CancelPeriodicTimeout cpt = new CancelPeriodicTimeout(periodicConnUpdate);
//            trigger(cpt, timerPort);
//            periodicConnUpdate = null;
//        }
//
//        public void schedulePendingHashTimeout(Identifier eventId) {
//            ScheduleTimeout st = new ScheduleTimeout(torrentConfig.defaultMsgTimeout);
//            Timeout t = new PendingPieceTimeout(st, eventId);
//            st.setTimeoutEvent(t);
//            trigger(st, timerPort);
//            pendingHashes.put(eventId, t.getTimeoutId());
//        }
//
//        public boolean cancelPendingHashTimeout(Identifier pieceId) {
//            UUID tId = pendingHashes.remove(pieceId);
//            if (tId == null) {
//                LOG.trace("{}late timeout for hash:{}", logPrefix, pieceId);
//                return false;
//            }
//            CancelTimeout ct = new CancelTimeout(tId);
//            trigger(ct, timerPort);
//            return true;
//        }
//
//        public void schedulePendingPieceTimeout(Identifier eventId) {
//            ScheduleTimeout st = new ScheduleTimeout(torrentConfig.defaultMsgTimeout);
//            Timeout t = new PendingPieceTimeout(st, eventId);
//            st.setTimeoutEvent(t);
//            trigger(st, timerPort);
//            pendingPieces.put(eventId, t.getTimeoutId());
//        }
//
//        public boolean cancelPendingPieceTimeout(Identifier pieceId) {
//            UUID tId = pendingPieces.remove(pieceId);
//            if (tId == null) {
//                LOG.trace("{}late timeout for piece:{}", logPrefix, pieceId);
//                return false;
//            }
//            CancelTimeout ct = new CancelTimeout(tId);
//            trigger(ct, timerPort);
//            return true;
//        }
//    }
//
//    private class TorrentTimeout extends Timeout {
//
//        public TorrentTimeout(ScheduleTimeout st) {
//            super(st);
//        }
//    }
//
//    public class PeriodicConnUpdate extends Timeout {
//
//        public PeriodicConnUpdate(SchedulePeriodicTimeout schedule) {
//            super(schedule);
//        }
//    }
//
//    public class PendingHashTimeout extends Timeout {
//
//        public final Identifier eventId;
//
//        public PendingHashTimeout(ScheduleTimeout st, Identifier eventId) {
//            super(st);
//            this.eventId = eventId;
//        }
//    }
//
//    public class PendingPieceTimeout extends Timeout {
//
//        public final Identifier eventId;
//
//        public PendingPieceTimeout(ScheduleTimeout st, Identifier eventId) {
//            super(st);
//            this.eventId = eventId;
//        }
//    }
//}
