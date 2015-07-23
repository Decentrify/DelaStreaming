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
package se.sics.gvod.core.connMngr;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.javatuples.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.gvod.common.msg.ReqStatus;
import se.sics.gvod.croupierfake.CroupierPort;
import se.sics.gvod.croupierfake.CroupierSample;
import se.sics.gvod.common.util.VodDescriptor;
import se.sics.gvod.common.msg.vod.Connection;
import se.sics.gvod.common.msg.vod.Download;
import se.sics.gvod.common.utility.UtilityUpdate;
import se.sics.gvod.common.utility.UtilityUpdatePort;
import se.sics.gvod.core.connMngr.msg.DownloadDataTimeout;
import se.sics.gvod.core.connMngr.msg.DownloadHashTimeout;
import se.sics.gvod.core.connMngr.msg.Ready;
import se.sics.gvod.core.connMngr.msg.ScheduleConnUpdate;
import se.sics.kompics.ClassMatchedHandler;
import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.Handler;
import se.sics.kompics.Init;
import se.sics.kompics.Negative;
import se.sics.kompics.Positive;
import se.sics.kompics.network.Network;
import se.sics.kompics.network.Transport;
import se.sics.kompics.timer.CancelTimeout;
import se.sics.kompics.timer.SchedulePeriodicTimeout;
import se.sics.kompics.timer.ScheduleTimeout;
import se.sics.kompics.timer.Timeout;
import se.sics.kompics.timer.Timer;
import se.sics.p2ptoolbox.util.network.ContentMsg;
import se.sics.p2ptoolbox.util.network.impl.BasicContentMsg;
import se.sics.p2ptoolbox.util.network.impl.BasicHeader;
import se.sics.p2ptoolbox.util.network.impl.DecoratedAddress;
import se.sics.p2ptoolbox.util.network.impl.DecoratedHeader;

/**
 * @author Alex Ormenisan <aaor@sics.se>
 */
public class ConnMngrComp extends ComponentDefinition {

    private static final Logger log = LoggerFactory.getLogger(ConnMngrComp.class);

    private Negative<ConnMngrPort> myPort = provides(ConnMngrPort.class);
    private Positive<Network> network = requires(Network.class);
    private Positive<Timer> timer = requires(Timer.class);
    private Positive<CroupierPort> croupier = requires(CroupierPort.class);
    private Positive<UtilityUpdatePort> utilityUpdate = requires(UtilityUpdatePort.class);

    private final ConnMngrConfig config;

    private LocalVodDescriptor selfDesc;

    private Map<DecoratedAddress, DownloaderVodDescriptor> downloadersConn;
    private Map<DecoratedAddress, VodDescriptor> pendingUploadersConn;
    private Map<DecoratedAddress, UploaderVodDescriptor> uploadersConn;

    private Map<DecoratedAddress, Map<UUID, Pair<Download.DataRequest, UUID>>> pendingDownloadingData; //<source, <reqId, <req, timeoutId>>>
    private Map<DecoratedAddress, Map<UUID, Pair<Download.HashRequest, UUID>>> pendingDownloadingHash;
    private Map<Integer, Set<DecoratedAddress>> pendingUploadingData;
    private Map<Integer, Set<DecoratedAddress>> pendingUploadingHash;

    private UUID connUpdateTId;

    private boolean ready;

    public ConnMngrComp(ConnMngrInit init) {
        this.config = init.config;
        log.info("{} initiating ...", config.getSelf());

        this.selfDesc = null;
        this.downloadersConn = new HashMap<DecoratedAddress, DownloaderVodDescriptor>();
        this.pendingUploadersConn = new HashMap<DecoratedAddress, VodDescriptor>();
        this.uploadersConn = new HashMap<DecoratedAddress, UploaderVodDescriptor>();
        this.pendingDownloadingData = new HashMap<DecoratedAddress, Map<UUID, Pair<Download.DataRequest, UUID>>>();
        this.pendingDownloadingHash = new HashMap<DecoratedAddress, Map<UUID, Pair<Download.HashRequest, UUID>>>();
        this.pendingUploadingData = new HashMap<Integer, Set<DecoratedAddress>>();
        this.pendingUploadingHash = new HashMap<Integer, Set<DecoratedAddress>>();
        this.ready = false;

        subscribe(handleUpdateUtility, utilityUpdate);
    }

    private Handler<UtilityUpdate> handleUpdateUtility = new Handler<UtilityUpdate>() {

        @Override
        public void handle(UtilityUpdate event) {
            log.info("{} updating self descriptor", config.getSelf(), selfDesc);
            printComponentStatus();

            if (selfDesc == null) {
                selfDesc = new LocalVodDescriptor(new VodDescriptor(event.downloadPos), event.downloading);
                start();
            } else {

                if (selfDesc.downloading && !event.downloading) {
                    log.debug("{} completed - closing download connections", config.getSelf());
                    for (DecoratedAddress partner : uploadersConn.keySet()) {
                        Connection.Close msgContent = new Connection.Close(UUID.randomUUID());
                        DecoratedHeader<DecoratedAddress> msgHeader = new DecoratedHeader(new BasicHeader(config.getSelf(), partner, Transport.UDP), null, config.overlayId);
                        ContentMsg msg = new BasicContentMsg(msgHeader, msgContent);
                        trigger(msg, network);
                    }
                    for (DecoratedAddress partner : pendingUploadersConn.keySet()) {
                        Connection.Close msgContent = new Connection.Close(UUID.randomUUID());
                        DecoratedHeader<DecoratedAddress> msgHeader = new DecoratedHeader(new BasicHeader(config.getSelf(), partner, Transport.UDP), null, config.overlayId);
                        ContentMsg msg = new BasicContentMsg(msgHeader, msgContent);
                        trigger(msg, network);
                    }
                    uploadersConn = new HashMap<DecoratedAddress, UploaderVodDescriptor>();
                    pendingUploadersConn = new HashMap<DecoratedAddress, VodDescriptor>();

                    //TODO Alex do proper cleanups
//                    log.debug("{} cleaning timeouts", config.getSelf());
//                    for (Map<UUID, Pair<Download.DataRequest, TimeoutId>> partnerTIds : pendingDownloadingData.values()) {
//                        for (Pair<Download.DataRequest, TimeoutId> downReq : partnerTIds.values()) {
//                            CancelTimeout ct = new CancelTimeout(downReq.getValue1());
//                            trigger(ct, timer);
//                        }
//                    }
//
//                    CancelTimeout ct = new CancelTimeout(connUpdateTId);
//                    trigger(ct, timer);
//
//                    pendingDownloadingData = new HashMap<VodAddress, Map<UUID, Pair<Download.DataRequest, TimeoutId>>>();
                }
                selfDesc = new LocalVodDescriptor(new VodDescriptor(event.downloadPos), event.downloading);
            }
        }
    };

    private void start() {
        log.info("{} starting...", config.getSelf());
        if (selfDesc.downloading) {
            connUpdateTId = scheduleConnectionUpdate();
        } else {
            ready = true;
            trigger(new Ready(UUID.randomUUID()), myPort);
        }

        subscribe(handleScheduledConnectionUpdate, timer);
        subscribe(handleCroupierSample, croupier);

        subscribe(handleConnectionRequest, network);
        subscribe(handleConnectionResponse, network);
        subscribe(handleConnectionUpdate, network);
        subscribe(handleConnectionClose, network);

        subscribe(handleLocalHashRequest, myPort);
        subscribe(handleLocalHashResponse, myPort);
        subscribe(handleLocalDataRequest, myPort);
        subscribe(handleLocalDataResponse, myPort);
        subscribe(handleDownloadDataTimeout, timer);
        subscribe(handleDownloadHashTimeout, timer);

        subscribe(handleNetHashRequest, network);
        subscribe(handleNetHashResponse, network);
        subscribe(handleNetDataRequest, network);
        subscribe(handleNetDataResponse, network);
    }

    private void printComponentStatus() {
        log.info("connections - downloaders:{} uploaders:{}", downloadersConn.size(), uploadersConn.size());
        for (Map.Entry<DecoratedAddress, Map<UUID, Pair<Download.HashRequest, UUID>>> e : pendingDownloadingHash.entrySet()) {
            log.info("uploader:{} hash req:{}", e.getKey(), e.getValue().size());
        }
        for (Map.Entry<DecoratedAddress, Map<UUID, Pair<Download.DataRequest, UUID>>> e : pendingDownloadingData.entrySet()) {
            log.info("uploader:{} data req:{}", e.getKey(), e.getValue().size());
        }
        for (Map.Entry<DecoratedAddress, UploaderVodDescriptor> e : uploadersConn.entrySet()) {
            log.info("uploader:{} used slots:{}", e.getKey(), e.getValue().slots());
        }
        for (Map.Entry<DecoratedAddress, DownloaderVodDescriptor> e : downloadersConn.entrySet()) {
            log.info("downloader:{} used slots:{}", e.getKey(), e.getValue().slots());
        }
    }

    // CONNECTION MANAGEMENT
    Handler handleCroupierSample = new Handler<CroupierSample>() {

        @Override
        public void handle(CroupierSample event) {
            log.debug("{} handle new samples {}", config.getSelf(), event.sample);

            for (Map.Entry<DecoratedAddress, VodDescriptor> e : event.sample.entrySet()) {
                if (e.getValue().downloadPos < selfDesc.vodDesc.downloadPos) {
                    continue;
                }
                if (uploadersConn.containsKey(e.getKey()) || pendingUploadersConn.containsKey(e.getKey())) {
                    continue;
                }
                log.info("{} opening connection to {}", config.getSelf(), e.getKey());
                pendingUploadersConn.put(e.getKey(), e.getValue());
                Connection.Request requestContent = new Connection.Request(UUID.randomUUID(), selfDesc.vodDesc);
                DecoratedHeader<DecoratedAddress> requestHeader = new DecoratedHeader(new BasicHeader(config.getSelf(), e.getKey(), Transport.UDP), null, config.overlayId);
                ContentMsg request = new BasicContentMsg(requestHeader, requestContent);
                trigger(request, network);
            }
        }
    };

    Handler handleScheduledConnectionUpdate = new Handler<ScheduleConnUpdate>() {

        @Override
        public void handle(ScheduleConnUpdate event) {
            log.debug("{} handle {}", config.getSelf(), event);
            for (DecoratedAddress partner : downloadersConn.keySet()) {
                Connection.Update msgContent = new Connection.Update(UUID.randomUUID(), selfDesc.vodDesc);
                DecoratedHeader<DecoratedAddress> msgHeader = new DecoratedHeader(new BasicHeader(config.getSelf(), partner, Transport.UDP), null, config.overlayId);
                ContentMsg msg = new BasicContentMsg(msgHeader, msgContent);
                trigger(msg, network);
            }

            for (DecoratedAddress partner : uploadersConn.keySet()) {
                Connection.Update msgContent = new Connection.Update(UUID.randomUUID(), selfDesc.vodDesc);
                DecoratedHeader<DecoratedAddress> msgHeader = new DecoratedHeader(new BasicHeader(config.getSelf(), partner, Transport.UDP), null, config.overlayId);
                ContentMsg msg = new BasicContentMsg(msgHeader, msgContent);
                trigger(msg, network);
            }
        }
    };

    ClassMatchedHandler handleConnectionRequest
            = new ClassMatchedHandler<Connection.Request, BasicContentMsg<DecoratedAddress, DecoratedHeader<DecoratedAddress>, Connection.Request>>() {

                @Override
                public void handle(Connection.Request content, BasicContentMsg<DecoratedAddress, DecoratedHeader<DecoratedAddress>, Connection.Request> container) {
                    log.trace("{} net received:{} from:{}", new Object[]{config.getSelf(), content, container.getHeader().getSource()});

                    if (downloadersConn.containsKey(container.getHeader().getSource())) {
//                return;
                    }

                    log.debug("{} new connection to downloader {}", config.getSelf(), container.getHeader().getSource());
                    downloadersConn.put(container.getHeader().getSource(), new DownloaderVodDescriptor(content.desc, config.defaultMaxPipeline));
                    Connection.Response responseContent = content.accept();
                    DecoratedHeader<DecoratedAddress> responseHeader = new DecoratedHeader(new BasicHeader(config.getSelf(), container.getHeader().getSource(), Transport.UDP), null, config.overlayId);
                    ContentMsg response = new BasicContentMsg(responseHeader, responseContent);
                    trigger(response, network);
                }
            };

    ClassMatchedHandler handleConnectionResponse
            = new ClassMatchedHandler<Connection.Response, BasicContentMsg<DecoratedAddress, DecoratedHeader<DecoratedAddress>, Connection.Response>>() {

                @Override
                public void handle(Connection.Response content, BasicContentMsg<DecoratedAddress, DecoratedHeader<DecoratedAddress>, Connection.Response> container) {
                    log.trace("{} net received:{} from:{}", new Object[]{config.getSelf(), content, container.getHeader().getSource()});

                    if (!content.status.equals(ReqStatus.SUCCESS)) {
                        log.debug("{} connection req status {}", config.getSelf(), content.status);
                        pendingUploadersConn.remove(container.getHeader().getSource());
                        return;
                    }

                    if (!pendingUploadersConn.containsKey(container.getHeader().getSource())) {
                        log.info("{} closing connection to {}", config.getSelf(), container.getHeader().getSource());
                        Connection.Close msgContent = new Connection.Close(UUID.randomUUID());
                        DecoratedHeader<DecoratedAddress> msgHeader = new DecoratedHeader(new BasicHeader(config.getSelf(), container.getHeader().getSource(), Transport.UDP), null, config.overlayId);
                        ContentMsg msg = new BasicContentMsg(msgHeader, msgContent);
                        trigger(msg, network);
                        return;
                    }

                    log.debug("{} new connection to uploader {}", config.getSelf(), container.getHeader().getSource());
                    uploadersConn.put(container.getHeader().getSource(), new UploaderVodDescriptor(pendingUploadersConn.remove(container.getHeader().getSource()), config.defaultMaxPipeline));

                    if (!ready) {
                        log.debug("{} first uploader connection, can start serving", config.getSelf());
                        ready = true;
                        trigger(new Ready(UUID.randomUUID()), myPort);
                    }
                }
            };

    ClassMatchedHandler handleConnectionUpdate
            = new ClassMatchedHandler<Connection.Update, BasicContentMsg<DecoratedAddress, DecoratedHeader<DecoratedAddress>, Connection.Update>>() {

                @Override
                public void handle(Connection.Update content, BasicContentMsg<DecoratedAddress, DecoratedHeader<DecoratedAddress>, Connection.Update> container) {
                    log.trace("{} net received:{} from:{}", new Object[]{config.getSelf(), content, container.getHeader().getSource()});

                    if (downloadersConn.containsKey(container.getHeader().getSource())) {
                        downloadersConn.get(container.getHeader().getSource()).updateDesc(content.desc);
                    }

                    if (uploadersConn.containsKey(container.getHeader().getSource())) {
                        uploadersConn.get(container.getHeader().getSource()).updateDesc(content.desc);
                    }
                }
            };

    ClassMatchedHandler handleConnectionClose
            = new ClassMatchedHandler<Connection.Close, BasicContentMsg<DecoratedAddress, DecoratedHeader<DecoratedAddress>, Connection.Close>>() {

                @Override
                public void handle(Connection.Close content, BasicContentMsg<DecoratedAddress, DecoratedHeader<DecoratedAddress>, Connection.Close> container) {
                    log.debug("{} net received:{}, from:{}", new Object[]{config.getSelf(), content, container.getHeader().getSource()});
                    downloadersConn.remove(container.getHeader().getSource());
                    pendingUploadersConn.remove(container.getHeader().getSource());
                    uploadersConn.remove(container.getHeader().getSource());
                }
            };

    //DOWNLOAD MANAGEMENT - partener load management
    Handler handleLocalDataRequest = new Handler<Download.DataRequest>() {

        @Override
        public void handle(Download.DataRequest requestContent) {
            log.trace("{} handle local {}", config.getSelf(), requestContent);

            Map.Entry<DecoratedAddress, UploaderVodDescriptor> uploader = getUploader(requestContent.pieceId);
            if (uploader == null) {
                log.debug("{} no candidate for piece {}", new Object[]{config.getSelf(), requestContent.pieceId});
                trigger(requestContent.busy(), myPort);
                return;
            }
            uploader.getValue().useSlot();

            Map<UUID, Pair<Download.DataRequest, UUID>> partnerReq = pendingDownloadingData.get(uploader.getKey());
            if (partnerReq == null) {
                partnerReq = new HashMap<UUID, Pair<Download.DataRequest, UUID>>();
                pendingDownloadingData.put(uploader.getKey(), partnerReq);
            }
            partnerReq.put(requestContent.id, Pair.with(requestContent, scheduleDownloadDataTimeout(uploader.getKey(), requestContent.id)));
            DecoratedHeader<DecoratedAddress> requestHeader = new DecoratedHeader(new BasicHeader(config.getSelf(), uploader.getKey(), Transport.UDP), null, config.overlayId);
            ContentMsg request = new BasicContentMsg(requestHeader, requestContent);
            trigger(request, network);
        }

    };

    ClassMatchedHandler handleNetDataRequest
            = new ClassMatchedHandler<Download.DataRequest, BasicContentMsg<DecoratedAddress, DecoratedHeader<DecoratedAddress>, Download.DataRequest>>() {

                @Override
                public void handle(Download.DataRequest content, BasicContentMsg<DecoratedAddress, DecoratedHeader<DecoratedAddress>, Download.DataRequest> container) {
                    log.trace("{} net received:{} from:{}", new Object[]{config.getSelf(), content, container.getHeader().getSource()});
                    log.debug("{} requested unique pieces {}", pendingUploadingData.size());

                    DownloaderVodDescriptor downDesc = downloadersConn.get(container.getHeader().getSource());
                    if (downDesc == null) {
                        log.debug("{} no connection open, dropping req {} from {}", new Object[]{config.getSelf(), content, container.getHeader().getSource()});
                        return;
                    }
                    if (!downDesc.isViable()) {
                        //TODO should not happen yet, but will need to be treated later, when downloader has no more slots available
                        log.info("{} no more slots for peer:{}", config.getSelf(), container.getHeader().getSource());
                        return;
                    }
                    downDesc.useSlot();

                    Set<DecoratedAddress> requesters = pendingUploadingData.get(content.pieceId);
                    if (requesters == null) {
                        requesters = new HashSet<DecoratedAddress>();
                        pendingUploadingData.put(content.pieceId, requesters);
                        trigger(content, myPort);
                    }
                    requesters.add(container.getHeader().getSource());
                }
            };

    Handler handleLocalDataResponse = new Handler<Download.DataResponse>() {

        @Override
        public void handle(Download.DataResponse responseContent) {
            log.trace("{} handle local {}", config.getSelf(), responseContent);

            Set<DecoratedAddress> requesters = pendingUploadingData.get(responseContent.pieceId);
            if (requesters == null) {
                log.debug("{} no requesters for piece {}", config.getSelf(), responseContent.pieceId);
                return;
            }

            for (DecoratedAddress src : requesters) {
                DownloaderVodDescriptor down = downloadersConn.get(src);
                if (down != null) {
                    log.debug("{} sending piece {} to {}", new Object[]{config.getSelf(), responseContent.pieceId, src});
                    DecoratedHeader<DecoratedAddress> responseHeader = new DecoratedHeader(new BasicHeader(config.getSelf(), src, Transport.UDP), null, config.overlayId);
                    ContentMsg response = new BasicContentMsg(responseHeader, responseContent);
                    trigger(response, network);
                    down.freeSlot();
                }
            }
            pendingUploadingData.remove(responseContent.pieceId);
        }
    };

    ClassMatchedHandler handleNetDataResponse
            = new ClassMatchedHandler<Download.DataResponse, BasicContentMsg<DecoratedAddress, DecoratedHeader<DecoratedAddress>, Download.DataResponse>>() {

                @Override
                public void handle(Download.DataResponse content, BasicContentMsg<DecoratedAddress, DecoratedHeader<DecoratedAddress>, Download.DataResponse> container) {
                    log.trace("{} net received:{} from:{}", new Object[]{config.getSelf(), content, container.getHeader().getSource()});
                    UploaderVodDescriptor up = uploadersConn.get(container.getHeader().getSource());

                    Map<UUID, Pair<Download.DataRequest, UUID>> aux = pendingDownloadingData.get(container.getHeader().getSource());
                    if (aux == null) {
                        log.debug("{} data posibly late", config.getSelf());
                        //TODO Alex fix this;
                        return;
                    }
                    Pair<Download.DataRequest, UUID> req = aux.remove(content.id);
                    if (req == null) {
                        log.debug("{} data posibly late", config.getSelf());
                        //TODO Alex fix this;
                        return;
                    }
                    if(up == null) {
                        log.warn("received data from someone who i didn't connect to or clossed connection");
                        return;
                    }
                    up.freeSlot();
                    cancelDownloadDataTimeout(req.getValue0().id, req.getValue1());
                    trigger(content, myPort);
                }
            };

    Handler handleLocalHashRequest = new Handler<Download.HashRequest>() {

        @Override
        public void handle(Download.HashRequest requestContent) {
            log.debug("{} handle local {}", config.getSelf(), requestContent);
            Map.Entry<DecoratedAddress, UploaderVodDescriptor> uploader = getUploader(requestContent.targetPos);
            if (uploader == null) {
                log.info("{} no candidate for position {}", new Object[]{config.getSelf(), requestContent.targetPos});
                trigger(requestContent.busy(), myPort);
                return;
            }
            uploader.getValue().useSlot();

            Map<UUID, Pair<Download.HashRequest, UUID>> partnerReq = pendingDownloadingHash.get(uploader.getKey());
            if (partnerReq == null) {
                partnerReq = new HashMap<UUID, Pair<Download.HashRequest, UUID>>();
                pendingDownloadingHash.put(uploader.getKey(), partnerReq);
            }
            log.debug("{} sending hash req:{}", new Object[]{config.getSelf(), requestContent.hashes});
            partnerReq.put(requestContent.id, Pair.with(requestContent, scheduleDownloadHashTimeout(uploader.getKey(), requestContent.id)));
            DecoratedHeader<DecoratedAddress> requestHeader = new DecoratedHeader(new BasicHeader(config.getSelf(), uploader.getKey(), Transport.UDP), null, config.overlayId);
            ContentMsg request = new BasicContentMsg(requestHeader, requestContent);
            trigger(request, network);
        }

    };

    ClassMatchedHandler handleNetHashRequest
            = new ClassMatchedHandler<Download.HashRequest, BasicContentMsg<DecoratedAddress, DecoratedHeader<DecoratedAddress>, Download.HashRequest>>() {

                @Override
                public void handle(Download.HashRequest content, BasicContentMsg<DecoratedAddress, DecoratedHeader<DecoratedAddress>, Download.HashRequest> container) {
                    log.trace("{} net received:{} from:{}", new Object[]{config.getSelf(), content, container.getHeader().getSource()});

                    DownloaderVodDescriptor downDesc = downloadersConn.get(container.getHeader().getSource());
                    if (downDesc == null) {
                        log.debug("{} no connection open, dropping req {} from {}", new Object[]{config.getSelf(), content, container.getHeader().getSource()});
                        return;
                    }
                    if (!downDesc.isViable()) {
                        //TODO should not happen yet, but will need to be treated later, when downloader has no more slots available
                        log.info("{} no more slots for peer:{}", config.getSelf(), container.getHeader().getSource());
                        return;
                    }
                    downDesc.useSlot();

                    Set<DecoratedAddress> requesters = pendingUploadingHash.get(content.targetPos);
                    if (requesters == null) {
                        requesters = new HashSet<DecoratedAddress>();
                        pendingUploadingHash.put(content.targetPos, requesters);
                        trigger(content, myPort);
                    }
                    requesters.add(container.getHeader().getSource());
                }
            };

    private Handler<Download.HashResponse> handleLocalHashResponse = new Handler<Download.HashResponse>() {

        @Override
        public void handle(Download.HashResponse responseContent) {
            log.trace("{} handle local {}", config.getSelf(), responseContent);

            Set<DecoratedAddress> requesters = pendingUploadingHash.get(responseContent.targetPos);
            if (requesters == null) {
                log.debug("{} no requesters for piece {}", config.getSelf(), responseContent.targetPos);
                return;
            }

            for (DecoratedAddress src : requesters) {
                DownloaderVodDescriptor down = downloadersConn.get(src);
                if (down != null) {
                    log.debug("{} sending hash set {} to {}", new Object[]{config.getSelf(), responseContent.targetPos, src});
                    DecoratedHeader<DecoratedAddress> responseHeader = new DecoratedHeader(new BasicHeader(config.getSelf(), src, Transport.UDP), null, config.overlayId);
                    ContentMsg response = new BasicContentMsg(responseHeader, responseContent);
                    trigger(response, network);
                    down.freeSlot();
                }
            }
            pendingUploadingHash.remove(responseContent.targetPos);
        }
    };

    ClassMatchedHandler handleNetHashResponse = 
            new ClassMatchedHandler<Download.HashResponse, BasicContentMsg<DecoratedAddress, DecoratedHeader<DecoratedAddress>, Download.HashResponse>>() {

        @Override
        public void handle(Download.HashResponse content, BasicContentMsg<DecoratedAddress, DecoratedHeader<DecoratedAddress>, Download.HashResponse> container) {
            log.debug("{} net received:{} with status:{} from:{}", new Object[]{config.getSelf(), content, content.status, container.getHeader().getSource()});
            Map<UUID, Pair<Download.HashRequest, UUID>> aux = pendingDownloadingHash.get(container.getHeader().getSource());
            if (aux == null) {
                log.debug("{} hash posibly late", config.getSelf());
                //TODO Alex fix this;
                return;
            }
            Pair<Download.HashRequest, UUID> req = aux.remove(content.id);
            if (req == null) {
                log.debug("{} data posibly late", config.getSelf());
                //TODO Alex fix this;
                return;
            }
            UploaderVodDescriptor up = uploadersConn.get(container.getHeader().getSource());
            if (up != null) {
                up.freeSlot();
            }

            log.debug("{} received hashes:{} missing:{}", new Object[]{config.getSelf(), content.hashes.keySet(), content.missingHashes});
            aux.remove(content.id);
            cancelDownloadHashTimeout(req.getValue0().id, req.getValue1());
            trigger(content, myPort);
        }
    };

    Handler handleDownloadDataTimeout = new Handler<DownloadDataTimeout>() {

        @Override
        public void handle(DownloadDataTimeout timeout) {
            log.trace("{} handle {}", config.getSelf(), timeout);

            Map<UUID, Pair<Download.DataRequest, UUID>> targetDataTraffic = pendingDownloadingData.get(timeout.target);
            if (targetDataTraffic == null) {
                log.debug("{} timeout:{} for req:{} from:{} - possibly late", new Object[]{config.getSelf(), timeout.getTimeoutId(), timeout.reqId, timeout.target});
                return;
            }
            Pair<Download.DataRequest, UUID> req = targetDataTraffic.get(timeout.reqId);
            if (req == null) {
                log.debug("{} timeout:{} for req:{} from:{} - possibly late", new Object[]{config.getSelf(), timeout.getTimeoutId(), timeout.reqId, timeout.target});
                return;
            }
            uploadersConn.get(timeout.target).freeSlot();
            trigger(req.getValue0().timeout(), myPort);

            //cleaning
            targetDataTraffic.remove(timeout.reqId);
            if (targetDataTraffic.isEmpty()) {
                pendingDownloadingData.remove(timeout.target);
            }
        }
    };

    Handler handleDownloadHashTimeout = new Handler<DownloadHashTimeout>() {

        @Override
        public void handle(DownloadHashTimeout timeout) {
            log.info("{} handle {}", config.getSelf(), timeout);

            Map<UUID, Pair<Download.HashRequest, UUID>> targetHashTraffic = pendingDownloadingHash.get(timeout.target);
            if (targetHashTraffic == null) {
                log.info("{} Hash timeout:{} for req:{} from:{} - possibly late", new Object[]{config.getSelf(), timeout.getTimeoutId(), timeout.reqId, timeout.target});
                return;
            }
            Pair<Download.HashRequest, UUID> req = targetHashTraffic.get(timeout.reqId);
            if (req == null) {
                log.info("{} Hash timeout:{} for req:{} from:{} - possibly late", new Object[]{config.getSelf(), timeout.getTimeoutId(), timeout.reqId, timeout.target});
                return;
            }
            log.info("{} timeout hashes:{}", new Object[]{config.getSelf(), req.getValue0().hashes});
            uploadersConn.get(timeout.target).freeSlot();
            trigger(req.getValue0().timeout(), myPort);

            //cleaning
            targetHashTraffic.remove(timeout.reqId);
            if (targetHashTraffic.isEmpty()) {
                pendingDownloadingHash.remove(timeout.target);
            }
        }
    };

    private Map.Entry<DecoratedAddress, UploaderVodDescriptor> getUploader(int pieceId) {
        Iterator<Map.Entry<DecoratedAddress, UploaderVodDescriptor>> it = uploadersConn.entrySet().iterator();
        Map.Entry<DecoratedAddress, UploaderVodDescriptor> candidate = null;
        //get first viable candidate
        while (it.hasNext()) {
            candidate = it.next();
            int blockPos = pieceId / config.piecesPerBlock;
            if (candidate.getValue().isViable(blockPos)) {
                break;
            }
            candidate = null;
        }
        //get best candidate
        while (it.hasNext()) {
            Map.Entry<DecoratedAddress, UploaderVodDescriptor> nextC = it.next();
            int blockPos = pieceId / config.piecesPerBlock;
            if (nextC.getValue().betterCandidate(candidate.getValue(), blockPos)) {
                candidate = nextC;
            }
        }

        return candidate;
    }

    private UUID scheduleConnectionUpdate() {
        SchedulePeriodicTimeout spt = new SchedulePeriodicTimeout(config.updatePeriod, config.updatePeriod);
        Timeout t = new ScheduleConnUpdate(spt);
        spt.setTimeoutEvent(t);
        trigger(spt, timer);
        return t.getTimeoutId();
    }

    private UUID scheduleDownloadHashTimeout(DecoratedAddress target, UUID reqId) {
        ScheduleTimeout st = new ScheduleTimeout(config.reqTimeoutPeriod);
        Timeout t = new DownloadHashTimeout(st, target, reqId);
        st.setTimeoutEvent(t);
        trigger(st, timer);
        log.trace("{} schedule req:{} timeout:{}", new Object[]{config.getSelf(), reqId, t.getTimeoutId()});
        return t.getTimeoutId();
    }

    private UUID scheduleDownloadDataTimeout(DecoratedAddress target, UUID reqId) {
        ScheduleTimeout st = new ScheduleTimeout(config.reqTimeoutPeriod);
        Timeout t = new DownloadDataTimeout(st, target, reqId);
        st.setTimeoutEvent(t);
        trigger(st, timer);
        log.trace("{} schedule req:{} timeout:{}", new Object[]{config.getSelf(), reqId, t.getTimeoutId()});
        return t.getTimeoutId();
    }

    private void cancelDownloadDataTimeout(UUID reqId, UUID tid) {
        log.trace("{} canceling timeout:{} for req:{}", new Object[]{config.getSelf(), tid, reqId});
        CancelTimeout ct = new CancelTimeout(tid);
        trigger(ct, timer);

    }

    private void cancelDownloadHashTimeout(UUID reqId, UUID tid) {
        log.trace("{} canceling timeout:{} for req:{}", new Object[]{config.getSelf(), tid, reqId});
        CancelTimeout ct = new CancelTimeout(tid);
        trigger(ct, timer);

    }

    public static class ConnMngrInit extends Init<ConnMngrComp> {

        public final ConnMngrConfig config;

        public ConnMngrInit(ConnMngrConfig config) {
            this.config = config;
        }
    }
}
