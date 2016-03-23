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
import org.javatuples.Triplet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.gvod.common.event.vod.Connection;
import se.sics.gvod.common.event.vod.Download;
import se.sics.gvod.common.util.VodDescriptor;
import se.sics.gvod.common.utility.UtilityUpdate;
import se.sics.gvod.common.utility.UtilityUpdatePort;
import se.sics.gvod.core.aggregation.ConnMngrStatePacket;
import se.sics.gvod.core.aggregation.ConnMngrStateReducer;
import se.sics.gvod.core.connMngr.event.Ready;
import se.sics.kompics.ClassMatchedHandler;
import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.Handler;
import se.sics.kompics.Negative;
import se.sics.kompics.Positive;
import se.sics.kompics.Start;
import se.sics.kompics.network.Network;
import se.sics.kompics.network.Transport;
import se.sics.kompics.timer.CancelTimeout;
import se.sics.kompics.timer.SchedulePeriodicTimeout;
import se.sics.kompics.timer.ScheduleTimeout;
import se.sics.kompics.timer.Timeout;
import se.sics.kompics.timer.Timer;
import se.sics.ktoolbox.croupier.CroupierPort;
import se.sics.ktoolbox.croupier.event.CroupierSample;
import se.sics.ktoolbox.util.aggregation.CompTracker;
import se.sics.ktoolbox.util.aggregation.CompTrackerImpl;
import se.sics.ktoolbox.util.config.impl.SystemKCWrapper;
import se.sics.ktoolbox.util.identifiable.Identifier;
import se.sics.ktoolbox.util.network.KAddress;
import se.sics.ktoolbox.util.network.KContentMsg;
import se.sics.ktoolbox.util.network.KHeader;
import se.sics.ktoolbox.util.network.basic.BasicContentMsg;
import se.sics.ktoolbox.util.network.basic.BasicHeader;
import se.sics.ktoolbox.util.network.basic.DecoratedHeader;
import se.sics.ktoolbox.util.other.AgingAdrContainer;
import se.sics.ktoolbox.util.overlays.view.OverlayViewUpdate;
import se.sics.ktoolbox.util.overlays.view.OverlayViewUpdatePort;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class ConnMngrComp extends ComponentDefinition {

    private static final Logger LOG = LoggerFactory.getLogger(ConnMngrComp.class);
    private String logPrefix;

    //***************************CONNECTIONS************************************
    final Negative<ConnMngrPort> myPort = provides(ConnMngrPort.class);
    final Positive<Network> network = requires(Network.class);
    final Positive<Timer> timer = requires(Timer.class);
    final Negative<OverlayViewUpdatePort> croupierViewUpdatePort = provides(OverlayViewUpdatePort.class);
    final Positive<CroupierPort> croupierPort = requires(CroupierPort.class);
    final Positive<UtilityUpdatePort> utilityUpdate = requires(UtilityUpdatePort.class);
    //***************************CONFIGURATION**********************************
    private final ConnMngrKCWrapper connMngrConfig;
    private final Identifier overlayId;
    //***************************EXTERNAL STATE*********************************
    private KAddress selfAdr;
    //***************************INTERNAL STATE*********************************
    private ConnectionTracker connTracker;
    private UploadTracker uploadTracker;
    private DownloadTracker downloadTracker;
    private LocalVodDescriptor selfDesc;
    //******************************AUX STATE***********************************
    private UUID periodicISCheckTid = null;
    private UUID periodicConnUpdateTid = null;
    //******************************TRACKING************************************
    private CompTracker compTracker;

    public ConnMngrComp(Init init) {
        connMngrConfig = new ConnMngrKCWrapper(config());
        selfAdr = init.selfAdr;
        overlayId = init.overlayId;
        logPrefix = "<nid:" + selfAdr.getId() + ", oid:" + overlayId + "> ";
        LOG.info("{} initiating...", logPrefix);

        connTracker = new ConnectionTracker();
        uploadTracker = new UploadTracker();
        downloadTracker = new DownloadTracker();

        //control
        subscribe(handleStart, control);

        //external state update
        subscribe(handleUpdateUtility, utilityUpdate);

        //state tracking
        subscribe(handlePeriodicISCheck, timer);
        setCompTracker();
    }

    public boolean ready() {
        if(selfDesc == null) {
            LOG.warn("{}self desc is not defined", logPrefix);
            return false;
        }

        return true;
    }

    //*****************************CONTROL**************************************
    private Handler handleStart = new Handler<Start>() {
        @Override
        public void handle(Start e) {
            LOG.info("{} starting...", logPrefix);
            compTracker.start();
            schedulePeriodicISCheck();
        }
    };

    //***************************STATE TRACKING*********************************
    private void setCompTracker() {
        switch (connMngrConfig.connMngrAggLevel) {
            case NONE:
                compTracker = new CompTrackerImpl(proxy, Pair.with(LOG, logPrefix), connMngrConfig.connMngrAggPeriod);
                break;
            case BASIC:
                compTracker = new CompTrackerImpl(proxy, Pair.with(LOG, logPrefix), connMngrConfig.connMngrAggPeriod);
                setEventTracking();
                break;
            case FULL:
                compTracker = new CompTrackerImpl(proxy, Pair.with(LOG, logPrefix), connMngrConfig.connMngrAggPeriod);
                setEventTracking();
                setStateTracking();
                break;
            default:
                throw new RuntimeException("Undefined:" + connMngrConfig.connMngrAggLevel);
        }
    }

    private void setEventTracking() {
        compTracker.registerPositivePort(network);
        compTracker.registerPositivePort(timer);
        compTracker.registerNegativePort(myPort);
        compTracker.registerPositivePort(utilityUpdate);
        compTracker.registerPositivePort(croupierPort);
        compTracker.registerNegativePort(croupierViewUpdatePort);
    }

    private void setStateTracking() {
        compTracker.registerReducer(new ConnMngrStateReducer());
    }

    Handler handlePeriodicISCheck = new Handler<PeriodicInternalStateCheck>() {
        @Override
        public void handle(PeriodicInternalStateCheck e) {
            compTracker.updateState(new ConnMngrStatePacket(
                    connTracker.publishInternalState(),
                    uploadTracker.publishInternalState(),
                    downloadTracker.publishInternalState()));
            //todo self update
        }
    };

    //**************************************************************************
    private Handler<UtilityUpdate> handleUpdateUtility = new Handler<UtilityUpdate>() {

        @Override
        public void handle(UtilityUpdate update) {
            LocalVodDescriptor newSelfDesc = new LocalVodDescriptor(new VodDescriptor(update.downloadPos), update.downloading);

            LOG.info("{}update self descriptor from:{} to:{}",
                    new Object[]{logPrefix, selfDesc, newSelfDesc});

            if (!ready()) {
                selfDesc = newSelfDesc;
                if (ready()) {
                    start();
                }
            } else {
                selfDesc = newSelfDesc;
            }
            if (selfDesc.downloading && !update.downloading) {
                LOG.info("{}download complete - closing download connections", logPrefix);
                if (!downloadTracker.isEmpty()) {
                    LOG.warn("{}download tracker not empty at download complete", logPrefix);
                }
                downloadTracker.clear();
                connTracker.cleanCloseDownloadConnections();
            }
            trigger(new OverlayViewUpdate.Indication(overlayId, false, selfDesc.vodDesc.deepCopy()), croupierViewUpdatePort);
        }
    };

    private void start() {
        startConnectionTracker();
        startUploadTracker();
        if (selfDesc.downloading) {
            startDownloadTracker();
        }
    }

    private void startConnectionTracker() {
        subscribe(connTracker.handleConnectionRequest, network);
        subscribe(connTracker.handleConnectionResponse, network);
        subscribe(connTracker.handleConnectionUpdate, network);
        subscribe(connTracker.handleConnectionClose, network);
        subscribe(connTracker.handleScheduledConnectionUpdate, timer);
        scheduleConnUpdate();
    }

    private void startDownloadTracker() {
        subscribe(downloadTracker.handleCroupierSample, croupierPort);
        subscribe(downloadTracker.handleLocalHashRequest, myPort);
        subscribe(downloadTracker.handleLocalDataRequest, myPort);
        subscribe(downloadTracker.handleNetHashResponse, network);
        subscribe(downloadTracker.handleNetDataResponse, network);
        subscribe(downloadTracker.handleDownloadHashTimeout, timer);
        subscribe(downloadTracker.handleDownloadDataTimeout, timer);
    }

    private void startUploadTracker() {
        subscribe(uploadTracker.handleNetHashRequest, network);
        subscribe(uploadTracker.handleNetDataRequest, network);
        subscribe(uploadTracker.handleLocalHashResponse, myPort);
        subscribe(uploadTracker.handleLocalDataResponse, myPort);
    }

    //**********************Timeouts*******************************************
    private void schedulePeriodicISCheck() {
        if (periodicISCheckTid != null) {
            LOG.warn("{} double schedule of periodic ISCHeck timeout", logPrefix);
        } else {
            LOG.trace("{} scheduling periodic ISCheck timeout", logPrefix);
        }
        SchedulePeriodicTimeout spt = new SchedulePeriodicTimeout(connMngrConfig.periodicStateCheck, connMngrConfig.periodicStateCheck);
        Timeout t = new PeriodicInternalStateCheck(spt);
        spt.setTimeoutEvent(t);
        trigger(spt, timer);
        periodicISCheckTid = t.getTimeoutId();
    }

    private void cancelPeriodicISCheck() {
        if (periodicISCheckTid == null) {
            LOG.warn("{} double cancelation of periodic ISCHeck timeout", logPrefix);
        } else {
            LOG.trace("{} canceling periodic ISCheck timeout", logPrefix);
        }
        CancelTimeout ct = new CancelTimeout(periodicISCheckTid);
        trigger(ct, timer);
        periodicISCheckTid = null;

    }

    public static class PeriodicInternalStateCheck extends Timeout {

        public PeriodicInternalStateCheck(SchedulePeriodicTimeout schedule) {
            super(schedule);
        }
    }

    private void scheduleConnUpdate() {
        if (periodicConnUpdateTid != null) {
            LOG.warn("{} double schedule of periodic ConnUpdate timeout", logPrefix);
        } else {
            LOG.trace("{} scheduling periodic ConnUpdate timeout", logPrefix);
        }
        SchedulePeriodicTimeout spt = new SchedulePeriodicTimeout(connMngrConfig.periodicConnUpdate, connMngrConfig.periodicConnUpdate);
        Timeout t = new PeriodicConnUpdate(spt);
        spt.setTimeoutEvent(t);
        trigger(spt, timer);
        periodicConnUpdateTid = t.getTimeoutId();
    }

    private void cancelConnUpdate() {
        if (periodicConnUpdateTid == null) {
            LOG.warn("{} double cancelation of periodic ConnUpdate timeout", logPrefix);
        } else {
            LOG.trace("{} canceling periodic ConnUpdate timeout", logPrefix);
        }
        CancelTimeout ct = new CancelTimeout(periodicISCheckTid);
        trigger(ct, timer);
        periodicConnUpdateTid = null;

    }

    public static class PeriodicConnUpdate extends Timeout {

        public PeriodicConnUpdate(SchedulePeriodicTimeout schedule) {
            super(schedule);
        }
    }

    private UUID scheduleDownloadDataTimeout(Identifier reqId, Identifier target) {
        ScheduleTimeout st = new ScheduleTimeout(connMngrConfig.reqTimeoutPeriod);
        Timeout t = new DownloadDataTimeout(st, reqId, target);
        st.setTimeoutEvent(t);
        trigger(st, timer);
        return t.getTimeoutId();
    }

    private void cancelDownloadDataTimeout(UUID tid) {
        CancelTimeout ct = new CancelTimeout(tid);
        trigger(ct, timer);

    }

    public class DownloadDataTimeout extends Timeout {

        public final Identifier target;
        public final Identifier reqId;

        public DownloadDataTimeout(ScheduleTimeout schedule, Identifier reqId, Identifier target) {
            super(schedule);
            this.reqId = reqId;
            this.target = target;
        }
    }

    private UUID scheduleDownloadHashTimeout(Identifier reqId, Identifier target) {
        ScheduleTimeout st = new ScheduleTimeout(connMngrConfig.reqTimeoutPeriod);
        Timeout t = new DownloadHashTimeout(st, reqId, target);
        st.setTimeoutEvent(t);
        trigger(st, timer);
        return t.getTimeoutId();
    }

    private void cancelDownloadHashTimeout(UUID tid) {
        CancelTimeout ct = new CancelTimeout(tid);
        trigger(ct, timer);

    }

    public class DownloadHashTimeout extends Timeout {

        public final Identifier target;
        public final Identifier reqId;

        public DownloadHashTimeout(ScheduleTimeout schedule, Identifier reqId, Identifier target) {
            super(schedule);
            this.reqId = reqId;
            this.target = target;
        }
    }

//**************************************************************************
    public static class Init extends se.sics.kompics.Init<ConnMngrComp> {

        public final KAddress selfAdr;
        public final Identifier overlayId;

        public Init(KAddress selfAdr, Identifier overlayId) {
            this.selfAdr = selfAdr;
            this.overlayId = overlayId;
        }
    }

    public class DownloadTracker {

        //<address, <requestId, <request, timeoutId>>> 
        private Map<Identifier, Map<Identifier, Pair<Download.HashRequest, UUID>>> pendingDownloadingHash
                = new HashMap<>();
        //<address, <requestId, <request, timeoutId>>> 
        private Map<Identifier, Map<Identifier, Pair<Download.DataRequest, UUID>>> pendingDownloadingData
                = new HashMap<>();
        private Map<Identifier, VodDescriptor> uploaders
                = new HashMap<>();

        public DownloadTracker() {
        }

        /**
         * @return <uploaders, uploadingHashReq, uploadingDataReq>
         */
        public Triplet<Integer, Integer, Integer> publishInternalState() {
//            LOG.info("{} {} available uploaders", logPrefix, uploaders.size());
            int uppHash = 0, uppData = 0;
            for (Map.Entry<Identifier, Map<Identifier, Pair<Download.HashRequest, UUID>>> e : pendingDownloadingHash.entrySet()) {
//                LOG.info("{} pending download hash from:{} size:{}", new Object[]{logPrefix, e.getKey(), e.getValue().size()});
                uppHash += e.getValue().size();
            }
            for (Map.Entry<Identifier, Map<Identifier, Pair<Download.DataRequest, UUID>>> e : pendingDownloadingData.entrySet()) {
//                LOG.info("{} pending download data from:{} size:{}", new Object[]{logPrefix, e.getKey(), e.getValue().size()});
                uppData += e.getValue().size();
            }

            return Triplet.with(uploaders.size(), uppHash, uppData);
        }

        public boolean isEmpty() {
            return pendingDownloadingData.isEmpty() && pendingDownloadingHash.isEmpty();
        }

        public void clear() {
            uploaders.clear();
        }

        public boolean connect(KAddress target, VodDescriptor desc) {
            tryPublishReady();
            uploaders.put(target.getId(), desc);
            return true;
        }

        public boolean keepConnect(KAddress target, VodDescriptor desc) {
            if (uploaders.containsKey(target.getId())) {
                tryPublishReady();
                uploaders.put(target.getId(), desc);
                return true;
            }
            return false;
        }

        //TODO Alex - might want to keep local load in a variable...this might be expensive
        public void tryPublishReady() {
            int localLoad = 0;
            for (Map<Identifier, Pair<Download.HashRequest, UUID>> hashQueue : pendingDownloadingHash.values()) {
                localLoad += hashQueue.size();
            }
            for (Map<Identifier, Pair<Download.DataRequest, UUID>> dataQueue : pendingDownloadingData.values()) {
                localLoad += dataQueue.size();
            }
            int freeSlots = connMngrConfig.defaultMaxPipeline - localLoad;
            //TODO Alex hardcoded slots
            freeSlots = freeSlots > 15 ? 15 : freeSlots;
            if (freeSlots > 0) {
                LOG.info("{} ready slots:{}", logPrefix, freeSlots);
                trigger(new Ready(freeSlots), myPort);
            }
        }

        public void close(KAddress target) {
            Map<Identifier, Pair<Download.HashRequest, UUID>> hashQueue = pendingDownloadingHash.remove(target.getId());
            if (hashQueue != null) {
                for (Map.Entry<Identifier, Pair<Download.HashRequest, UUID>> e : hashQueue.entrySet()) {
                    Download.HashRequest hashReq = e.getValue().getValue0();
                    trigger(hashReq.timeout(), myPort);
                    cancelDownloadHashTimeout(e.getValue().getValue1());
                }
            }

            Map<Identifier, Pair<Download.DataRequest, UUID>> dataQueue = pendingDownloadingData.remove(target.getId());
            if (dataQueue != null) {
                for (Map.Entry<Identifier, Pair<Download.DataRequest, UUID>> e : dataQueue.entrySet()) {
                    Download.DataRequest dataReq = e.getValue().getValue0();
                    trigger(dataReq.timeout(), myPort);
                    cancelDownloadDataTimeout(e.getValue().getValue1());
                }
            }

            uploaders.remove(target.getId());
        }

        Handler handleCroupierSample = new Handler<CroupierSample<VodDescriptor>>() {

            @Override
            public void handle(CroupierSample<VodDescriptor> event) {
                if (selfDesc.downloading) {
                    LOG.info("{} received new croupier samples:{}", logPrefix, event.privateSample.size() + event.publicSample.size());
                    for (AgingAdrContainer<KAddress, VodDescriptor> container : event.publicSample.values()) {
                        LOG.info("{} {} vod descriptor:{}", new Object[]{logPrefix, container.getSource().getId(), container.getContent().downloadPos});
                        if (container.getContent().downloadPos < selfDesc.vodDesc.downloadPos) {
                            continue;
                        }
                        if (uploaders.containsKey(container.getSource().getId())) {
                            continue;
                        }
                        connTracker.openDownloadConnection(container.getSource());
                    }

                    for (AgingAdrContainer<KAddress, VodDescriptor> container : event.privateSample.values()) {
                        if (container.getContent().downloadPos < selfDesc.vodDesc.downloadPos) {
                            continue;
                        }
                        if (uploaders.containsKey(container.getSource().getId())) {
                            continue;
                        }
                        connTracker.openDownloadConnection(container.getSource());
                    }
                }
            }
        };

        Handler handleLocalHashRequest = new Handler<Download.HashRequest>() {

            @Override
            public void handle(Download.HashRequest requestContent) {
                LOG.debug("{} handle local hash request:{}", logPrefix, requestContent.targetPos);
                KAddress uploader = getUploader(requestContent.targetPos);
                if (uploader == null) {
                    LOG.info("{} no candidate for position:{}", new Object[]{logPrefix, requestContent.targetPos});
                    trigger(requestContent.busy(), myPort);
                    return;
                }
                Map<Identifier, Pair<Download.HashRequest, UUID>> uploaderQueue = pendingDownloadingHash.get(uploader.getId());
                if (uploaderQueue == null) {
                    uploaderQueue = new HashMap<>();
                    pendingDownloadingHash.put(uploader.getId(), uploaderQueue);
                }
                LOG.debug("{} sending hash request:{} to:{}",
                        new Object[]{logPrefix, requestContent.hashes, uploader.getId()});
                UUID tId = scheduleDownloadHashTimeout(requestContent.eventId, uploader.getId());
                uploaderQueue.put(requestContent.eventId, Pair.with(requestContent, tId));
                KHeader<KAddress> requestHeader
                        = new DecoratedHeader(new BasicHeader(selfAdr, uploader, Transport.UDP), overlayId);
                KContentMsg request = new BasicContentMsg(requestHeader, requestContent);
                trigger(request, network);
            }
        };

        Handler handleLocalDataRequest = new Handler<Download.DataRequest>() {
            @Override
            public void handle(Download.DataRequest requestContent) {
                LOG.debug("{} handle local data request:{}", logPrefix, requestContent.pieceId);

                KAddress uploader = getUploader(requestContent.pieceId / connMngrConfig.piecesPerBlock);
                if (uploader == null) {
                    LOG.debug("{} no candidate for piece:{}", new Object[]{logPrefix, requestContent.pieceId});
                    trigger(requestContent.busy(), myPort);
                    return;
                }
                Map<Identifier, Pair<Download.DataRequest, UUID>> uploaderQueue = pendingDownloadingData.get(uploader.getId());
                if (uploaderQueue == null) {
                    uploaderQueue = new HashMap<>();
                    pendingDownloadingData.put(uploader.getId(), uploaderQueue);
                }
                LOG.debug("{} sending data request:{} to:{}",
                        new Object[]{logPrefix, requestContent.pieceId, uploader.getId()});
                UUID tId = scheduleDownloadDataTimeout(requestContent.eventId, uploader.getId());
                uploaderQueue.put(requestContent.eventId, Pair.with(requestContent, tId));
                KHeader<KAddress> requestHeader
                        = new DecoratedHeader(new BasicHeader(selfAdr, uploader, Transport.UDP), overlayId);
                KContentMsg request = new BasicContentMsg(requestHeader, requestContent);
                trigger(request, network);
            }
        };

        private KAddress getUploader(int blockPos) {
            Iterator<Map.Entry<Identifier, VodDescriptor>> it = uploaders.entrySet().iterator();
            Map.Entry<Identifier, VodDescriptor> candidate = null;

            //get first viable candidate
            while (it.hasNext() && candidate == null) {
                candidate = it.next();
                if (candidate.getValue().downloadPos < blockPos) {
                    candidate = null;
                }
            }
            if (candidate == null) {
                return null;
            }
            int candidateLoad = 0;
            candidateLoad += pendingDownloadingData.containsKey(candidate.getKey()) ? pendingDownloadingData.get(candidate.getKey()).size() : 0;
            candidateLoad += pendingDownloadingHash.containsKey(candidate.getKey()) ? pendingDownloadingHash.get(candidate.getKey()).size() : 0;
            //get best candidate
            while (it.hasNext()) {
                Map.Entry<Identifier, VodDescriptor> nextCandidate = it.next();
                if (nextCandidate.getValue().downloadPos < blockPos) {
                    continue;
                }
                int nextCandidateLoad = 0;
                nextCandidateLoad += pendingDownloadingData.containsKey(nextCandidate.getKey()) ? pendingDownloadingData.get(nextCandidate.getKey()).size() : 0;
                nextCandidateLoad += pendingDownloadingHash.containsKey(nextCandidate.getKey()) ? pendingDownloadingHash.get(nextCandidate.getKey()).size() : 0;
                if (nextCandidateLoad < candidateLoad) {
                    candidate = nextCandidate;
                    candidateLoad = nextCandidateLoad;
                }
            }
            return connTracker.getDownloadConn(candidate.getKey());
        }

        Handler handleDownloadDataTimeout = new Handler<DownloadDataTimeout>() {

            @Override
            public void handle(DownloadDataTimeout timeout) {
                LOG.debug("{} timeout for data from:{}", new Object[]{logPrefix, timeout.target});

                Map<Identifier, Pair<Download.DataRequest, UUID>> uploaderQueue = pendingDownloadingData.get(timeout.target);
                if (uploaderQueue == null) {
                    LOG.info("{} timeout for data from:{} - possibly late", new Object[]{logPrefix, timeout.target});
                    return;
                }
                Pair<Download.DataRequest, UUID> req = uploaderQueue.remove(timeout.reqId);
                if (req == null) {
                    LOG.info("{} timeout for data from:{} - possibly late", new Object[]{logPrefix, timeout.target});
                    return;
                }
                trigger(req.getValue0().timeout(), myPort);

                if (uploaderQueue.isEmpty()) {
                    pendingDownloadingData.remove(timeout.target);
                }
            }
        };

        Handler handleDownloadHashTimeout = new Handler<DownloadHashTimeout>() {

            @Override
            public void handle(DownloadHashTimeout timeout) {
                LOG.debug("{} timeout for hash from:{}", logPrefix, timeout.target);

                Map<Identifier, Pair<Download.HashRequest, UUID>> uploaderQueue = pendingDownloadingHash.get(timeout.target);
                if (uploaderQueue == null) {
                    LOG.info("{} timeout for hash from:{} - possibly late", new Object[]{logPrefix, timeout.target});
                    return;
                }
                Pair<Download.HashRequest, UUID> req = uploaderQueue.remove(timeout.reqId);
                if (req == null) {
                    LOG.info("{} timeout for hash from:{} - possibly late", new Object[]{logPrefix, timeout.target});
                    return;
                }
                trigger(req.getValue0().timeout(), myPort);

                if (uploaderQueue.isEmpty()) {
                    pendingDownloadingHash.remove(timeout.target);
                }
            }
        };

        ClassMatchedHandler handleNetHashResponse
                = new ClassMatchedHandler<Download.HashResponse, BasicContentMsg<KAddress, DecoratedHeader<KAddress>, Download.HashResponse>>() {

                    @Override
                    public void handle(Download.HashResponse content, BasicContentMsg<KAddress, DecoratedHeader<KAddress>, Download.HashResponse> container) {
                        KAddress target = container.getHeader().getSource();
                        connTracker.updateAddress(target);

                        LOG.debug("{} received net hash response from:{}", new Object[]{logPrefix, target.getId()});
                        Map<Identifier, Pair<Download.HashRequest, UUID>> uploaderQueue = pendingDownloadingHash.get(target.getId());
                        if (uploaderQueue == null) {
                            LOG.info("{} hash from:{} - posibly late", logPrefix, target.getId());
                            return;
                        }
                        Pair<Download.HashRequest, UUID> req = uploaderQueue.remove(content.eventId);
                        if (req == null) {
                            LOG.debug("{} hash from:{} - posibly late", logPrefix, target.getId());
                            return;
                        }

                        cancelDownloadHashTimeout(req.getValue1());
                        trigger(content, myPort);

                        if (uploaderQueue.isEmpty()) {
                            pendingDownloadingHash.remove(target.getId());
                        }
                    }
                };

        ClassMatchedHandler handleNetDataResponse
                = new ClassMatchedHandler<Download.DataResponse, BasicContentMsg<KAddress, DecoratedHeader<KAddress>, Download.DataResponse>>() {

                    @Override
                    public void handle(Download.DataResponse content, BasicContentMsg<KAddress, DecoratedHeader<KAddress>, Download.DataResponse> container) {
                        KAddress target = container.getHeader().getSource();
                        connTracker.updateAddress(target);

                        LOG.trace("{} receiver net data response from:{}", new Object[]{logPrefix, target.getId()});
                        Map<Identifier, Pair<Download.DataRequest, UUID>> uploaderQueue = pendingDownloadingData.get(target.getId());
                        if (uploaderQueue == null) {
                            LOG.info("{} data from:{} - posibly late", logPrefix, target.getId());
                            return;
                        }
                        Pair<Download.DataRequest, UUID> req = uploaderQueue.remove(content.eventId);
                        if (req == null) {
                            LOG.info("{} data from:{} posibly late", logPrefix, target.getId());
                            return;
                        }

                        cancelDownloadDataTimeout(req.getValue1());
                        trigger(content, myPort);

                        if (uploaderQueue.isEmpty()) {
                            pendingDownloadingData.remove(target.getId());
                        }
                    }
                };
    }

    public class UploadTracker {

        //TODO Alex - might want to optimise and to catch requests for same package and ask only once from the store
        public Map<Identifier, VodDescriptor> downloaders = new HashMap<>();
        //<requestId, target>
        private Map<Identifier, Identifier> pendingUploadingHash = new HashMap<>();
        //<requestId, target>
        private Map<Identifier, Identifier> pendingUploadingData = new HashMap<>();

        public UploadTracker() {
        }

        /**
         * @return <downloaders, downloadingHashReq, downloadingDataReq>
         */
        public Triplet<Integer, Integer, Integer> publishInternalState() {
            return Triplet.with(downloaders.size(), pendingUploadingHash.size(), pendingUploadingData.size());
        }

        public boolean connect(KAddress target, VodDescriptor desc) {
            downloaders.put(target.getId(), desc);
            return true;
        }

        public boolean keepConnect(KAddress target, VodDescriptor desc) {
            if (downloaders.containsKey(target.getId())) {
                downloaders.put(target.getId(), desc);
                return true;
            }
            return false;
        }

        public void close(KAddress target) {
            downloaders.remove(target.getId());

            Iterator<Map.Entry<Identifier, Identifier>> it;
            int removed;

            it = pendingUploadingHash.entrySet().iterator();
            removed = 0;
            while (it.hasNext()) {
                Map.Entry<Identifier, Identifier> e = it.next();
                if (target.getId().equals(e.getValue())) {
                    removed++;
                    it.remove();
                }
            }
            if (removed > 0) {
                LOG.info("{} closing connection to:{}, with {} pending hash requests", logPrefix, removed);
            }

            it = pendingUploadingData.entrySet().iterator();
            removed = 0;
            while (it.hasNext()) {
                Map.Entry<Identifier, Identifier> e = it.next();
                if (target.getId().equals(e.getValue())) {
                    removed++;
                    it.remove();
                }
            }
            if (removed > 0) {
                LOG.info("{} closing connection to:{}, with {} pending data requests", logPrefix, removed);
            }
        }

        ClassMatchedHandler handleNetHashRequest
                = new ClassMatchedHandler<Download.HashRequest, BasicContentMsg<KAddress, DecoratedHeader<KAddress>, Download.HashRequest>>() {

                    @Override
                    public void handle(Download.HashRequest content, BasicContentMsg<KAddress, DecoratedHeader<KAddress>, Download.HashRequest> container) {
                        KAddress target = container.getHeader().getSource();
                        connTracker.updateAddress(target);

                        LOG.debug("{} received net hash request from:{}, for:{}", new Object[]{logPrefix, target.getId(), content.targetPos});
                        if (!downloaders.containsKey(target.getId())) {
                            LOG.warn("{} no connection open to:{}, dropping hash request:{}", new Object[]{logPrefix, target.getId(), content.hashes});
                            connTracker.closeGhostUploadConnection(target);
                            return;
                        }

                        if (!pendingUploadingHash.containsKey(content.eventId)) {
                            pendingUploadingHash.put(content.eventId, target.getId());
                            trigger(content, myPort);
                        } else {
                            LOG.warn("{} request already registered", logPrefix);
                        }
                    }
                };

        ClassMatchedHandler handleNetDataRequest
                = new ClassMatchedHandler<Download.DataRequest, BasicContentMsg<KAddress, DecoratedHeader<KAddress>, Download.DataRequest>>() {

                    @Override
                    public void handle(Download.DataRequest content, BasicContentMsg<KAddress, DecoratedHeader<KAddress>, Download.DataRequest> container) {
                        KAddress target = container.getHeader().getSource();
                        connTracker.updateAddress(target);

                        LOG.debug("{} received net data request from:{}, for:{}", new Object[]{logPrefix, target.getId(), content.pieceId});
                        if (!downloaders.containsKey(target.getId())) {
                            LOG.warn("{} no connection open to:{}, dropping data request:{}", new Object[]{logPrefix, target.getId(), content.pieceId});
                            connTracker.closeGhostUploadConnection(target);
                            return;
                        }

                        if (!pendingUploadingData.containsKey(content.eventId)) {
                            pendingUploadingData.put(content.eventId, target.getId());
                            trigger(content, myPort);
                        } else {
                            LOG.warn("{} request already registered", logPrefix);
                        }
                    }
                };

        private Handler handleLocalHashResponse = new Handler<Download.HashResponse>() {

            @Override
            public void handle(Download.HashResponse responseContent) {
                LOG.debug("{} received local hash response:{}", logPrefix, responseContent.targetPos);

                if (!pendingUploadingHash.containsKey(responseContent.eventId)) {
                    LOG.warn("{} late local hash response, inconsistency");
                    return;
                }
                KAddress target = connTracker.getUploadConn(pendingUploadingHash.remove(responseContent.eventId));
                if (target == null) {
                    LOG.warn("{} late local hash response, connection inconsistency");
                    return;
                }
                LOG.debug("{} sending hash:{} to:{}", new Object[]{logPrefix, responseContent.targetPos, target.getId()});
                KHeader<KAddress> responseHeader
                        = new DecoratedHeader(new BasicHeader(selfAdr, target, Transport.UDP), overlayId);
                KContentMsg response = new BasicContentMsg(responseHeader, responseContent);
                trigger(response, network);
            }
        };

        Handler handleLocalDataResponse = new Handler<Download.DataResponse>() {

            @Override
            public void handle(Download.DataResponse responseContent) {
                LOG.debug("{} received local data response:{}", logPrefix, responseContent.pieceId);

                if (!pendingUploadingData.containsKey(responseContent.eventId)) {
                    LOG.warn("{} late local data response, inconsistency", logPrefix);
                    return;
                }

                KAddress target = connTracker.getUploadConn(pendingUploadingData.remove(responseContent.eventId));
                if (target == null) {
                    LOG.warn("{} late local data response, connection inconsistency");
                    return;
                }
                LOG.debug("{} sending data:{} to:{}", new Object[]{logPrefix, responseContent.pieceId, target.getId()});
                KHeader<KAddress> responseHeader
                        = new DecoratedHeader(new BasicHeader(selfAdr, target, Transport.UDP), overlayId);
                KContentMsg response = new BasicContentMsg(responseHeader, responseContent);
                trigger(response, network);
            }
        };
    }

    public class ConnectionTracker {

        private final Map<Identifier, KAddress> uploadConnections = new HashMap<>();
        private final Map<Identifier, KAddress> downloadConnections = new HashMap<>();
        private final Set<Identifier> ghostUploadConnection = new HashSet<>();
        private final Set<Identifier> ghostDownloadConnection = new HashSet<>();

        public ConnectionTracker() {
        }

        /**
         * @return <uppConn, downConn>
         */
        public Pair<Integer, Integer> publishInternalState() {
            //cleaning ghost connections
            ghostDownloadConnection.clear();
            ghostUploadConnection.clear();

            return Pair.with(uploadConnections.size(), downloadConnections.size());
        }

        //fire and forget - if you get a response it is good. you might fire multiple requests to same node..solve duplicate requests on response side
        public void openDownloadConnection(KAddress target) {
            LOG.info("{} opening connection to:{}", logPrefix, target.getId());
            Connection.Request requestContent = new Connection.Request(overlayId, selfDesc.vodDesc);
            KHeader<KAddress> requestHeader
                    = new DecoratedHeader(new BasicHeader(selfAdr, target, Transport.UDP), overlayId);
            KContentMsg request = new BasicContentMsg(requestHeader, requestContent);
            trigger(request, network);
        }

        //fire and forget
        public void cleanCloseDownloadConnections() {
            for (KAddress target : downloadConnections.values()) {
                LOG.info("{} cleanup - closing download connection to:{}", new Object[]{logPrefix, target.getId()});
                sendClose(target, true);
            }
            downloadConnections.clear();
        }

        public void closeGhostUploadConnection(KAddress target) {
            if (uploadConnections.containsKey(target.getId())) {
                LOG.info("{} ignore close - not ghost upload connection to:{}", logPrefix, target.getId());
                return;
            }
            if (!ghostUploadConnection.contains(target.getId())) {
                LOG.info("{} marking ghost upload connection to:{}", logPrefix, target.getId());
                ghostUploadConnection.add(target.getId());
            } else {
                LOG.info("{} cleanup - closing ghost upload connection to:{}", new Object[]{logPrefix, target.getId()});
                ghostUploadConnection.remove(target.getId());
                sendClose(target, false);
            }
        }

        public void closeGhostDownloadConnection(KAddress target) {
            if (downloadConnections.containsKey(target.getId())) {
                LOG.info("{} ignore close - not ghost download connection to:{}", logPrefix, target.getId());
                return;
            }
            if (!ghostDownloadConnection.contains(target.getId())) {
                LOG.info("{} marking ghost download connection to:{}", logPrefix, target.getId());
                ghostDownloadConnection.add(target.getId());
            } else {
                LOG.info("{} cleanup - closing ghost download connection to:{}", new Object[]{logPrefix, target.getId()});
                ghostDownloadConnection.remove(target.getId());
                sendClose(target, false);
            }
        }

        public KAddress getDownloadConn(Identifier target) {
            KAddress addr = downloadConnections.get(target);
            if (addr == null) {
                LOG.warn("{} download connection inconsistency", logPrefix);
            }
            return addr;
        }

        public KAddress getUploadConn(Identifier target) {
            KAddress addr = uploadConnections.get(target);
            if (addr == null) {
                LOG.warn("{} upload connection inconsistency", logPrefix);
            }
            return addr;
        }

        public void updateAddress(KAddress target) {
            if (uploadConnections.containsKey(target.getId())) {
                uploadConnections.put(target.getId(), target);
            }
            if (downloadConnections.containsKey(target.getId())) {
                downloadConnections.put(target.getId(), target);
            }
        }

        //fire and forget
        Handler handleScheduledConnectionUpdate = new Handler<PeriodicConnUpdate>() {

            @Override
            public void handle(PeriodicConnUpdate event) {
                LOG.debug("{} connection update", logPrefix);
                for (KAddress partner : downloadConnections.values()) {
                    Connection.Update msgContent = new Connection.Update(overlayId, selfDesc.vodDesc, true);
                    KHeader<KAddress> msgHeader
                            = new DecoratedHeader(new BasicHeader(selfAdr, partner, Transport.UDP), overlayId);
                    KContentMsg msg = new BasicContentMsg(msgHeader, msgContent);
                    trigger(msg, network);
                }

                for (KAddress partner : uploadConnections.values()) {
                    Connection.Update msgContent = new Connection.Update(overlayId, selfDesc.vodDesc, false);
                    KHeader<KAddress> msgHeader
                            = new DecoratedHeader(new BasicHeader(selfAdr, partner, Transport.UDP), overlayId);
                    KContentMsg msg = new BasicContentMsg(msgHeader, msgContent);
                    trigger(msg, network);
                }
            }
        };

        ClassMatchedHandler handleConnectionRequest
                = new ClassMatchedHandler<Connection.Request, BasicContentMsg<KAddress, DecoratedHeader<KAddress>, Connection.Request>>() {

                    @Override
                    public void handle(Connection.Request content, BasicContentMsg<KAddress, DecoratedHeader<KAddress>, Connection.Request> container) {
                        KAddress target = container.getHeader().getSource();
                        updateAddress(target);

                        LOG.info("{} received download connection request from:{}", new Object[]{logPrefix, target.getId()});
                        if (!uploadConnections.containsKey(target.getId())) {
                            if (!uploadTracker.connect(target, content.desc)) {
                                LOG.info("{} rejecting download connection request from:{}", logPrefix, target.getId());
                                return;
                            }
                            ghostUploadConnection.remove(target.getId());
                            uploadConnections.put(target.getId(), target);
                        }
                        LOG.info("{} accept download connection to:{}", logPrefix, target);
                        Connection.Response responseContent = content.accept(selfDesc.vodDesc);
                        KHeader<KAddress> responseHeader
                        = new DecoratedHeader(new BasicHeader(selfAdr, target, Transport.UDP), overlayId);
                        KContentMsg response = new BasicContentMsg(responseHeader, responseContent);
                        trigger(response, network);
                    }
                };

        //TODO Alex - connection response status - currently a response means by default connection accepted
        ClassMatchedHandler handleConnectionResponse
                = new ClassMatchedHandler<Connection.Response, BasicContentMsg<KAddress, DecoratedHeader<KAddress>, Connection.Response>>() {

                    @Override
                    public void handle(Connection.Response content, BasicContentMsg<KAddress, DecoratedHeader<KAddress>, Connection.Response> container) {
                        KAddress target = container.getHeader().getSource();
                        updateAddress(target);

                        LOG.info("{} received download connection response from:{}", new Object[]{logPrefix, target.getId()});
                        if (!downloadTracker.connect(target, content.desc)) {
                            LOG.info("{} dT reject: closing download connection to:{}", logPrefix, target.getId());
                            ghostDownloadConnection.remove(target.getId());
                            sendClose(target, true);
                            return;
                        }
                        ghostDownloadConnection.remove(target.getId());
                        downloadConnections.put(target.getId(), target);
                    }
                };

        //TODO Alex - if miss more than k updates consider connection stale and close
        ClassMatchedHandler handleConnectionUpdate
                = new ClassMatchedHandler<Connection.Update, BasicContentMsg<KAddress, DecoratedHeader<KAddress>, Connection.Update>>() {

                    @Override
                    public void handle(Connection.Update content, BasicContentMsg<KAddress, DecoratedHeader<KAddress>, Connection.Update> container) {
                        KAddress target = container.getHeader().getSource();
                        updateAddress(target);

                        //remember local view of connection is reverse to what the sender sees it 
                        String connectionType = !content.downloadConnection ? "download" : "upload";
                        LOG.debug("{} received {} connection update from:{}", new Object[]{logPrefix, connectionType, target.getId()});
                        if (!content.downloadConnection) {
                            if (downloadConnections.containsKey(target.getId())) {
                                if (!downloadTracker.keepConnect(target, content.desc)) {
                                    LOG.info("{} dT reject - closing download connection to:{}", logPrefix, target.getId());
                                    sendClose(target, true);
                                    downloadConnections.remove(target.getId());
                                    ghostDownloadConnection.remove(target.getId());
                                }
                            } else {
                                closeGhostDownloadConnection(target);
                            }
                        } else {
                            if (uploadConnections.containsKey(target.getId())) {
                                if (!uploadTracker.keepConnect(target, content.desc)) {
                                    LOG.info("{} uT reject - closing upload connection to:{}", logPrefix, target.getId());
                                    sendClose(target, false);
                                    ghostUploadConnection.remove(target.getId());
                                    uploadConnections.remove(target.getId());
                                }
                            } else {
                                closeGhostUploadConnection(target);
                            }
                        }
                    }
                };

        ClassMatchedHandler handleConnectionClose
                = new ClassMatchedHandler<Connection.Close, BasicContentMsg<KAddress, DecoratedHeader<KAddress>, Connection.Close>>() {

                    @Override
                    public void handle(Connection.Close content, BasicContentMsg<KAddress, DecoratedHeader<KAddress>, Connection.Close> container) {
                        KAddress target = container.getHeader().getSource();
                        String connectionType = !content.downloadConnection ? "upload" : "download"; //from receiver perspective
                        LOG.info("{} received close {} connection from:{}", new Object[]{logPrefix, connectionType, target.getId()});

                        //from receiver perspective it is opposite
                        if (!content.downloadConnection) {
                            downloadTracker.close(target);
                            downloadConnections.remove(target.getId());
                        } else {
                            uploadTracker.close(target);
                            uploadConnections.remove(target.getId());
                        }
                    }
                };

        private void sendClose(KAddress target, boolean downloadConnection) {
            Connection.Close msgContent = new Connection.Close(overlayId, downloadConnection);
            KHeader<KAddress> msgHeader
                    = new DecoratedHeader(new BasicHeader(selfAdr, target, Transport.UDP), overlayId);
            KContentMsg msg = new BasicContentMsg(msgHeader, msgContent);
            trigger(msg, network);
        }
    }
}
