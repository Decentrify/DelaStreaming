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
import se.sics.gvod.croupierfake.CroupierPort;
import se.sics.gvod.croupierfake.CroupierSample;
import se.sics.gvod.common.util.VodDescriptor;
import se.sics.gvod.common.msg.vod.Connection;
import se.sics.gvod.common.msg.vod.Download;
import se.sics.gvod.common.utility.UtilityUpdate;
import se.sics.gvod.common.utility.UtilityUpdatePort;
import se.sics.gvod.core.connMngr.msg.Ready;
import se.sics.kompics.ClassMatchedHandler;
import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.Handler;
import se.sics.kompics.Init;
import se.sics.kompics.Negative;
import se.sics.kompics.Positive;
import se.sics.kompics.Start;
import se.sics.kompics.Stop;
import se.sics.kompics.network.Network;
import se.sics.kompics.network.Transport;
import se.sics.kompics.timer.CancelTimeout;
import se.sics.kompics.timer.SchedulePeriodicTimeout;
import se.sics.kompics.timer.ScheduleTimeout;
import se.sics.kompics.timer.Timeout;
import se.sics.kompics.timer.Timer;
import se.sics.p2ptoolbox.util.network.ContentMsg;
import se.sics.p2ptoolbox.util.network.impl.BasicAddress;
import se.sics.p2ptoolbox.util.network.impl.BasicContentMsg;
import se.sics.p2ptoolbox.util.network.impl.BasicHeader;
import se.sics.p2ptoolbox.util.network.impl.DecoratedAddress;
import se.sics.p2ptoolbox.util.network.impl.DecoratedHeader;

/**
 * @author Alex Ormenisan <aaor@sics.se>
 */
public class ConnMngrComp extends ComponentDefinition {

    private static final Logger LOG = LoggerFactory.getLogger(ConnMngrComp.class);
    private String logPrefix;

    private Negative<ConnMngrPort> myPort = provides(ConnMngrPort.class);
    private Positive<Network> network = requires(Network.class);
    private Positive<Timer> timer = requires(Timer.class);
    private Positive<CroupierPort> croupier = requires(CroupierPort.class);
    private Positive<UtilityUpdatePort> utilityUpdate = requires(UtilityUpdatePort.class);

    private final ConnMngrConfig config;
    private ConnectionTracker connTracker;
    private UploadTracker uploadTracker;
    private DownloadTracker downloadTracker;

    private UUID periodicISCheckTid = null;
    private UUID periodicConnUpdateTid = null;

    private LocalVodDescriptor selfDesc;

    public ConnMngrComp(ConnMngrInit init) {
        this.config = init.config;
        this.logPrefix = config.getSelf().getId().toString() + "<" + config.overlayId + ">";
        LOG.info("{} initiating...", logPrefix);

        this.connTracker = new ConnectionTracker();
        this.uploadTracker = new UploadTracker();
        this.downloadTracker = new DownloadTracker();

        subscribe(handleStart, control);
        subscribe(handleStop, control);
        subscribe(handlePeriodicISCheck, timer);
        subscribe(handleUpdateUtility, utilityUpdate);
    }

    private Handler handleStart = new Handler<Start>() {
        @Override
        public void handle(Start e) {
            LOG.info("{} starting...", logPrefix);
            schedulePeriodicISCheck();
        }
    };

    private Handler handleStop = new Handler<Stop>() {
        @Override
        public void handle(Stop e) {
            LOG.info("{} stopping...", logPrefix);
            cancelPeriodicISCheck();
            cancelConnUpdate();
        }
    };

    private void startConnectionTracker() {
        subscribe(connTracker.handleConnectionRequest, network);
        subscribe(connTracker.handleConnectionResponse, network);
        subscribe(connTracker.handleConnectionUpdate, network);
        subscribe(connTracker.handleConnectionClose, network);
        subscribe(connTracker.handleScheduledConnectionUpdate, timer);
        scheduleConnUpdate();
    }

    private void startDownloadTracker() {
        subscribe(downloadTracker.handleCroupierSample, croupier);
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

    private Handler<UtilityUpdate> handleUpdateUtility = new Handler<UtilityUpdate>() {

        @Override
        public void handle(UtilityUpdate event) {
            LOG.debug("{} descriptor update:{}", logPrefix, selfDesc);

            if (selfDesc == null) {
                startConnectionTracker();
                startUploadTracker();
                if (event.downloading) {
                    startDownloadTracker();
                }
            } else {
                if (selfDesc.downloading && !event.downloading) {
                    LOG.info("{} download complete - closing download connections", logPrefix);
                    if (!downloadTracker.isEmpty()) {
                        LOG.warn("{} download tracker not empty at download complete", logPrefix);
                    }
                    downloadTracker.clear();
                    connTracker.cleanCloseDownloadConnections();
                }
            }
            selfDesc = new LocalVodDescriptor(new VodDescriptor(event.downloadPos), event.downloading);
        }
    };

    Handler handlePeriodicISCheck = new Handler<PeriodicInternalStateCheck>() {
        @Override
        public void handle(PeriodicInternalStateCheck e) {
            LOG.debug("{} periodic ISCheck");
            LOG.info("{} descriptor:{}", logPrefix, selfDesc);
            connTracker.publishInternalState();
            uploadTracker.publishInternalState();
            downloadTracker.publishInternalState();
        }
    };

    //**********************Timeouts*******************************************
    private void schedulePeriodicISCheck() {
        if (periodicISCheckTid != null) {
            LOG.warn("{} double schedule of periodic ISCHeck timeout", logPrefix);
        } else {
            LOG.trace("{} scheduling periodic ISCheck timeout", logPrefix);
        }
        SchedulePeriodicTimeout spt = new SchedulePeriodicTimeout(config.periodicStateCheck, config.periodicStateCheck);
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
        SchedulePeriodicTimeout spt = new SchedulePeriodicTimeout(config.periodicConnUpdate, config.periodicConnUpdate);
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

    private UUID scheduleDownloadDataTimeout(UUID reqId, BasicAddress target) {
        ScheduleTimeout st = new ScheduleTimeout(config.reqTimeoutPeriod);
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

        public final BasicAddress target;
        public final UUID reqId;

        public DownloadDataTimeout(ScheduleTimeout schedule, UUID reqId, BasicAddress target) {
            super(schedule);
            this.reqId = reqId;
            this.target = target;
        }
    }

    private UUID scheduleDownloadHashTimeout(UUID reqId, BasicAddress target) {
        ScheduleTimeout st = new ScheduleTimeout(config.reqTimeoutPeriod);
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

        public final BasicAddress target;
        public final UUID reqId;

        public DownloadHashTimeout(ScheduleTimeout schedule, UUID reqId, BasicAddress target) {
            super(schedule);
            this.reqId = reqId;
            this.target = target;
        }
    }

    //**************************************************************************
    public static class ConnMngrInit extends Init<ConnMngrComp> {

        public final ConnMngrConfig config;

        public ConnMngrInit(ConnMngrConfig config) {
            this.config = config;
        }
    }

    public class DownloadTracker {

        private Map<BasicAddress, Map<UUID, Pair<Download.HashRequest, UUID>>> pendingDownloadingHash; //<address, <requestId, <request, timeoutId>>> 
        private Map<BasicAddress, Map<UUID, Pair<Download.DataRequest, UUID>>> pendingDownloadingData; //<address, <requestId, <request, timeoutId>>> 
        private Map<BasicAddress, VodDescriptor> uploaders;

        public DownloadTracker() {
            this.pendingDownloadingHash = new HashMap<BasicAddress, Map<UUID, Pair<Download.HashRequest, UUID>>>();
            this.pendingDownloadingData = new HashMap<BasicAddress, Map<UUID, Pair<Download.DataRequest, UUID>>>();
            this.uploaders = new HashMap<BasicAddress, VodDescriptor>();
        }

        public void publishInternalState() {
            LOG.info("{} {} available uploaders", logPrefix, uploaders.size());
            for (Map.Entry<BasicAddress, Map<UUID, Pair<Download.HashRequest, UUID>>> e : pendingDownloadingHash.entrySet()) {
                LOG.info("{} pending download hash from:{} size:{}", new Object[]{logPrefix, e.getKey(), e.getValue().size()});
            }
            for (Map.Entry<BasicAddress, Map<UUID, Pair<Download.DataRequest, UUID>>> e : pendingDownloadingData.entrySet()) {
                LOG.info("{} pending download data from:{} size:{}", new Object[]{logPrefix, e.getKey(), e.getValue().size()});
            }
        }

        public boolean isEmpty() {
            return pendingDownloadingData.isEmpty() && pendingDownloadingHash.isEmpty();
        }

        public void clear() {
            uploaders.clear();
        }

        public boolean connect(DecoratedAddress target, VodDescriptor desc) {
            tryPublishReady();
            uploaders.put(target.getBase(), desc);
            return true;
        }

        public boolean keepConnect(DecoratedAddress target, VodDescriptor desc) {
            if (uploaders.containsKey(target.getBase())) {
                tryPublishReady();
                uploaders.put(target.getBase(), desc);
                return true;
            }
            return false;
        }

        //TODO Alex - might want to keep local load in a variable...this might be expensive
        public void tryPublishReady() {
            int localLoad = 0;
            for (Map<UUID, Pair<Download.HashRequest, UUID>> hashQueue : pendingDownloadingHash.values()) {
                localLoad += hashQueue.size();
            }
            for (Map<UUID, Pair<Download.DataRequest, UUID>> dataQueue : pendingDownloadingData.values()) {
                localLoad += dataQueue.size();
            }
            int freeSlots = config.defaultMaxPipeline - localLoad;
            //TODO Alex hardcoded slots
            freeSlots = freeSlots > 15 ? 15 : freeSlots;
            if (freeSlots > 0) {
                LOG.info("{} ready slots:{}", logPrefix, freeSlots);
                trigger(new Ready(UUID.randomUUID(), freeSlots), myPort);
            }
        }

        public void close(DecoratedAddress target) {
            Map<UUID, Pair<Download.HashRequest, UUID>> hashQueue = pendingDownloadingHash.remove(target.getBase());
            if (hashQueue != null) {
                for (Map.Entry<UUID, Pair<Download.HashRequest, UUID>> e : hashQueue.entrySet()) {
                    Download.HashRequest hashReq = e.getValue().getValue0();
                    trigger(hashReq.timeout(), myPort);
                    cancelDownloadHashTimeout(e.getValue().getValue1());
                }
            }

            Map<UUID, Pair<Download.DataRequest, UUID>> dataQueue = pendingDownloadingData.remove(target.getBase());
            if (dataQueue != null) {
                for (Map.Entry<UUID, Pair<Download.DataRequest, UUID>> e : dataQueue.entrySet()) {
                    Download.DataRequest dataReq = e.getValue().getValue0();
                    trigger(dataReq.timeout(), myPort);
                    cancelDownloadDataTimeout(e.getValue().getValue1());
                }
            }

            uploaders.remove(target.getBase());
        }

        Handler handleCroupierSample = new Handler<CroupierSample>() {

            @Override
            public void handle(CroupierSample event) {
                if (selfDesc.downloading) {
                    LOG.debug("{} received new croupier samples", logPrefix);

                    for (Map.Entry<DecoratedAddress, VodDescriptor> e : event.sample.entrySet()) {
                        if (e.getValue().downloadPos < selfDesc.vodDesc.downloadPos) {
                            continue;
                        }
                        if (uploaders.containsKey(e.getKey().getBase())) {
                            continue;
                        }
                        connTracker.openDownloadConnection(e.getKey());
                    }
                }
            }
        };

        Handler handleLocalHashRequest = new Handler<Download.HashRequest>() {

            @Override
            public void handle(Download.HashRequest requestContent) {
                LOG.debug("{} handle local hash request:{}", logPrefix, requestContent.targetPos);
                DecoratedAddress uploader = getUploader(requestContent.targetPos);
                if (uploader == null) {
                    LOG.info("{} no candidate for position:{}", new Object[]{logPrefix, requestContent.targetPos});
                    trigger(requestContent.busy(), myPort);
                    return;
                }
                Map<UUID, Pair<Download.HashRequest, UUID>> uploaderQueue = pendingDownloadingHash.get(uploader.getBase());
                if (uploaderQueue == null) {
                    uploaderQueue = new HashMap<UUID, Pair<Download.HashRequest, UUID>>();
                    pendingDownloadingHash.put(uploader.getBase(), uploaderQueue);
                }
                LOG.debug("{} sending hash request:{} to:{}",
                        new Object[]{logPrefix, requestContent.hashes, uploader.getBase()});
                UUID tId = scheduleDownloadHashTimeout(requestContent.id, uploader.getBase());
                uploaderQueue.put(requestContent.id, Pair.with(requestContent, tId));
                DecoratedHeader<DecoratedAddress> requestHeader = new DecoratedHeader(new BasicHeader(config.getSelf(), uploader, Transport.UDP), null, config.overlayId);
                ContentMsg request = new BasicContentMsg(requestHeader, requestContent);
                trigger(request, network);
            }
        };

        Handler handleLocalDataRequest = new Handler<Download.DataRequest>() {
            @Override
            public void handle(Download.DataRequest requestContent) {
                LOG.debug("{} handle local data request:{}", logPrefix, requestContent.pieceId);

                DecoratedAddress uploader = getUploader(requestContent.pieceId / config.piecesPerBlock);
                if (uploader == null) {
                    LOG.debug("{} no candidate for piece:{}", new Object[]{logPrefix, requestContent.pieceId});
                    trigger(requestContent.busy(), myPort);
                    return;
                }
                Map<UUID, Pair<Download.DataRequest, UUID>> uploaderQueue = pendingDownloadingData.get(uploader.getBase());
                if (uploaderQueue == null) {
                    uploaderQueue = new HashMap<UUID, Pair<Download.DataRequest, UUID>>();
                    pendingDownloadingData.put(uploader.getBase(), uploaderQueue);
                }
                LOG.debug("{} sending data request:{} to:{}",
                        new Object[]{logPrefix, requestContent.pieceId, uploader.getBase()});
                UUID tId = scheduleDownloadDataTimeout(requestContent.id, uploader.getBase());
                uploaderQueue.put(requestContent.id, Pair.with(requestContent, tId));
                DecoratedHeader<DecoratedAddress> requestHeader = new DecoratedHeader(new BasicHeader(config.getSelf(), uploader, Transport.UDP), null, config.overlayId);
                ContentMsg request = new BasicContentMsg(requestHeader, requestContent);
                trigger(request, network);
            }
        };

        private DecoratedAddress getUploader(int blockPos) {
            Iterator<Map.Entry<BasicAddress, VodDescriptor>> it = uploaders.entrySet().iterator();
            Map.Entry<BasicAddress, VodDescriptor> candidate = null;

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
                Map.Entry<BasicAddress, VodDescriptor> nextCandidate = it.next();
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

                Map<UUID, Pair<Download.DataRequest, UUID>> uploaderQueue = pendingDownloadingData.get(timeout.target);
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

                Map<UUID, Pair<Download.HashRequest, UUID>> uploaderQueue = pendingDownloadingHash.get(timeout.target);
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
                = new ClassMatchedHandler<Download.HashResponse, BasicContentMsg<DecoratedAddress, DecoratedHeader<DecoratedAddress>, Download.HashResponse>>() {

                    @Override
                    public void handle(Download.HashResponse content, BasicContentMsg<DecoratedAddress, DecoratedHeader<DecoratedAddress>, Download.HashResponse> container) {
                        DecoratedAddress target = container.getHeader().getSource();
                        connTracker.updateAddress(target);

                        LOG.debug("{} received net hash response from:{}", new Object[]{logPrefix, target.getBase()});
                        Map<UUID, Pair<Download.HashRequest, UUID>> uploaderQueue = pendingDownloadingHash.get(target.getBase());
                        if (uploaderQueue == null) {
                            LOG.info("{} hash from:{} - posibly late", logPrefix, target.getBase());
                            return;
                        }
                        Pair<Download.HashRequest, UUID> req = uploaderQueue.remove(content.id);
                        if (req == null) {
                            LOG.debug("{} hash from:{} - posibly late", logPrefix, target.getBase());
                            return;
                        }

                        cancelDownloadHashTimeout(req.getValue1());
                        trigger(content, myPort);

                        if (uploaderQueue.isEmpty()) {
                            pendingDownloadingHash.remove(target.getBase());
                        }
                    }
                };

        ClassMatchedHandler handleNetDataResponse
                = new ClassMatchedHandler<Download.DataResponse, BasicContentMsg<DecoratedAddress, DecoratedHeader<DecoratedAddress>, Download.DataResponse>>() {

                    @Override
                    public void handle(Download.DataResponse content, BasicContentMsg<DecoratedAddress, DecoratedHeader<DecoratedAddress>, Download.DataResponse> container) {
                        DecoratedAddress target = container.getHeader().getSource();
                        connTracker.updateAddress(target);

                        LOG.trace("{} receiver net data response from:{}", new Object[]{logPrefix, target.getBase()});
                        Map<UUID, Pair<Download.DataRequest, UUID>> uploaderQueue = pendingDownloadingData.get(target.getBase());
                        if (uploaderQueue == null) {
                            LOG.info("{} data from:{} - posibly late", logPrefix, target.getBase());
                            return;
                        }
                        Pair<Download.DataRequest, UUID> req = uploaderQueue.remove(content.id);
                        if (req == null) {
                            LOG.info("{} data from:{} posibly late", logPrefix, target.getBase());
                            return;
                        }

                        cancelDownloadDataTimeout(req.getValue1());
                        trigger(content, myPort);

                        if (uploaderQueue.isEmpty()) {
                            pendingDownloadingData.remove(target.getBase());
                        }
                    }
                };
    }

    public class UploadTracker {

        //TODO Alex - might want to optimise and to catch requests for same package and ask only once from the store
        public Map<BasicAddress, VodDescriptor> downloaders;
        private Map<UUID, BasicAddress> pendingUploadingHash; //<requestId, target> 
        private Map<UUID, BasicAddress> pendingUploadingData; //<requestId, target> 

        public UploadTracker() {
            this.downloaders = new HashMap<BasicAddress, VodDescriptor>();
            this.pendingUploadingHash = new HashMap<UUID, BasicAddress>();
            this.pendingUploadingData = new HashMap<UUID, BasicAddress>();
        }

        public void publishInternalState() {
            LOG.info("{} downloaders:{}", logPrefix, downloaders.size());
            LOG.info("{} pending uploading hash:{}", logPrefix, pendingUploadingHash.size());
            LOG.info("{} pending uploading data:{}", logPrefix, pendingUploadingData.size());
        }

        public boolean connect(DecoratedAddress target, VodDescriptor desc) {
            downloaders.put(target.getBase(), desc);
            return true;
        }

        public boolean keepConnect(DecoratedAddress target, VodDescriptor desc) {
            if (downloaders.containsKey(target.getBase())) {
                downloaders.put(target.getBase(), desc);
                return true;
            }
            return false;
        }

        public void close(DecoratedAddress target) {
            downloaders.remove(target.getBase());

            Iterator<Map.Entry<UUID, BasicAddress>> it;
            int removed;

            it = pendingUploadingHash.entrySet().iterator();
            removed = 0;
            while (it.hasNext()) {
                Map.Entry<UUID, BasicAddress> e = it.next();
                if (target.getBase().equals(e.getValue())) {
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
                Map.Entry<UUID, BasicAddress> e = it.next();
                if (target.getBase().equals(e.getValue())) {
                    removed++;
                    it.remove();
                }
            }
            if (removed > 0) {
                LOG.info("{} closing connection to:{}, with {} pending data requests", logPrefix, removed);
            }
        }

        ClassMatchedHandler handleNetHashRequest
                = new ClassMatchedHandler<Download.HashRequest, BasicContentMsg<DecoratedAddress, DecoratedHeader<DecoratedAddress>, Download.HashRequest>>() {

                    @Override
                    public void handle(Download.HashRequest content, BasicContentMsg<DecoratedAddress, DecoratedHeader<DecoratedAddress>, Download.HashRequest> container) {
                        DecoratedAddress target = container.getHeader().getSource();
                        connTracker.updateAddress(target);

                        LOG.debug("{} received net hash request from:{}, for:{}", new Object[]{logPrefix, target.getBase(), content.targetPos});
                        if (!downloaders.containsKey(target.getBase())) {
                            LOG.warn("{} no connection open to:{}, dropping hash request:{}", new Object[]{logPrefix, target.getBase(), content.hashes});
                            connTracker.closeGhostUploadConnection(target);
                            return;
                        }

                        if (!pendingUploadingHash.containsKey(content.id)) {
                            pendingUploadingHash.put(content.id, target.getBase());
                            trigger(content, myPort);
                        } else {
                            LOG.warn("{} request already registered", logPrefix);
                        }
                    }
                };

        ClassMatchedHandler handleNetDataRequest
                = new ClassMatchedHandler<Download.DataRequest, BasicContentMsg<DecoratedAddress, DecoratedHeader<DecoratedAddress>, Download.DataRequest>>() {

                    @Override
                    public void handle(Download.DataRequest content, BasicContentMsg<DecoratedAddress, DecoratedHeader<DecoratedAddress>, Download.DataRequest> container) {
                        DecoratedAddress target = container.getHeader().getSource();
                        connTracker.updateAddress(target);

                        LOG.debug("{} received net data request from:{}, for:{}", new Object[]{logPrefix, target.getBase(), content.pieceId});
                        if (!downloaders.containsKey(target.getBase())) {
                            LOG.warn("{} no connection open to:{}, dropping data request:{}", new Object[]{logPrefix, target.getBase(), content.pieceId});
                            connTracker.closeGhostUploadConnection(target);
                            return;
                        }

                        if (!pendingUploadingData.containsKey(content.id)) {
                            pendingUploadingData.put(content.id, target.getBase());
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

                if (!pendingUploadingHash.containsKey(responseContent.id)) {
                    LOG.warn("{} late local hash response, inconsistency");
                    return;
                }
                DecoratedAddress target = connTracker.getUploadConn(pendingUploadingHash.remove(responseContent.id));
                if (target == null) {
                    LOG.warn("{} late local hash response, connection inconsistency");
                    return;
                }
                LOG.debug("{} sending hash:{} to:{}", new Object[]{logPrefix, responseContent.targetPos, target.getBase()});
                DecoratedHeader<DecoratedAddress> responseHeader = new DecoratedHeader(new BasicHeader(config.getSelf(), target, Transport.UDP), null, config.overlayId);
                ContentMsg response = new BasicContentMsg(responseHeader, responseContent);
                trigger(response, network);
            }
        };

        Handler handleLocalDataResponse = new Handler<Download.DataResponse>() {

            @Override
            public void handle(Download.DataResponse responseContent) {
                LOG.debug("{} received local data response:{}", logPrefix, responseContent.pieceId);

                if (!pendingUploadingData.containsKey(responseContent.id)) {
                    LOG.warn("{} late local data response, inconsistency", logPrefix);
                    return;
                }

                DecoratedAddress target = connTracker.getUploadConn(pendingUploadingData.remove(responseContent.id));
                if (target == null) {
                    LOG.warn("{} late local data response, connection inconsistency");
                    return;
                }
                LOG.debug("{} sending data:{} to:{}", new Object[]{logPrefix, responseContent.pieceId, target.getBase()});
                DecoratedHeader<DecoratedAddress> responseHeader = new DecoratedHeader(new BasicHeader(config.getSelf(), target, Transport.UDP), null, config.overlayId);
                ContentMsg response = new BasicContentMsg(responseHeader, responseContent);
                trigger(response, network);
            }
        };
    }

    public class ConnectionTracker {

        private final Map<BasicAddress, DecoratedAddress> uploadConnections;
        private final Map<BasicAddress, DecoratedAddress> downloadConnections;
        private final Set<BasicAddress> ghostUploadConnection;
        private final Set<BasicAddress> ghostDownloadConnection;

        public ConnectionTracker() {
            this.downloadConnections = new HashMap<BasicAddress, DecoratedAddress>();
            this.uploadConnections = new HashMap<BasicAddress, DecoratedAddress>();
            this.ghostDownloadConnection = new HashSet<BasicAddress>();
            this.ghostUploadConnection = new HashSet<BasicAddress>();
        }

        public void publishInternalState() {
            LOG.info("{} uploadConnection:{} downloadConnections:{}",
                    new Object[]{logPrefix, uploadConnections.size(), downloadConnections.size()});
            //cleaning ghost connections
            ghostDownloadConnection.clear();
            ghostUploadConnection.clear();
        }

        //fire and forget - if you get a response it is good. you might fire multiple requests to same node..solve duplicate requests on response side
        public void openDownloadConnection(DecoratedAddress target) {
            LOG.info("{} opening connection to:{}", logPrefix, target.getBase());
            Connection.Request requestContent = new Connection.Request(UUID.randomUUID(), selfDesc.vodDesc);
            DecoratedHeader<DecoratedAddress> requestHeader = new DecoratedHeader(new BasicHeader(config.getSelf(), target, Transport.UDP), null, config.overlayId);
            ContentMsg request = new BasicContentMsg(requestHeader, requestContent);
            trigger(request, network);
        }

        //fire and forget
        public void cleanCloseDownloadConnections() {
            for (DecoratedAddress target : downloadConnections.values()) {
                LOG.info("{} cleanup - closing download connection to:{}", new Object[]{logPrefix, target.getBase()});
                sendClose(target, true);
            }
            downloadConnections.clear();
        }

        public void closeGhostUploadConnection(DecoratedAddress target) {
            if (uploadConnections.containsKey(target.getBase())) {
                LOG.info("{} ignore close - not ghost upload connection to:{}", logPrefix, target.getBase());
                return;
            }
            if (!ghostUploadConnection.contains(target.getBase())) {
                LOG.info("{} marking ghost upload connection to:{}", logPrefix, target.getBase());
                ghostUploadConnection.add(target.getBase());
            } else {
                LOG.info("{} cleanup - closing ghost upload connection to:{}", new Object[]{logPrefix, target.getBase()});
                ghostUploadConnection.remove(target.getBase());
                sendClose(target, false);
            }
        }

        public void closeGhostDownloadConnection(DecoratedAddress target) {
            if (downloadConnections.containsKey(target.getBase())) {
                LOG.info("{} ignore close - not ghost download connection to:{}", logPrefix, target.getBase());
                return;
            }
            if (!ghostDownloadConnection.contains(target.getBase())) {
                LOG.info("{} marking ghost download connection to:{}", logPrefix, target.getBase());
                ghostDownloadConnection.add(target.getBase());
            } else {
                LOG.info("{} cleanup - closing ghost download connection to:{}", new Object[]{logPrefix, target.getBase()});
                ghostDownloadConnection.remove(target.getBase());
                sendClose(target, false);
            }
        }

        public DecoratedAddress getDownloadConn(BasicAddress target) {
            DecoratedAddress addr = downloadConnections.get(target);
            if (addr == null) {
                LOG.warn("{} download connection inconsistency", logPrefix);
            }
            return addr;
        }

        public DecoratedAddress getUploadConn(BasicAddress target) {
            DecoratedAddress addr = uploadConnections.get(target);
            if (addr == null) {
                LOG.warn("{} upload connection inconsistency", logPrefix);
            }
            return addr;
        }

        public void updateAddress(DecoratedAddress target) {
            if (uploadConnections.containsKey(target.getBase())) {
                uploadConnections.put(target.getBase(), target);
            }
            if (downloadConnections.containsKey(target.getBase())) {
                downloadConnections.put(target.getBase(), target);
            }
        }

        //fire and forget
        Handler handleScheduledConnectionUpdate = new Handler<PeriodicConnUpdate>() {

            @Override
            public void handle(PeriodicConnUpdate event) {
                LOG.debug("{} connection update", logPrefix);
                for (DecoratedAddress partner : downloadConnections.values()) {
                    Connection.Update msgContent = new Connection.Update(UUID.randomUUID(), selfDesc.vodDesc, true);
                    DecoratedHeader<DecoratedAddress> msgHeader = new DecoratedHeader(new BasicHeader(config.getSelf(), partner, Transport.UDP), null, config.overlayId);
                    ContentMsg msg = new BasicContentMsg(msgHeader, msgContent);
                    trigger(msg, network);
                }

                for (DecoratedAddress partner : uploadConnections.values()) {
                    Connection.Update msgContent = new Connection.Update(UUID.randomUUID(), selfDesc.vodDesc, false);
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
                        DecoratedAddress target = container.getHeader().getSource();
                        updateAddress(target);

                        LOG.info("{} received download connection request from:{}", new Object[]{logPrefix, target.getBase()});
                        if (!uploadConnections.containsKey(target.getBase())) {
                            if (!uploadTracker.connect(target, content.desc)) {
                                LOG.info("{} rejecting download connection request from:{}", logPrefix, target.getBase());
                                return;
                            }
                            ghostUploadConnection.remove(target.getBase());
                            uploadConnections.put(target.getBase(), target);
                        }
                        LOG.info("{} accept download connection to:{}", logPrefix, target);
                        Connection.Response responseContent = content.accept(selfDesc.vodDesc);
                        DecoratedHeader<DecoratedAddress> responseHeader = new DecoratedHeader(new BasicHeader(config.getSelf(), target, Transport.UDP), null, config.overlayId);
                        ContentMsg response = new BasicContentMsg(responseHeader, responseContent);
                        trigger(response, network);
                    }
                };

        //TODO Alex - connection response status - currently a response means by default connection accepted
        ClassMatchedHandler handleConnectionResponse
                = new ClassMatchedHandler<Connection.Response, BasicContentMsg<DecoratedAddress, DecoratedHeader<DecoratedAddress>, Connection.Response>>() {

                    @Override
                    public void handle(Connection.Response content, BasicContentMsg<DecoratedAddress, DecoratedHeader<DecoratedAddress>, Connection.Response> container) {
                        DecoratedAddress target = container.getHeader().getSource();
                        updateAddress(target);

                        LOG.info("{} received download connection response from:{}", new Object[]{logPrefix, target.getBase()});
                        if (!downloadTracker.connect(target, content.desc)) {
                            LOG.info("{} dT reject: closing download connection to:{}", logPrefix, target.getBase());
                            ghostDownloadConnection.remove(target.getBase());
                            sendClose(target, true);
                            return;
                        }
                        ghostDownloadConnection.remove(target.getBase());
                        downloadConnections.put(target.getBase(), target);
                    }
                };

        //TODO Alex - if miss more than k updates consider connection stale and close
        ClassMatchedHandler handleConnectionUpdate
                = new ClassMatchedHandler<Connection.Update, BasicContentMsg<DecoratedAddress, DecoratedHeader<DecoratedAddress>, Connection.Update>>() {

                    @Override
                    public void handle(Connection.Update content, BasicContentMsg<DecoratedAddress, DecoratedHeader<DecoratedAddress>, Connection.Update> container) {
                        DecoratedAddress target = container.getHeader().getSource();
                        updateAddress(target);

                        //remember local view of connection is reverse to what the sender sees it 
                        String connectionType = !content.downloadConnection ? "download" : "upload";
                        LOG.debug("{} received {} connection update from:{}", new Object[]{logPrefix, connectionType, target.getBase()});
                        if (!content.downloadConnection) {
                            if (downloadConnections.containsKey(target.getBase())) {
                                if (!downloadTracker.keepConnect(target, content.desc)) {
                                    LOG.info("{} dT reject - closing download connection to:{}", logPrefix, target.getBase());
                                    sendClose(target, true);
                                    downloadConnections.remove(target.getBase());
                                    ghostDownloadConnection.remove(target.getBase());
                                }
                            } else {
                                closeGhostDownloadConnection(target);
                            }
                        } else {
                            if (uploadConnections.containsKey(target.getBase())) {
                                if (!uploadTracker.keepConnect(target, content.desc)) {
                                    LOG.info("{} uT reject - closing upload connection to:{}", logPrefix, target.getBase());
                                    sendClose(target, false);
                                    ghostUploadConnection.remove(target.getBase());
                                    uploadConnections.remove(target.getBase());
                                }
                            } else {
                                closeGhostUploadConnection(target);
                            }
                        }
                    }
                };

        ClassMatchedHandler handleConnectionClose
                = new ClassMatchedHandler<Connection.Close, BasicContentMsg<DecoratedAddress, DecoratedHeader<DecoratedAddress>, Connection.Close>>() {

                    @Override
                    public void handle(Connection.Close content, BasicContentMsg<DecoratedAddress, DecoratedHeader<DecoratedAddress>, Connection.Close> container) {
                        DecoratedAddress target = container.getHeader().getSource();
                        String connectionType = !content.downloadConnection ? "upload" : "download"; //from receiver perspective
                        LOG.info("{} received close {} connection from:{}", new Object[]{logPrefix, connectionType, target.getBase()});

                        //from receiver perspective it is opposite
                        if (!content.downloadConnection) {
                            downloadTracker.close(target);
                            downloadConnections.remove(target.getBase());
                        } else {
                            uploadTracker.close(target);
                            uploadConnections.remove(target.getBase());
                        }
                    }
                };

        private void sendClose(DecoratedAddress target, boolean downloadConnection) {
            Connection.Close msgContent = new Connection.Close(UUID.randomUUID(), downloadConnection);
            DecoratedHeader<DecoratedAddress> msgHeader = new DecoratedHeader(new BasicHeader(config.getSelf(), target, Transport.UDP), null, config.overlayId);
            ContentMsg msg = new BasicContentMsg(msgHeader, msgContent);
            trigger(msg, network);
        }
    }
}
