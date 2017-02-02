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
package se.sics.nstream.hops.library;

import com.google.common.base.Optional;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.security.UserGroupInformation;
import org.javatuples.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.gvod.mngr.util.ElementSummary;
import se.sics.gvod.mngr.util.TorrentExtendedStatus;
import se.sics.kompics.ComponentProxy;
import se.sics.kompics.Handler;
import se.sics.kompics.Negative;
import se.sics.kompics.Positive;
import se.sics.kompics.config.Config;
import se.sics.ktoolbox.util.Either;
import se.sics.ktoolbox.util.identifiable.Identifier;
import se.sics.ktoolbox.util.identifiable.overlay.OverlayId;
import se.sics.ktoolbox.util.identifiable.overlay.OverlayIdFactory;
import se.sics.ktoolbox.util.network.KAddress;
import se.sics.ktoolbox.util.result.Result;
import se.sics.nstream.FileId;
import se.sics.nstream.StreamId;
import se.sics.nstream.TorrentIds;
import se.sics.nstream.hops.HopsFED;
import se.sics.nstream.hops.hdfs.HDFSComp;
import se.sics.nstream.hops.hdfs.HDFSEndpoint;
import se.sics.nstream.hops.hdfs.HDFSHelper;
import se.sics.nstream.hops.hdfs.HDFSResource;
import se.sics.nstream.hops.kafka.KafkaEndpoint;
import se.sics.nstream.hops.kafka.KafkaResource;
import se.sics.nstream.hops.library.event.core.HopsTorrentDownloadEvent;
import se.sics.nstream.hops.library.event.core.HopsTorrentUploadEvent;
import se.sics.nstream.hops.manifest.DiskHelper;
import se.sics.nstream.hops.manifest.ManifestHelper;
import se.sics.nstream.hops.manifest.ManifestJSON;
import se.sics.nstream.library.Library;
import se.sics.nstream.library.disk.LibrarySummaryHelper;
import se.sics.nstream.library.disk.LibrarySummaryJSON;
import se.sics.nstream.library.event.torrent.HopsContentsEvent;
import se.sics.nstream.library.event.torrent.TorrentExtendedStatusEvent;
import se.sics.nstream.library.util.TorrentState;
import se.sics.nstream.storage.durable.DEndpointCtrlPort;
import se.sics.nstream.storage.durable.DurableStorageProvider;
import se.sics.nstream.storage.durable.disk.DiskComp;
import se.sics.nstream.storage.durable.disk.DiskEndpoint;
import se.sics.nstream.storage.durable.disk.DiskFED;
import se.sics.nstream.storage.durable.disk.DiskResource;
import se.sics.nstream.storage.durable.events.DEndpointConnect;
import se.sics.nstream.storage.durable.util.FileExtendedDetails;
import se.sics.nstream.storage.durable.util.MyStream;
import se.sics.nstream.storage.durable.util.StreamEndpoint;
import se.sics.nstream.storage.durable.util.StreamResource;
import se.sics.nstream.torrent.TorrentMngrPort;
import se.sics.nstream.torrent.event.StartTorrent;
import se.sics.nstream.torrent.status.event.DownloadSummaryEvent;
import se.sics.nstream.torrent.tracking.TorrentStatusPort;
import se.sics.nstream.torrent.transfer.TransferCtrlPort;
import se.sics.nstream.torrent.transfer.event.ctrl.GetRawTorrent;
import se.sics.nstream.torrent.transfer.event.ctrl.SetupTransfer;
import se.sics.nstream.transfer.MyTorrent;
import se.sics.nstream.util.FileBaseDetails;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class HopsLibraryMngr {

    private static final Logger LOG = LoggerFactory.getLogger(HopsLibraryMngr.class);
    private String logPrefix = "";

    private final Config config;
    private final ComponentHelper componentHelper;
    private final KAddress selfAdr;
    private final Library library;
    private final EndpointTracker endpointTracker;
    private final TransferTracker transferTracker;
    private final LibOpTracker libOpTracker;

    public HopsLibraryMngr(ComponentProxy proxy, Config config, String logPrefix, KAddress selfAdr) {
        this.logPrefix = logPrefix;
        this.config = config;
        this.selfAdr = selfAdr;

        componentHelper = new ComponentHelper(proxy);
        library = new Library(config.getValue("library.summary", String.class));
        endpointTracker = new EndpointTracker(logPrefix, componentHelper);
        transferTracker = new TransferTracker(logPrefix, componentHelper);

        libOpTracker = new LibOpTracker(logPrefix, config, componentHelper, endpointTracker, transferTracker, library, selfAdr.getId());
    }

    public void start() {
        restartTorrents();
    }

    public void close() {
    }

    public void cleanAndDestroy(OverlayId torrentId) {
        library.destroyed(torrentId);
    }

    private void restartTorrents() {
        LOG.info("{}restarting torrents", logPrefix);
        HopsLibraryKConfig hopsLibraryConfig = new HopsLibraryKConfig(config);
        OverlayIdFactory torrentIdFactory = TorrentIds.torrentIdFactory();

        Result<LibrarySummaryJSON> librarySummary = LibrarySummaryHelper.readTorrentList(config.getValue("library.summary", String.class));
        if (!librarySummary.isSuccess()) {
            throw new RuntimeException("fix me - corrupted library");
        }
        Map<OverlayId, Library.Torrent> torrents = LibrarySummaryHelper.fromSummary(librarySummary.getValue(), TorrentIds.torrentIdFactory());

        //TODO Alex - important - when registering endpoints - what happens if I register multiple endpoints
        for(Map.Entry<OverlayId, Library.Torrent> torrent : torrents.entrySet()) {
            //TODO - cleaner way than check resource
            if(torrent.getValue().manifestStream.endpoint instanceof DiskEndpoint) {
                LOG.info("{}restarting DISK torrent:{}", logPrefix, torrent.getValue().torrentName);
                libOpTracker.restartDiskUpload(torrent.getKey(), torrent.getValue().torrentName, torrent.getValue().manifestStream);
            } else {
                LOG.info("{}restarting HDFS torrent:{}", logPrefix, torrent.getValue().torrentName);
                libOpTracker.restartHDFSUpload(torrent.getKey(), torrent.getValue().torrentName, torrent.getValue().manifestStream);
            }
        }
    }

    public static interface BasicCompleteCallback<V extends Object> {

        public void ready(Result<V> result);
    }

    //**************************************************************************
    public static class ComponentHelper {

        private final ComponentProxy proxy;
        //**************************************************************************
        private final Negative<HopsTorrentPort> libraryCtrlPort;
        private final Positive<DEndpointCtrlPort> endpointControlPort;
        private final Positive<TorrentMngrPort> torrentMngrPort;
        private final Positive<TransferCtrlPort> transferCtrlPort;
        private final Positive<TorrentStatusPort> torrentStatusPort;

        public ComponentHelper(ComponentProxy proxy) {
            this.proxy = proxy;
            libraryCtrlPort = proxy.getPositive(HopsTorrentPort.class).getPair();
            endpointControlPort = proxy.getNegative(DEndpointCtrlPort.class).getPair();
            torrentMngrPort = proxy.getNegative(TorrentMngrPort.class).getPair();
            transferCtrlPort = proxy.getNegative(TransferCtrlPort.class).getPair();
            torrentStatusPort = proxy.getNegative(TorrentStatusPort.class).getPair();
        }
    }

    //**************************CONNECTING ENDPOINTS****************************
    public static interface EndpointCallback extends BasicCompleteCallback<Boolean> {

        public void setEndpoints(Map<String, Identifier> nameToId, Map<Identifier, StreamEndpoint> endpoints);
    }

    public static class EndpointTracker {

        private final String logPrefix;
        private final ComponentHelper helper;
        private final EndpointIdRegistry endpointIdRegistry;
        //**********************************************************************
        private final Map<Identifier, List<EndpointCallbackWrapper>> pendingEndpoints = new HashMap<>();

        public EndpointTracker(String logPrefix, ComponentHelper componentHelper) {
            this.logPrefix = logPrefix;
            this.helper = componentHelper;
            this.endpointIdRegistry = new EndpointIdRegistry();

            helper.proxy.subscribe(handleEndpointConnected, helper.endpointControlPort);
        }

        private void registerEndpoints(List<DurableStorageProvider> storageEndpoints, final EndpointCallback callback) {
            EndpointCallbackWrapper callbackWrapper = new EndpointCallbackWrapper(callback);
            for (DurableStorageProvider dsp : storageEndpoints) {
                String endpointName = dsp.getName();
                if (endpointIdRegistry.registered(endpointName)) {
                    Identifier endpointId = endpointIdRegistry.lookup(endpointName);
                    LOG.info("{}endpoint:{} already registered", logPrefix, endpointName);
                    callbackWrapper.addConnected(endpointName, endpointId, dsp.getEndpoint());
                } else {
                    LOG.info("{}endpoint:{} waiting", logPrefix, endpointName);
                    Identifier endpointId = endpointIdRegistry.register(endpointName);
                    callbackWrapper.addWaiting(endpointName, endpointId, dsp.getEndpoint());
                    List<EndpointCallbackWrapper> waiting = pendingEndpoints.get(endpointId);
                    if (waiting == null) {
                        waiting = new ArrayList<>();
                        pendingEndpoints.put(endpointId, waiting);
                    }
                    waiting.add(callbackWrapper);
                    helper.proxy.trigger(new DEndpointConnect.Request(endpointId, dsp), helper.endpointControlPort);
                }
            }
            callbackWrapper.setupComplete();
        }

        Handler handleEndpointConnected = new Handler<DEndpointConnect.Success>() {
            @Override
            public void handle(DEndpointConnect.Success resp) {
                LOG.info("{}connected endpoint:{}", logPrefix, resp.req.endpointId);
                List<EndpointCallbackWrapper> waiting = pendingEndpoints.remove(resp.req.endpointId);
                if (waiting != null) {
                    for (EndpointCallbackWrapper callbackWrapper : waiting) {
                        callbackWrapper.connected(resp.req.endpointId);
                    }
                }
            }
        };
    }

    private static class EndpointCallbackWrapper {

        private final Map<String, Identifier> nameToId = new HashMap<>();
        private final Map<Identifier, StreamEndpoint> endpoints = new HashMap<>();
        private final Set<Identifier> connecting = new HashSet<>();
        private final EndpointCallback result;

        public EndpointCallbackWrapper(EndpointCallback result) {
            this.result = result;
        }

        public void addWaiting(String endpointName, Identifier endpointId, StreamEndpoint endpoint) {
            nameToId.put(endpointName, endpointId);
            endpoints.put(endpointId, endpoint);
            connecting.add(endpointId);
        }

        public void addConnected(String endpointName, Identifier endpointId, StreamEndpoint endpoint) {
            nameToId.put(endpointName, endpointId);
            endpoints.put(endpointId, endpoint);
        }

        public void setupComplete() {
            result.setEndpoints(nameToId, endpoints);
            if (connecting.isEmpty()) {
                result.ready(Result.success(true));
            }
        }

        public void connected(Identifier endpointId) {
            connecting.remove(endpointId);
            if (connecting.isEmpty()) {
                result.ready(Result.success(true));
            }
        }
    }

    //*****************************TRANSFER_START*******************************
    public static interface TransferCallbackWrapper<V extends Object> extends BasicCompleteCallback<V> {

        /**
         * @param result
         * @return false if should stop
         */
        public boolean started(Result<Boolean> result);

        public boolean hasTorrent();

        public MyTorrent getTorrent();

        public List<KAddress> getPeers();

        /**
         * @param transferResult
         * @return false if should stop
         */
        public boolean manifest(Result<MyTorrent.Manifest> transferResult, AdvanceTransferCallback callback);
    }

    public static class TransferTracker {

        private final String logPrefix;
        private final ComponentHelper helper;
        //**********************************************************************
        private final Map<OverlayId, TransferCallbackWrapper<Boolean>> pendingTransferStarts = new HashMap<>();

        public TransferTracker(String logPrefix, ComponentHelper helper) {
            this.logPrefix = logPrefix;
            this.helper = helper;
            helper.proxy.subscribe(handleTorrentStarted, helper.torrentMngrPort);
            helper.proxy.subscribe(handleRawTorrent, helper.transferCtrlPort);
            helper.proxy.subscribe(handleAdvanceTransfer, helper.transferCtrlPort);
        }

        public void startTransfer(OverlayId torrentId, TransferCallbackWrapper<Boolean> callback) {
            LOG.info("{}torrent:{} - starting", logPrefix, torrentId);
            StartTorrent.Request req = new StartTorrent.Request(torrentId, callback.getPeers());
            pendingTransferStarts.put(torrentId, callback);
            helper.proxy.trigger(req, helper.torrentMngrPort);
        }

        Handler handleTorrentStarted = new Handler<StartTorrent.Response>() {
            @Override
            public void handle(StartTorrent.Response resp) {
                if (resp.result.isSuccess()) {
                    LOG.debug("{}torrent:{} - started", logPrefix, resp.overlayId());
                } else {
                    LOG.debug("{}torrent:{} - start failed", logPrefix, resp.overlayId());
                }

                TransferCallbackWrapper callback = pendingTransferStarts.get(resp.torrentId);
                if (!callback.started(resp.result)) {
                    throw new RuntimeException("cleanup - todo");
                }

                if (callback.hasTorrent()) {
                    advanceTransfer(resp.torrentId, callback.getTorrent());
                } else {
                    LOG.debug("{}torrent:{} - getting raw torrent", logPrefix, resp.torrentId);
                    GetRawTorrent.Request req = new GetRawTorrent.Request(resp.torrentId);
                    helper.proxy.trigger(req, helper.transferCtrlPort);
                }
            }
        };

        Handler handleRawTorrent = new Handler<GetRawTorrent.Response>() {
            @Override
            public void handle(final GetRawTorrent.Response resp) {
                if (resp.result.isSuccess()) {
                    LOG.debug("{}torrent:{} - get raw torrent - received, saving...", logPrefix, resp.overlayId());
                } else {
                    LOG.debug("{}torrent:{} - get raw torrent - failed", logPrefix, resp.overlayId());
                }
                TransferCallbackWrapper mainCallback = pendingTransferStarts.get(resp.overlayId());
                AdvanceTransferCallback advanceCallback = new AdvanceTransferCallback() {
                    @Override
                    public void ready(MyTorrent torrent) {
                        advanceTransfer(resp.overlayId(), torrent);
                    }
                };
                if (!mainCallback.manifest(resp.result, advanceCallback)) {
                    throw new RuntimeException("cleanup - todo");
                }
            }
        };

        public void advanceTransfer(OverlayId torrentId, MyTorrent torrent) {
            LOG.debug("{}torrent:{} - transfer - setting up", logPrefix, torrentId);
            SetupTransfer.Request req = new SetupTransfer.Request(torrentId, torrent);
            helper.proxy.trigger(req, helper.transferCtrlPort);
        }

        Handler handleAdvanceTransfer = new Handler<SetupTransfer.Response>() {
            @Override
            public void handle(SetupTransfer.Response resp) {
                TransferCallbackWrapper callback = pendingTransferStarts.get(resp.overlayId());
                if (resp.result.isSuccess()) {
                    LOG.debug("{}torrent:{} - transfer - set up", logPrefix, resp.overlayId());
                } else {
                    LOG.debug("{}torrent:{} - transfer - set up failed", logPrefix, resp.overlayId());
                }
                callback.ready(resp.result);
            }
        };
    }

    public static interface AdvanceTransferCallback {

        public void ready(MyTorrent torrent);
    }

    public static class BasicTransfer implements TransferCallbackWrapper<Boolean> {

        private final MyTorrent torrent;
        private final List<KAddress> peers;
        private final BasicCompleteCallback<Boolean> complete;

        public BasicTransfer(MyTorrent torrent, List<KAddress> peers, BasicCompleteCallback<Boolean> complete) {
            this.torrent = torrent;
            this.peers = peers;
            this.complete = complete;
        }

        @Override
        public boolean started(Result<Boolean> result) {
            return true;
        }

        @Override
        public boolean hasTorrent() {
            return true;
        }

        @Override
        public MyTorrent getTorrent() {
            return torrent;
        }

        @Override
        public List<KAddress> getPeers() {
            return peers;
        }

        @Override
        public boolean manifest(Result<MyTorrent.Manifest> result, AdvanceTransferCallback callback) {
            throw new UnsupportedOperationException("basic should not get here");
        }

        @Override
        public void ready(Result<Boolean> result) {
            complete.ready(result);
        }
    }

    public static class ExtendedTransfer implements TransferCallbackWrapper<Boolean> {

        private final MyExtendedHelper helper;
        private final MyTorrentBuilder torrentBuilder;
        private final List<KAddress> peers;
        private final MyStream manifestStream;
        private final BasicCompleteCallback<Boolean> complete;
        //**********************************************************************
        private MyTorrent torrent = null;

        public ExtendedTransfer(MyExtendedHelper helper, MyTorrentBuilder torrentBuilder,
                List<KAddress> peers, MyStream manifestStream, BasicCompleteCallback<Boolean> complete) {
            this.helper = helper;
            this.torrentBuilder = torrentBuilder;
            this.peers = peers;
            this.manifestStream = manifestStream;
            this.complete = complete;
        }

        @Override
        public boolean started(Result<Boolean> result) {
            return true;
        }

        @Override
        public boolean hasTorrent() {
            return torrent != null;
        }

        @Override
        public MyTorrent getTorrent() {
            return torrent;
        }

        @Override
        public List<KAddress> getPeers() {
            return peers;
        }

        @Override
        public boolean manifest(Result<MyTorrent.Manifest> transferResult, final AdvanceTransferCallback callback) {
            if (!transferResult.isSuccess()) {
                throw new RuntimeException("todo cleanup");
            }
            Result<Boolean> writeResult = helper.manifestHelper().writeManifest(manifestStream.endpoint, manifestStream.resource, transferResult.getValue());
            if (!writeResult.isSuccess()) {
                throw new RuntimeException("todo cleanup");
            }
            Either<MyTorrent.Manifest, ManifestJSON> manifestResult = Either.left(transferResult.getValue());
            torrentBuilder.setManifest(manifestResult);
            BasicCompleteCallback<Pair<KafkaEndpoint, Map<String, KafkaResource>>> extendedDetailsCallback
                    = new BasicCompleteCallback<Pair<KafkaEndpoint, Map<String, KafkaResource>>>() {
                        @Override
                        public void ready(Result<Pair<KafkaEndpoint, Map<String, KafkaResource>>> kafkaDetails) {
                            if (kafkaDetails.isSuccess()) {
                                Map<FileId, FileExtendedDetails> extendedDetails
                                = helper.manifestHelper().extendedDetails(torrentBuilder, kafkaDetails.getValue().getValue0(), kafkaDetails.getValue().getValue1());
                                torrentBuilder.setExtendedDetails(extendedDetails);
                                torrent = torrentBuilder.getTorrent();
                            }
                            callback.ready(torrent);
                        }
                    };
            helper.extendedDetailsHelper().extendedDetails(extendedDetailsCallback);
            return true;
        }

        @Override
        public void ready(Result<Boolean> result) {
            complete.ready(result);
        }
    }

    public static interface MyManifestHelper {

        public Result<ManifestJSON> readManifest(StreamEndpoint endpoint, StreamResource resource);

        public Result<Boolean> writeManifest(StreamEndpoint endpoint, StreamResource resource, MyTorrent.Manifest result);

        public Map<FileId, FileExtendedDetails> extendedDetails(MyTorrentBuilder torrentBuilder, KafkaEndpoint kafkaEndpoint, Map<String, KafkaResource> kafkaDetails);
    }

    public static interface MyExtendedDetailsHelper {

        public void extendedDetails(BasicCompleteCallback<Pair<KafkaEndpoint, Map<String, KafkaResource>>> complete);
    }

    public static interface MyStorageHelper {

        public DurableStorageProvider getStorageProvider(StreamEndpoint endpoint);
    }

    public static interface MyBasicHelper {

        public MyStorageHelper storageHelper();

        public MyManifestHelper manifestHelper();
    }

    public static interface MyExtendedHelper extends MyBasicHelper {

        public MyExtendedDetailsHelper extendedDetailsHelper();
    }

    public static interface MyTorrentBuilder {

        public void setEndpoints(Map<String, Identifier> endpointNameToId, Map<Identifier, StreamEndpoint> endpoints);

        public void setManifestStream(StreamId streamId, MyStream stream);

        public Pair<StreamId, MyStream> getManifestStream();

        public void setManifest(Either<MyTorrent.Manifest, ManifestJSON> manifest);

        public Map<String, FileId> getFiles();

        public void setExtendedDetails(Map<FileId, FileExtendedDetails> extendedDetails);

        public MyTorrent getTorrent();
    }

    public static class MyTorrentBuilderImpl implements MyTorrentBuilder {

        public final OverlayId torrentId;

        private Map<String, Identifier> endpointNameToId;
        private Map<Identifier, StreamEndpoint> endpoints;
        private Pair<StreamId, MyStream> manifestStream;
        private MyTorrent.Manifest manifest;
        private Map<String, FileId> fileNameToId;
        private Map<FileId, FileBaseDetails> baseDetails;
        private Map<FileId, FileExtendedDetails> extendedDetails = new HashMap<>();

        public MyTorrentBuilderImpl(OverlayId torrentId) {
            this.torrentId = torrentId;
        }

        @Override
        public void setManifestStream(StreamId streamId, MyStream stream) {
            manifestStream = Pair.with(streamId, stream);
        }

        @Override
        public Pair<StreamId, MyStream> getManifestStream() {
            return manifestStream;
        }

        @Override
        public void setEndpoints(Map<String, Identifier> endpointNameToId, Map<Identifier, StreamEndpoint> endpoints) {
            this.endpointNameToId = endpointNameToId;
            this.endpoints = endpoints;
        }

        @Override
        public void setManifest(Either<MyTorrent.Manifest, ManifestJSON> auxManifest) {
            ManifestJSON manifestJSON;
            if (auxManifest.isLeft()) {
                manifest = auxManifest.getLeft();
                manifestJSON = ManifestHelper.getManifestJSON(manifest.manifestByte);
            } else {
                manifestJSON = auxManifest.getRight();
                manifest = ManifestHelper.getManifest(manifestJSON);
            }
            Pair< Map< String, FileId>, Map<FileId, FileBaseDetails>> aux = ManifestHelper.getBaseDetails(torrentId, manifestJSON, MyTorrent.defaultDataBlock);
            fileNameToId = aux.getValue0();
            baseDetails = aux.getValue1();
        }

        @Override
        public Map<String, FileId> getFiles() {
            return fileNameToId;
        }

        @Override
        public void setExtendedDetails(Map<FileId, FileExtendedDetails> extendedDetails) {
            this.extendedDetails = extendedDetails;
        }

        @Override
        public MyTorrent getTorrent() {
            if (endpointNameToId == null || endpoints == null || manifest == null || fileNameToId == null || baseDetails == null) {
                throw new RuntimeException("cleanup - todo");
            }

            return new MyTorrent(manifest, fileNameToId, baseDetails, extendedDetails);
        }
    }

    public static class BasicTorrent {

        private final EndpointTracker endpointTracker;
        private final TransferTracker transferTracker;
        private final MyBasicHelper helper;
        private final BasicCompleteCallback<Boolean> completeCallback;
        //**********************************************************************
        private final OverlayId torrentId;
        private final MyStream manifestStream;
        private final List<KAddress> torrentPeers;
        private final MyTorrentBuilder torrentBuilder;

        public BasicTorrent(EndpointTracker endpointTracker, TransferTracker transferTracker, MyBasicHelper helper, BasicCompleteCallback<Boolean> completeCallback,
                OverlayId torrentId, MyStream manifestStream, List<KAddress> torrentPeers, MyTorrentBuilder torrentBuilder) {
            this.endpointTracker = endpointTracker;
            this.transferTracker = transferTracker;
            this.helper = helper;
            this.completeCallback = completeCallback;
            this.torrentId = torrentId;
            this.manifestStream = manifestStream;
            this.torrentPeers = torrentPeers;
            this.torrentBuilder = torrentBuilder;
        }

        public void start() {
            setupEndpoints();
        }

        private void setupEndpoints() {
            List<DurableStorageProvider> storageProviders = new ArrayList<>();
            DurableStorageProvider storageProvider = helper.storageHelper().getStorageProvider(manifestStream.endpoint);
            storageProviders.add(storageProvider);

            EndpointCallback callback = new EndpointCallback() {
                @Override
                public void setEndpoints(Map<String, Identifier> nameToId, Map<Identifier, StreamEndpoint> endpoints) {
                    torrentBuilder.setEndpoints(nameToId, endpoints);
                    Identifier manifestEndpointId = nameToId.get(manifestStream.endpoint.getEndpointName());
                    StreamId manifestStreamId = TorrentIds.streamId(manifestEndpointId, TorrentIds.fileId(torrentId, MyTorrent.MANIFEST_ID));
                    torrentBuilder.setManifestStream(manifestStreamId, manifestStream);
                }

                @Override
                public void ready(Result<Boolean> result) {
                    if (result.isSuccess()) {
                        setupTransfer();
                    }
                }
            };
            endpointTracker.registerEndpoints(storageProviders, callback);
        }

        private void setupTransfer() {
            final Result<ManifestJSON> manifestJSON = helper.manifestHelper().readManifest(manifestStream.endpoint, manifestStream.resource);
            if (!manifestJSON.isSuccess()) {
                throw new RuntimeException("cleanup - todo");
            }
            Either<MyTorrent.Manifest, ManifestJSON> manifestResult = Either.right(manifestJSON.getValue());
            torrentBuilder.setManifest(manifestResult);
            Map<FileId, FileExtendedDetails> extendedDetails = helper.manifestHelper().extendedDetails(torrentBuilder, null, new HashMap<String, KafkaResource>());
            torrentBuilder.setExtendedDetails(extendedDetails);
            BasicCompleteCallback<Boolean> setupComplete = new BasicCompleteCallback<Boolean>() {
                @Override
                public void ready(Result<Boolean> result) {
                    BasicTorrent.this.ready(result);
                }
            };
            MyTorrent torrent = torrentBuilder.getTorrent();
            TransferCallbackWrapper setupTransfer = new BasicTransfer(torrent, torrentPeers, setupComplete);
            transferTracker.startTransfer(torrentId, setupTransfer);
        }

        private void ready(Result<Boolean> result) {
            completeCallback.ready(result);
        }
    }

    public static class ExtendedTorrent {

        private final EndpointTracker endpointTracker;
        private final TransferTracker transferTracker;
        private final MyExtendedHelper helper;
        private final BasicCompleteCallback<Boolean> completeCallback;
        //**********************************************************************
        private final OverlayId torrentId;
        private final MyStream manifestStream;
        private final List<KAddress> torrentPeers;
        private final MyTorrentBuilder torrentBuilder;
        //**********************************************************************
        private StreamId manifestStreamId;

        public ExtendedTorrent(EndpointTracker endpointTracker, TransferTracker transferTracker, MyExtendedHelper helper, BasicCompleteCallback<Boolean> completeCallback,
                OverlayId torrentId, MyStream manifestStream, List<KAddress> torrentPeers, MyTorrentBuilder torrentBuilder) {
            this.endpointTracker = endpointTracker;
            this.transferTracker = transferTracker;
            this.helper = helper;
            this.completeCallback = completeCallback;
            this.torrentId = torrentId;
            this.manifestStream = manifestStream;
            this.torrentPeers = torrentPeers;
            this.torrentBuilder = torrentBuilder;
        }

        public void start() {
            setupEndpoints();
        }

        private void setupEndpoints() {
            List<DurableStorageProvider> storageProviders = new ArrayList<>();
            DurableStorageProvider storageProvider = helper.storageHelper().getStorageProvider(manifestStream.endpoint);
            storageProviders.add(storageProvider);

            EndpointCallback callback = new EndpointCallback() {
                @Override
                public void setEndpoints(Map<String, Identifier> nameToId, Map<Identifier, StreamEndpoint> endpoints) {
                    torrentBuilder.setEndpoints(nameToId, endpoints);
                    Identifier manifestEndpointId = nameToId.get(manifestStream.endpoint.getEndpointName());
                    StreamId manifestStreamId = TorrentIds.streamId(manifestEndpointId, TorrentIds.fileId(torrentId, MyTorrent.MANIFEST_ID));
                    torrentBuilder.setManifestStream(manifestStreamId, manifestStream);
                }

                @Override
                public void ready(Result<Boolean> result) {
                    if (result.isSuccess()) {
                        setupTransfer();
                    }
                }
            };
            endpointTracker.registerEndpoints(storageProviders, callback);
        }

        private void setupTransfer() {
            BasicCompleteCallback<Boolean> setupComplete = new BasicCompleteCallback<Boolean>() {
                @Override
                public void ready(Result<Boolean> result) {
                    ExtendedTorrent.this.ready(result);
                }
            };
            ExtendedTransfer setupTransfer = new ExtendedTransfer(helper, torrentBuilder, torrentPeers, manifestStream, setupComplete);
            transferTracker.startTransfer(torrentId, setupTransfer);
        }

        private void ready(Result<Boolean> result) {
            completeCallback.ready(result);
        }
    }

    public static class MyDiskHelper implements MyBasicHelper {

        private final Identifier selfId;

        public MyDiskHelper(Identifier selfId) {
            this.selfId = selfId;
        }

        @Override
        public MyStorageHelper storageHelper() {
            return new MyStorageHelper() {
                @Override
                public DurableStorageProvider getStorageProvider(StreamEndpoint endpoint) {
                    return new DiskComp.StorageProvider(selfId);
                }
            };
        }

        @Override
        public MyManifestHelper manifestHelper() {
            return new MyManifestHelper() {

                @Override
                public Result<ManifestJSON> readManifest(StreamEndpoint endpoint, StreamResource resource) {
                    return DiskHelper.readManifest((DiskResource) resource);
                }

                @Override
                public Result<Boolean> writeManifest(StreamEndpoint endpoint, StreamResource resource, MyTorrent.Manifest manifest) {
                    ManifestJSON manifestJSON = ManifestHelper.getManifestJSON(manifest.manifestByte);
                    Result<Boolean> result = DiskHelper.writeManifest((DiskResource) resource, manifestJSON);
                    return result;
                }

                @Override
                public Map<FileId, FileExtendedDetails> extendedDetails(MyTorrentBuilder torrentBuilder, KafkaEndpoint kafkaEndpoint, Map<String, KafkaResource> kafkaDetails) {
                    Map<FileId, FileExtendedDetails> extendedDetails = new HashMap<>();
                    Pair<StreamId, MyStream> manifestStream = torrentBuilder.getManifestStream();
                    DiskResource manifestResource = (DiskResource) manifestStream.getValue1().resource;
                    for (Map.Entry<String, FileId> file : torrentBuilder.getFiles().entrySet()) {
                        StreamId fileStreamId = manifestStream.getValue0().withFile(file.getValue());
                        DiskResource fileResource = manifestResource.withFile(file.getKey());
                        MyStream fileStream = manifestStream.getValue1().withResource(fileResource);
                        extendedDetails.put(file.getValue(), new DiskFED(fileStreamId, fileStream));
                    }
                    return extendedDetails;
                }
            };
        }
    }

    public static class MyHDFSHelper implements MyBasicHelper {

        private final Identifier selfId;

        public MyHDFSHelper(Identifier selfId) {
            this.selfId = selfId;

        }

        @Override
        public MyStorageHelper storageHelper() {
            return new MyStorageHelper() {
                @Override
                public DurableStorageProvider getStorageProvider(StreamEndpoint endpoint) {
                    return new HDFSComp.StorageProvider(selfId, (HDFSEndpoint) endpoint);
                }
            };
        }

        @Override
        public MyManifestHelper manifestHelper() {
            return new MyManifestHelper() {

                @Override
                public Result<ManifestJSON> readManifest(StreamEndpoint endpoint, StreamResource resource) {
                    HDFSEndpoint hdfsEndpoint = (HDFSEndpoint) endpoint;
                    //TODO Alex - creating too many ugi's
                    UserGroupInformation ugi = UserGroupInformation.createRemoteUser(hdfsEndpoint.user);
                    return HDFSHelper.readManifest(ugi, hdfsEndpoint, (HDFSResource) resource);
                }

                @Override
                public Result<Boolean> writeManifest(StreamEndpoint endpoint, StreamResource resource, MyTorrent.Manifest manifest) {
                    ManifestJSON manifestJSON = ManifestHelper.getManifestJSON(manifest.manifestByte);
                    HDFSEndpoint hdfsEndpoint = (HDFSEndpoint) endpoint;
                    //TODO Alex - creating too many ugi's
                    UserGroupInformation ugi = UserGroupInformation.createRemoteUser(hdfsEndpoint.user);
                    Result<Boolean> result = HDFSHelper.writeManifest(ugi, hdfsEndpoint, (HDFSResource) resource, manifestJSON);
                    return result;
                }

                @Override
                public Map<FileId, FileExtendedDetails> extendedDetails(MyTorrentBuilder torrentBuilder, KafkaEndpoint kafkaEndpoint, Map<String, KafkaResource> kafkaDetails) {
                    Map<FileId, FileExtendedDetails> extendedDetails = new HashMap<>();
                    Pair<StreamId, MyStream> manifestStream = torrentBuilder.getManifestStream();
                    HDFSResource manifestResource = (HDFSResource) manifestStream.getValue1().resource;
                    for (Map.Entry<String, FileId> file : torrentBuilder.getFiles().entrySet()) {
                        StreamId fileStreamId = manifestStream.getValue0().withFile(file.getValue());
                        HDFSResource fileResource = manifestResource.withFile(file.getKey());
                        MyStream fileStream = manifestStream.getValue1().withResource(fileResource);
                        //TODO Alex - kafka
                        Optional<Pair<StreamId, MyStream>> kafkaStream = Optional.absent();
                        extendedDetails.put(file.getValue(), new HopsFED(Pair.with(fileStreamId, fileStream), kafkaStream));
                    }
                    return extendedDetails;
                }
            };
        }
    }

    //**************************************************************************
    public static class LibOpTracker {

        private static final HopsTorrentUploadEvent.Request RESTART_NOHUP_REQ = new HopsTorrentUploadEvent.Request(null, null, null, null);

        private final String logPrefix;
        private final HopsLibraryKConfig hopsLibraryConfig;
        private final ComponentHelper comp;
        private final EndpointTracker endpointTracker;
        private final TransferTracker transferTracker;
        private final Library library;
        private final Identifier selfId;
        //**********************************************************************
        private final Map<OverlayId, HopsTorrentUploadEvent.Request> pendingDiskUploads = new HashMap<>();
        private final Map<OverlayId, HopsTorrentDownloadEvent.StartRequest> pendingDiskDownloads = new HashMap<>();
        private final Map<OverlayId, HopsTorrentUploadEvent.Request> pendingHDFSUploads = new HashMap<>();
        private final Map<OverlayId, HopsTorrentDownloadEvent.StartRequest> pendingHDFSStartDownloads = new HashMap<>();
        private final Map<OverlayId, BasicCompleteCallback> pendingHDFSDownloadCallback = new HashMap<>();
        private final Map<OverlayId, HopsTorrentDownloadEvent.AdvanceRequest> pendingHDFSAdvanceDownloads = new HashMap<>();

        public LibOpTracker(String logPrefix, Config config, ComponentHelper comp, EndpointTracker endpointTracker, TransferTracker transferTracker,
                Library library, Identifier selfId) {
            this.logPrefix = logPrefix;
            this.comp = comp;
            this.endpointTracker = endpointTracker;
            this.transferTracker = transferTracker;
            this.library = library;
            this.selfId = selfId;

            this.hopsLibraryConfig = new HopsLibraryKConfig(config);

            if (hopsLibraryConfig.baseEndpointType.equals(Details.Types.DISK)) {
                comp.proxy.subscribe(handleDiskTorrentUpload, comp.libraryCtrlPort);
                comp.proxy.subscribe(handleDiskTorrentDownload, comp.libraryCtrlPort);
            } else {
                comp.proxy.subscribe(handleHDFSTorrentUpload, comp.libraryCtrlPort);
                comp.proxy.subscribe(handleHDFSTorrentDownload, comp.libraryCtrlPort);
                comp.proxy.subscribe(handleHDFSTorrentAdvanceDownload, comp.libraryCtrlPort);
            }
            comp.proxy.subscribe(handleContents, comp.libraryCtrlPort);
            comp.proxy.subscribe(handleTorrentDetails, comp.libraryCtrlPort);
            comp.proxy.subscribe(handleTorrentStatus, comp.torrentStatusPort);
        }

        Handler handleContents = new Handler<HopsContentsEvent.Request>() {

            @Override
            public void handle(HopsContentsEvent.Request req) {
                LOG.trace("{}received:{}", logPrefix, req);
                comp.proxy.answer(req, req.success(library.getSummary()));
            }
        };
        
        Handler handleTorrentDetails = new Handler<TorrentExtendedStatusEvent.Request>() {
            @Override
            public void handle(TorrentExtendedStatusEvent.Request req) {
                LOG.trace("{}received:{}", logPrefix, req);
                TorrentExtendedStatus tes = library.getExtendedStatus(req.torrentId);
                if(tes == null) {
                    LOG.warn("{}torrent:{} not found", logPrefix, req.torrentId);
                    for(ElementSummary es : library.getSummary()) {
                        LOG.warn("{}found torrent:{}", logPrefix, es.torrentId);
                    }
                    tes = new TorrentExtendedStatus(req.torrentId, TorrentState.NONE, 0, 0);
                }
                comp.proxy.answer(req, req.succes(tes));
            }
        };
        
        Handler handleTorrentStatus = new Handler<DownloadSummaryEvent>(){
            @Override
            public void handle(DownloadSummaryEvent ind) {
                LOG.trace("{}received:{}", logPrefix, ind);
                LOG.info("{}download completed:{}", logPrefix, ind.torrentId);
                library.finishDownload(ind.torrentId);
            }
        };
        
        Handler handleDiskTorrentUpload = new Handler<HopsTorrentUploadEvent.Request>() {
            @Override
            public void handle(final HopsTorrentUploadEvent.Request req) {
                LOG.trace("{}received:{}", logPrefix, req);

                Result<Boolean> validateResult = validateUploadRequest(req);
                if (!validateResult.isSuccess() || validateResult.getValue()) {
                    comp.proxy.answer(req, req.failed(validateResult));
                }

                library.prepareUpload(req.torrentId, req.torrentName);

                OverlayId torrentId = req.torrentId;
                final MyStream torrentStream = new MyStream(new DiskEndpoint(), new DiskResource(req.manifestResource.dirPath, req.manifestResource.fileName));
                List<KAddress> torrentPeers = new ArrayList<>();
                MyTorrentBuilder torrentBuilder = new MyTorrentBuilderImpl(torrentId);

                MyBasicHelper torrentHelper = new MyDiskHelper(selfId);

                BasicCompleteCallback<Boolean> completeCallback = new BasicCompleteCallback<Boolean>() {
                    @Override
                    public void ready(Result<Boolean> result) {
                        pendingDiskUploads.remove(req.torrentId);
                        library.upload(req.torrentId, torrentStream);
                        comp.proxy.answer(req, req.success(result));
                    }
                };

                BasicTorrent upload = new BasicTorrent(endpointTracker, transferTracker, torrentHelper, completeCallback, torrentId, torrentStream, torrentPeers, torrentBuilder);
                pendingDiskUploads.put(req.torrentId, req);
                upload.start();
            }
        };

        public void restartDiskUpload(final OverlayId torrentId, String torrentName, final MyStream manifestStream) {
            library.prepareUpload(torrentId, torrentName);

            List<KAddress> torrentPeers = new ArrayList<>();
            MyTorrentBuilder torrentBuilder = new MyTorrentBuilderImpl(torrentId);

            MyBasicHelper torrentHelper = new MyDiskHelper(selfId);

            BasicCompleteCallback<Boolean> completeCallback = new BasicCompleteCallback<Boolean>() {
                @Override
                public void ready(Result<Boolean> result) {
                    pendingDiskUploads.remove(torrentId);
                    library.upload(torrentId, manifestStream);
                }
            };

            BasicTorrent upload = new BasicTorrent(endpointTracker, transferTracker, torrentHelper, completeCallback, torrentId, manifestStream, torrentPeers, torrentBuilder);
            pendingDiskUploads.put(torrentId, RESTART_NOHUP_REQ);
            upload.start();
        }

        Handler handleHDFSTorrentUpload = new Handler<HopsTorrentUploadEvent.Request>() {
            @Override
            public void handle(final HopsTorrentUploadEvent.Request req) {
                LOG.trace("{}received:{}", logPrefix, req);

                Result<Boolean> validateResult = validateUploadRequest(req);
                if (!validateResult.isSuccess()) {
                    LOG.debug("{}validation error", logPrefix);
                    comp.proxy.answer(req, req.failed(validateResult));
                }
                if (validateResult.getValue()) {
                    LOG.debug("{}already uploading", logPrefix);
                    comp.proxy.answer(req, req.success(validateResult));
                }

                library.prepareUpload(req.torrentId, req.torrentName);

                OverlayId torrentId = req.torrentId;
                final MyStream torrentStream = new MyStream(req.hdfsEndpoint, req.manifestResource);
                List<KAddress> torrentPeers = new ArrayList<>();
                MyTorrentBuilder torrentBuilder = new MyTorrentBuilderImpl(torrentId);

                MyBasicHelper torrentHelper = new MyHDFSHelper(selfId);

                BasicCompleteCallback<Boolean> completeCallback = new BasicCompleteCallback<Boolean>() {
                    @Override
                    public void ready(Result<Boolean> result) {
                        pendingHDFSUploads.remove(req.torrentId);
                        library.upload(req.torrentId, torrentStream);
                        comp.proxy.answer(req, req.success(result));
                    }
                };

                BasicTorrent upload = new BasicTorrent(endpointTracker, transferTracker, torrentHelper, completeCallback, torrentId, torrentStream, torrentPeers, torrentBuilder);
                pendingHDFSUploads.put(req.torrentId, req);
                upload.start();
            }
        };

        public void restartHDFSUpload(final OverlayId torrentId, String torrentName, final MyStream manifestStream) {
            library.prepareUpload(torrentId, torrentName);

            List<KAddress> torrentPeers = new ArrayList<>();
            MyTorrentBuilder torrentBuilder = new MyTorrentBuilderImpl(torrentId);

            MyBasicHelper torrentHelper = new MyHDFSHelper(selfId);

            BasicCompleteCallback<Boolean> completeCallback = new BasicCompleteCallback<Boolean>() {
                @Override
                public void ready(Result<Boolean> result) {
                    pendingHDFSUploads.remove(torrentId);
                    library.upload(torrentId, manifestStream);
                }
            };

            BasicTorrent upload = new BasicTorrent(endpointTracker, transferTracker, torrentHelper, completeCallback, torrentId, manifestStream, torrentPeers, torrentBuilder);
            pendingHDFSUploads.put(torrentId, RESTART_NOHUP_REQ);
            upload.start();
        }

        private Result<Boolean> validateUploadRequest(HopsTorrentUploadEvent.Request req) {
            TorrentState status = library.getStatus(req.torrentId);
            switch (status) {
                case PREPARE_UPLOAD:
                case UPLOADING:
                    if (compareUploadDetails()) {
                        return Result.success(true);
                    } else {
                        return Result.badArgument("existing - upload details do not match");
                    }
                case PREPARE_DOWNLOAD:
                case DOWNLOADING:
                    return Result.badArgument("expected NONE/UPLOADING, found:" + status);
                case DESTROYED:
                case NONE:
                    return Result.success(false);
                default:
                    return Result.internalStateFailure("missing logic");
            }
        }

        private boolean compareUploadDetails() {
            return true;
        }

        Handler handleDiskTorrentDownload = new Handler<HopsTorrentDownloadEvent.StartRequest>() {
            @Override
            public void handle(final HopsTorrentDownloadEvent.StartRequest req) {
                LOG.trace("{}received:{}", logPrefix, req);

                Result<Boolean> validateResult = validateDownloadRequest(req);
                if (!validateResult.isSuccess()) {
                    LOG.debug("{}validation error", logPrefix);
                    comp.proxy.answer(req, req.failed(validateResult));
                }
                if (validateResult.getValue()) {
                    LOG.debug("{}already uploading", logPrefix);
                    comp.proxy.answer(req, req.success(validateResult));
                }

                library.prepareDownload(req.torrentId, req.torrentName);
                OverlayId torrentId = req.torrentId;
                final MyStream torrentStream = new MyStream(new DiskEndpoint(), new DiskResource(req.manifest.dirPath, req.manifest.fileName));
                List<KAddress> torrentPeers = req.partners;
                MyTorrentBuilder torrentBuilder = new MyTorrentBuilderImpl(torrentId);

                MyExtendedHelper torrentHelper = new MyExtendedHelper() {
                    private final MyBasicHelper base = new MyDiskHelper(selfId);

                    @Override
                    public MyStorageHelper storageHelper() {
                        return base.storageHelper();
                    }

                    @Override
                    public MyManifestHelper manifestHelper() {
                        return base.manifestHelper();
                    }

                    @Override
                    public MyExtendedDetailsHelper extendedDetailsHelper() {
                        return new MyExtendedDetailsHelper() {
                            @Override
                            public void extendedDetails(BasicCompleteCallback complete) {
                                complete.ready(Result.success(Pair.with(null, new HashMap<String, KafkaResource>())));
                            }
                        };
                    }
                };

                BasicCompleteCallback<Boolean> completeCallback = new BasicCompleteCallback<Boolean>() {
                    @Override
                    public void ready(Result<Boolean> result) {
                        if (result.isSuccess()) {
                            pendingDiskDownloads.remove(req.torrentId);
                            library.download(req.torrentId, torrentStream);
                            comp.proxy.answer(req, req.success(result));
                        } else {
                            throw new RuntimeException("todo - cleanup");
                        }
                    }
                };

                ExtendedTorrent upload = new ExtendedTorrent(endpointTracker, transferTracker, torrentHelper, completeCallback, torrentId, torrentStream, torrentPeers, torrentBuilder);
                pendingDiskDownloads.put(req.torrentId, req);
                upload.start();
            }
        };

        Handler handleHDFSTorrentDownload = new Handler<HopsTorrentDownloadEvent.StartRequest>() {
            @Override
            public void handle(final HopsTorrentDownloadEvent.StartRequest req) {
                LOG.trace("{}received:{}", logPrefix, req);

                Result<Boolean> validateResult = validateDownloadRequest(req);
                if (!validateResult.isSuccess()) {
                    LOG.debug("{}validation error", logPrefix);
                    comp.proxy.answer(req, req.failed(validateResult));
                }
                if (validateResult.getValue()) {
                    LOG.debug("{}already uploading", logPrefix);
                    comp.proxy.answer(req, req.success(validateResult));
                }

                library.prepareDownload(req.torrentId, req.torrentName);
                OverlayId torrentId = req.torrentId;
                final MyStream torrentStream = new MyStream(req.hdfsEndpoint, req.manifest);
                List<KAddress> torrentPeers = req.partners;
                MyTorrentBuilder torrentBuilder = new MyTorrentBuilderImpl(torrentId);

                MyExtendedHelper torrentHelper = new MyExtendedHelper() {
                    private final MyBasicHelper base = new MyHDFSHelper(selfId);

                    @Override
                    public MyStorageHelper storageHelper() {
                        return base.storageHelper();
                    }

                    @Override
                    public MyManifestHelper manifestHelper() {
                        return base.manifestHelper();
                    }

                    @Override
                    public MyExtendedDetailsHelper extendedDetailsHelper() {
                        return new MyExtendedDetailsHelper() {
                            @Override
                            public void extendedDetails(BasicCompleteCallback complete) {
                                pendingHDFSDownloadCallback.put(req.torrentId, complete);
                                comp.proxy.answer(req, req.success(Result.success(true)));
                                pendingHDFSStartDownloads.remove(req.torrentId);
                            }
                        };
                    }
                };

                BasicCompleteCallback<Boolean> completeCallback = new BasicCompleteCallback<Boolean>() {
                    @Override
                    public void ready(Result<Boolean> result) {
                        if (result.isSuccess()) {
                            HopsTorrentDownloadEvent.AdvanceRequest areq = pendingHDFSAdvanceDownloads.remove(req.torrentId);
                            library.download(req.torrentId, torrentStream);
                            comp.proxy.answer(areq, areq.success(result));
                        } else {
                            throw new RuntimeException("todo - cleanup");
                        }
                    }
                };

                ExtendedTorrent upload = new ExtendedTorrent(endpointTracker, transferTracker, torrentHelper, completeCallback, torrentId, torrentStream, torrentPeers, torrentBuilder);
                pendingHDFSStartDownloads.put(req.torrentId, req);
                upload.start();
            }
        };

        Handler handleHDFSTorrentAdvanceDownload = new Handler<HopsTorrentDownloadEvent.AdvanceRequest>() {
            @Override
            public void handle(HopsTorrentDownloadEvent.AdvanceRequest req) {
                LOG.trace("{}received:{}", logPrefix, req);
                
                pendingHDFSAdvanceDownloads.put(req.torrentId, req);
                BasicCompleteCallback<Pair<KafkaEndpoint, Map<String, KafkaResource>>> advanceCallback = pendingHDFSDownloadCallback.remove(req.torrentId);
                KafkaEndpoint kafkaEndpoint = req.kafkaEndpoint.isPresent() ? req.kafkaEndpoint.get() : null;
                Map<String, KafkaResource> kafkaResources = kafkaEndpoint != null ? req.kafkaDetails : new HashMap<String, KafkaResource>();
                advanceCallback.ready(Result.success(Pair.with(kafkaEndpoint, kafkaResources)));
            }
        };

        private Result<Boolean> validateDownloadRequest(HopsTorrentDownloadEvent.StartRequest req) {
            TorrentState status = library.getStatus(req.torrentId);
            switch (status) {
                case PREPARE_DOWNLOAD:
                case DOWNLOADING:
                    if (compareDownloadDetails()) {
                        return Result.success(true);
                    } else {
                        return Result.badArgument("existing - download details do not match");
                    }
                case PREPARE_UPLOAD:
                case UPLOADING:
                    return Result.badArgument("expected NONE/DOWNLOADING, found:" + status);
                case DESTROYED:
                case NONE:
                    return Result.success(false);
                default:
                    return Result.internalStateFailure("missing logic");
            }
        }

        private boolean compareDownloadDetails() {
            return true;
        }
    }

}
