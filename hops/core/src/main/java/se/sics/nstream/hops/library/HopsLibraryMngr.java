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
import se.sics.kompics.ComponentProxy;
import se.sics.kompics.Handler;
import se.sics.kompics.Negative;
import se.sics.kompics.Positive;
import se.sics.kompics.config.Config;
import se.sics.ktoolbox.util.identifiable.Identifier;
import se.sics.ktoolbox.util.identifiable.overlay.OverlayId;
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
import se.sics.nstream.hops.library.event.core.HopsTorrentDownloadEvent;
import se.sics.nstream.hops.library.event.core.HopsTorrentStopEvent;
import se.sics.nstream.hops.library.event.core.HopsTorrentUploadEvent;
import se.sics.nstream.hops.manifest.DiskHelper;
import se.sics.nstream.hops.manifest.ManifestHelper;
import se.sics.nstream.hops.manifest.ManifestJSON;
import se.sics.nstream.library.event.torrent.HopsContentsEvent;
import se.sics.nstream.library.event.torrent.TorrentExtendedStatusEvent;
import se.sics.nstream.library.util.TorrentState;
import se.sics.nstream.storage.durable.DEndpointControlPort;
import se.sics.nstream.storage.durable.DurableStorageProvider;
import se.sics.nstream.storage.durable.disk.DiskComp;
import se.sics.nstream.storage.durable.disk.DiskEndpoint;
import se.sics.nstream.storage.durable.disk.DiskResource;
import se.sics.nstream.storage.durable.events.DEndpointConnect;
import se.sics.nstream.storage.durable.util.FileExtendedDetails;
import se.sics.nstream.storage.durable.util.MyStream;
import se.sics.nstream.storage.durable.util.StreamEndpoint;
import se.sics.nstream.storage.durable.util.StreamResource;
import se.sics.nstream.torrent.TorrentMngrPort;
import se.sics.nstream.torrent.event.StartDownload;
import se.sics.nstream.torrent.event.StartUpload;
import se.sics.nstream.torrent.tracking.TorrentStatusPort;
import se.sics.nstream.torrent.tracking.event.DownloadSummaryEvent;
import se.sics.nstream.torrent.tracking.event.StatusSummaryEvent;
import se.sics.nstream.torrent.tracking.event.TorrentStatus;
import se.sics.nstream.transfer.MyTorrent;
import se.sics.nstream.util.FileBaseDetails;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class HopsLibraryMngr {

    private static final String TORRENT_LIST = "torrent";
    private static final Details.Types RUN_TYPE = Details.Types.DISK;

    private static final Logger LOG = LoggerFactory.getLogger(HopsLibraryMngr.class);
    private String logPrefix = "";

    private final Config config;
    private final ComponentProxy proxy;
    private final KAddress selfAdr;
    //**************************************************************************
    private final Negative<HopsTorrentPort> torrentPort;
    private final Positive<TorrentMngrPort> torrentMngrPort;
    private final Positive<DEndpointControlPort> endpointControlPort;
    //**********************INTERNAL_DO_NOT_CONNECT_TO**************************
    private final Positive<TorrentStatusPort> torrentStatusPort;
    //**************************************************************************
    private Library library;
    private StorageRegistry storageRegistry;
    //**************************************************************************
    private Map<OverlayId, UploadOp> pendingUploads = new HashMap<>();
    private Map<OverlayId, DownloadOp> pendingDownloads = new HashMap<>();
    private Map<OverlayId, TorrentExtendedStatusEvent.Request> pendingRequests = new HashMap<>();

    public HopsLibraryMngr(ComponentProxy proxy, Config config, String logPrefix, KAddress selfAdr) {
        this.proxy = proxy;
        this.logPrefix = logPrefix;
        this.config = config;
        this.selfAdr = selfAdr;

        library = new Library(TorrentList.readTorrentList(config.getValue("torrent.list.save", String.class)));
        storageRegistry = new StorageRegistry();

        torrentPort = proxy.getPositive(HopsTorrentPort.class).getPair();
        endpointControlPort = proxy.getNegative(DEndpointControlPort.class).getPair();
        torrentMngrPort = proxy.getNegative(TorrentMngrPort.class).getPair();
        torrentStatusPort = proxy.getNegative(TorrentStatusPort.class).getPair();

        proxy.subscribe(handleEndpointConnected, endpointControlPort);

        proxy.subscribe(handleHopsTorrentUpload, torrentPort);
        proxy.subscribe(handleHopsTorrentDownload, torrentPort);
        proxy.subscribe(handleDownload3, torrentPort);
        proxy.subscribe(handleHopsTorrentStop, torrentPort);
        proxy.subscribe(handleContentsRequest, torrentPort);
        proxy.subscribe(handleTorrentStatusRequest, torrentPort);
        proxy.subscribe(handleTorrentStatusResponse, torrentStatusPort);

        proxy.subscribe(handleDownload2, torrentStatusPort);
        proxy.subscribe(handleDownloadCompleted, torrentStatusPort);
    }

    public void start() {
    }

    public void close() {
    }

    public void cleanAndDestroy(OverlayId torrentId) {
        library.destroyed(torrentId);
    }

    //**************************CONNECTING ENDPOINTS****************************
    //the endpointcallbacks are per endpoint, the master callback is for all endpoints. 
    //StorageRegistry will let us know when each of the endpoints is connected and we relay this to the master callback 
    private void registerEndpoints(List<DurableStorageProvider> storageEndpoints, final ConnectedEndpointsCallback callback) {
        for (DurableStorageProvider dsp : storageEndpoints) {
            String endpointName = dsp.getEndpoint().getEndpointName();
            StorageRegistry.Result registrationResult = storageRegistry.preRegister(dsp.getEndpoint());
            if (registrationResult.alreadyRegistered) {
                LOG.info("{}endpoint:{} already registered", logPrefix, endpointName);
                callback.addConnected(endpointName, registrationResult.endpointId, dsp.getEndpoint());
            } else {
                LOG.info("{}endpoint:{} waiting", logPrefix, endpointName);
                callback.addWaiting(endpointName, registrationResult.endpointId, dsp.getEndpoint());
                StorageRegistry.ConnectedCallback endpointCallback = new StorageRegistry.ConnectedCallback() {
                    @Override
                    public void success(Identifier endpointId) {
                        callback.connected(endpointId);
                    }
                };
                storageRegistry.register(registrationResult.endpointId, endpointCallback);
                proxy.trigger(new DEndpointConnect.Request(registrationResult.endpointId, dsp), endpointControlPort);
            }
        }
        callback.done();
    }

    Handler handleEndpointConnected = new Handler<DEndpointConnect.Success>() {
        @Override
        public void handle(DEndpointConnect.Success resp) {
            LOG.info("{}connected endpoint:{}", logPrefix, resp.req.endpointId);
            storageRegistry.connected(resp.req.endpointId);
        }
    };

    private static class ConnectedEndpointsCallback {

        private final Map<String, Identifier> nameToId = new HashMap<>();
        private final Map<Identifier, StreamEndpoint> endpoints = new HashMap<>();
        private final Set<Identifier> connecting = new HashSet<>();
        private final ConnectedEndpointsResult result;

        public ConnectedEndpointsCallback(ConnectedEndpointsResult result) {
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

        public void connected(Identifier endpointId) {
            connecting.remove(endpointId);
            if (connecting.isEmpty()) {
                result.ready();
            }
        }

        public void done() {
            result.setEndpoints(nameToId, endpoints);
            if (connecting.isEmpty()) {
                result.ready();
            }
        }
    }

    private static interface ConnectedEndpointsResult {

        public void setEndpoints(Map<String, Identifier> nameToId, Map<Identifier, StreamEndpoint> endpoints);

        public void ready();
    }

    //**************************************************************************
    private List<DurableStorageProvider> extractEndpoints(HDFSEndpoint hdfsEndpoint) {
        List<DurableStorageProvider> result = new ArrayList<>();
        if (RUN_TYPE.equals(Details.Types.DISK)) {
            result.add(new DiskComp.StorageProvider(selfAdr.getId()));
        } else {
            result.add(new HDFSComp.StorageProvider(selfAdr.getId(), hdfsEndpoint));
        }
        return result;
    }
    //**********************************UPLOAD**********************************
    Handler handleHopsTorrentUpload = new Handler<HopsTorrentUploadEvent.Request>() {
        @Override
        public void handle(HopsTorrentUploadEvent.Request req) {
            LOG.trace("{}received:{}", logPrefix, req);
            Result<Boolean> completed = validateRequest(req);
            if (!completed.isSuccess()) {
                proxy.answer(req, req.failed(completed));
                return;
            }
            if (completed.isSuccess() && completed.getValue()) {
                proxy.answer(req, req.success(completed));
                return;
            }
            Result<ManifestJSON> manifest = getManifest(req);
            if (!manifest.isSuccess()) {
                proxy.answer(req, req.failed(Result.failure(manifest.status, manifest.getException())));
                return;
            }
            List<DurableStorageProvider> endpoints = extractEndpoints(req.hdfsEndpoint);
            UploadOp pendingUpload = new UploadOp(req, manifest.getValue());
            registerEndpoints(endpoints, new ConnectedEndpointsCallback(pendingUpload));
            pendingUploads.put(req.torrentId, pendingUpload);
        }
    };

    /**
     * uses Library, proxy and cleans itself from pendingUploads
     */
    public class UploadOp implements ConnectedEndpointsResult {

        public HopsTorrentUploadEvent.Request req;

        private Map<String, Identifier> nameToId;
        private Map<Identifier, StreamEndpoint> endpoints;

        private Pair<Identifier, StreamEndpoint> mainEndpoint;
        private MyStream manifestStream;
        private MyTorrent.Manifest manifest;
        private ManifestJSON manifestJSON;

        public UploadOp(HopsTorrentUploadEvent.Request req, ManifestJSON manifestJSON) {
            this.req = req;
            this.manifestJSON = manifestJSON;
            this.manifest = MyTorrent.buildDefinition(ManifestHelper.getManifestByte(manifestJSON));
        }

        @Override
        public void setEndpoints(Map<String, Identifier> nameToId, Map<Identifier, StreamEndpoint> endpoints) {
            this.nameToId = nameToId;
            this.endpoints = endpoints;

            Identifier mainEndpointId;
            StreamResource manifestResource;
            if (RUN_TYPE.equals(Details.Types.DISK)) {
                mainEndpointId = nameToId.get(DiskEndpoint.DISK_ENDPOINT_NAME);
                manifestResource = new DiskResource(req.manifestResource.dirPath, req.manifestResource.fileName);
            } else {
                mainEndpointId = nameToId.get(req.hdfsEndpoint.getEndpointName());
                manifestResource = req.manifestResource;
            }
            StreamEndpoint endpoint = endpoints.get(mainEndpointId);
            mainEndpoint = Pair.with(mainEndpointId, endpoint);
            manifestStream = new MyStream(endpoint, manifestResource);
        }

        public Pair<Identifier, StreamEndpoint> getMainEndpoint() {
            return mainEndpoint;
        }

        @Override
        public void ready() {
            StartUpload.Request uploadReq = prepareUpload();
            proxy.trigger(uploadReq, torrentMngrPort);
            //TODO not waiting for success now - should I?
            proxy.answer(req, req.success(Result.success(true)));
            pendingUploads.remove(req.torrentId);
        }

        private StartUpload.Request prepareUpload() {
            Pair<Map<String, FileId>, Map<FileId, FileBaseDetails>> baseDetails = ManifestHelper.getBaseDetails(req.torrentId, manifestJSON, MyTorrent.defaultDataBlock);
            Map<FileId, FileExtendedDetails> extendedDetails = getExtendedDetails(mainEndpoint.getValue0(), mainEndpoint.getValue1(),
                    req.manifestResource.dirPath, baseDetails.getValue0());
            MyTorrent torrent = new MyTorrent(manifest, baseDetails.getValue0(), baseDetails.getValue1(), extendedDetails);

            Library.Torrent libTorrent = new Library.Torrent(endpoints, manifestStream, torrent);
            library.upload(req.torrentId, req.torrentName, libTorrent);

            return new StartUpload.Request(req.torrentId, torrent);
        }

        private Map<FileId, FileExtendedDetails> getExtendedDetails(Identifier endpointId, StreamEndpoint streamEndpoint,
                String dirPath, Map<String, FileId> nameToId) {
            Map<FileId, FileExtendedDetails> extendedDetails = new HashMap<>();
            for (Map.Entry<String, FileId> file : nameToId.entrySet()) {
                StreamResource streamResource;
                if (RUN_TYPE.equals(Details.Types.DISK)) {
                    streamResource = new DiskResource(dirPath, file.getKey());
                } else {
                    streamResource = new DiskResource(dirPath, file.getKey());
                }
                StreamId streamId = TorrentIds.streamId(endpointId, file.getValue());
                MyStream stream = new MyStream(streamEndpoint, streamResource);
                FileExtendedDetails fed = new HopsFED(Pair.with(streamId, stream));
                extendedDetails.put(file.getValue(), fed);
            }
            return extendedDetails;
        }
    }

    private Result<Boolean> validateRequest(HopsTorrentUploadEvent.Request req) {
        TorrentState status = library.getStatus(req.torrentId);
        HopsTorrentUploadEvent.Response resp;
        switch (status) {
            case UPLOADING:
                if (compareUploadDetails()) {
                    return Result.success(true);
                } else {
                    return Result.badArgument("existing - upload details do not match");
                }
            case DOWNLOADING:
            case PRE_DOWNLOAD_1:
            case PRE_DOWNLOAD_2:
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

    private Result<ManifestJSON> getManifest(HopsTorrentUploadEvent.Request req) {
        if (RUN_TYPE.equals(Details.Types.DISK)) {
            DiskResource manifestResource = new DiskResource(req.manifestResource.dirPath, req.manifestResource.fileName);
            return DiskHelper.readManifest(manifestResource);
        } else {
            UserGroupInformation ugi = UserGroupInformation.createRemoteUser(req.hdfsEndpoint.user);
            return HDFSHelper.readManifest(ugi, req.hdfsEndpoint, req.manifestResource);
        }
    }

    //*******************************DOWNLOAD***********************************
    Handler handleHopsTorrentDownload = new Handler<HopsTorrentDownloadEvent.StartRequest>() {
        @Override
        public void handle(HopsTorrentDownloadEvent.StartRequest req) {
            LOG.trace("{}received:{}", logPrefix, req);
            Result<Boolean> completed = validateRequest(req);
            if (!completed.isSuccess()) {
                proxy.answer(req, req.failed(Result.failure(completed.status, completed.getException())));
                return;
            }
            if (completed.isSuccess() && completed.getValue()) {
                proxy.answer(req, req.success(completed));
                return;
            }
            List<DurableStorageProvider> endpoints = extractEndpoints(req.hdfsEndpoint);
            DownloadOp pendingDownload = new DownloadOp(req);
            registerEndpoints(endpoints, new ConnectedEndpointsCallback(pendingDownload));
            pendingDownloads.put(req.torrentId, pendingDownload);
        }
    };

    /**
     * uses Library, proxy and cleans itself from pendingDownloads
     */
    public class DownloadOp implements ConnectedEndpointsResult {

        private HopsTorrentDownloadEvent.StartRequest startReq;
        private TorrentStatus.DownloadedManifest pendingAdvance;
        private HopsTorrentDownloadEvent.AdvanceRequest advanceReq;

        private Map<String, Identifier> nameToId;
        private Map<Identifier, StreamEndpoint> endpoints;

        private Pair<Identifier, StreamEndpoint> mainEndpoint;
        private MyStream manifestStream;
        private MyTorrent.Manifest manifest;
        private ManifestJSON manifestJSON;

        public DownloadOp(HopsTorrentDownloadEvent.StartRequest req) {
            this.startReq = req;
            //TODO simplify library since we now maintain data in the ops
            library.download1(req.torrentId, req.torrentName);
        }

        @Override
        public void setEndpoints(Map<String, Identifier> nameToId, Map<Identifier, StreamEndpoint> endpoints) {
            this.nameToId = nameToId;
            this.endpoints = endpoints;

            Identifier mainEndpointId;
            StreamResource manifestResource;
            if (RUN_TYPE.equals(Details.Types.DISK)) {
                mainEndpointId = nameToId.get(DiskEndpoint.DISK_ENDPOINT_NAME);
                manifestResource = new DiskResource(startReq.manifest.dirPath, startReq.manifest.fileName);
            } else {
                mainEndpointId = nameToId.get(startReq.hdfsEndpoint.getEndpointName());
                manifestResource = startReq.manifest;
            }
            StreamEndpoint endpoint = endpoints.get(mainEndpointId);
            mainEndpoint = Pair.with(mainEndpointId, endpoint);
            manifestStream = new MyStream(endpoint, manifestResource);
        }

        public Pair<Identifier, StreamEndpoint> getMainEndpoint() {
            return mainEndpoint;
        }

        public StreamResource getManifestResource() {
            return manifestStream.resource;
        }

        @Override
        public void ready() {
            StartDownload.Request downloadReq = new StartDownload.Request(startReq.torrentId, startReq.partners);
            proxy.trigger(downloadReq, torrentMngrPort);
            //TODO response when things are set up
        }

        public void downloadedManifest(TorrentStatus.DownloadedManifest pendingAdvance, MyTorrent.Manifest manifest, ManifestJSON manifestJSON) {
            this.pendingAdvance = pendingAdvance;
            this.manifest = manifest;
            this.manifestJSON = manifestJSON;
            library.download2(pendingAdvance.torrentId);
            proxy.answer(startReq, startReq.success(Result.success(true)));
        }

        public void advanceReq(HopsTorrentDownloadEvent.AdvanceRequest advanceReq) {
            this.advanceReq = advanceReq;
            Pair<Map<String, FileId>, Map<FileId, FileBaseDetails>> baseDetails = ManifestHelper.getBaseDetails(startReq.torrentId, manifestJSON, MyTorrent.defaultDataBlock);
            Map<FileId, FileExtendedDetails> extendedDetails = getExtendedDetails(mainEndpoint.getValue0(), mainEndpoint.getValue1(), startReq.manifest.dirPath, baseDetails.getValue0());
            MyTorrent torrent = new MyTorrent(manifest, baseDetails.getValue0(), baseDetails.getValue1(), extendedDetails);
            proxy.answer(pendingAdvance, pendingAdvance.success(torrent));
            library.download3(startReq.torrentId, new Library.Torrent(endpoints, manifestStream, torrent));

            //TODO not waiting for success now - should I?
            proxy.answer(advanceReq, advanceReq.success(Result.success(true)));
            pendingDownloads.remove(startReq.torrentId);
        }

        public void fail(Result result) {
            if (advanceReq != null) {
                proxy.answer(advanceReq, advanceReq.fail(result));
                pendingDownloads.remove(startReq.torrentId);
            }
        }

        private Map<FileId, FileExtendedDetails> getExtendedDetails(Identifier endpointId, StreamEndpoint streamEndpoint,
                String dirPath, Map<String, FileId> nameToId) {
            Map<FileId, FileExtendedDetails> extendedDetails = new HashMap<>();
            for (Map.Entry<String, FileId> file : nameToId.entrySet()) {
                StreamResource streamResource;
                if (RUN_TYPE.equals(Details.Types.DISK)) {
                    streamResource = new DiskResource(dirPath, file.getKey());
                } else {
                    streamResource = new DiskResource(dirPath, file.getKey());
                }
                StreamId streamId = TorrentIds.streamId(endpointId, file.getValue());
                MyStream stream = new MyStream(streamEndpoint, streamResource);
                FileExtendedDetails fed = new HopsFED(Pair.with(streamId, stream));
                extendedDetails.put(file.getValue(), fed);
            }
            return extendedDetails;
        }
    }

    private Result<Boolean> validateRequest(HopsTorrentDownloadEvent.StartRequest req) {
        TorrentState status = library.getStatus(req.torrentId);
        HopsTorrentUploadEvent.Response resp;
        switch (status) {
            case UPLOADING:
                return Result.badArgument("expected NONE/UPLOADING, found:" + status);
            case DOWNLOADING:
            case PRE_DOWNLOAD_1:
            case PRE_DOWNLOAD_2:
                if (compareDownloadDetails()) {
                    return Result.success(true);
                } else {
                    return Result.badArgument("existing - download details do not match");
                }
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

    Handler handleDownload2 = new Handler<TorrentStatus.DownloadedManifest>() {
        @Override
        public void handle(TorrentStatus.DownloadedManifest req) {
            LOG.trace("{}received:{}", logPrefix, req);
            DownloadOp op = pendingDownloads.get(req.torrentId);

            if (req.manifest.isSuccess()) {
                LOG.info("{}download:{} got manifest", logPrefix, req.torrentId);
                ManifestJSON manifestJSON = ManifestHelper.getManifestJSON(req.manifest.getValue().manifestByte);
                Pair<Identifier, StreamEndpoint> mainEndpoint = op.getMainEndpoint();
                Result<Boolean> writeManifestResult = writeManifest(mainEndpoint.getValue1(), op.getManifestResource(), manifestJSON);

                if (writeManifestResult.isSuccess()) {
                    op.downloadedManifest(req, req.manifest.getValue(), manifestJSON);
                } else {
                    cleanAndDestroy(req.torrentId);
                    op.fail(writeManifestResult);
                }
            } else {
                cleanAndDestroy(req.torrentId);
                op.fail(req.manifest);
            }
        }
    };

    private Result<Boolean> writeManifest(StreamEndpoint endpoint, StreamResource manifestResource, ManifestJSON manifest) {
        if (RUN_TYPE.equals(Details.Types.DISK)) {
            DiskResource diskManifestResource = (DiskResource) manifestResource;
            return DiskHelper.writeManifest(diskManifestResource, manifest);
        } else {
            HDFSEndpoint hdfsEndpoint = (HDFSEndpoint) endpoint;
            String user = hdfsEndpoint.user;
            HDFSResource hdfsManifestResource = (HDFSResource) manifestResource;
            UserGroupInformation ugi = UserGroupInformation.createRemoteUser(user);
            return HDFSHelper.writeManifest(ugi, hdfsEndpoint, hdfsManifestResource, manifest);
        }
    }

    Handler handleDownload3 = new Handler<HopsTorrentDownloadEvent.AdvanceRequest>() {
        @Override
        public void handle(HopsTorrentDownloadEvent.AdvanceRequest req) {
            LOG.trace("{}received:{}", logPrefix, req);
            DownloadOp op = pendingDownloads.get(req.torrentId);

            if (req.result.isSuccess()) {
                op.advanceReq(req);
            } else {
                cleanAndDestroy(req.torrentId);
            }
        }
    };
    //**************************************************************************

    Handler handleHopsTorrentStop = new Handler<HopsTorrentStopEvent.Request>() {
        @Override
        public void handle(HopsTorrentStopEvent.Request req) {
            LOG.trace("{}received:{}", logPrefix, req);
            cleanAndDestroy(req.torrentId);
            proxy.answer(req, req.success());
        }
    };

    Handler handleContentsRequest = new Handler<HopsContentsEvent.Request>() {
        @Override
        public void handle(HopsContentsEvent.Request req) {
            LOG.info("{}received:{}", logPrefix, req);
            List<ElementSummary> summary = library.getSummary();
            HopsContentsEvent.Response resp = req.success(summary);
            LOG.info("{}answering:{}", logPrefix, resp);
            proxy.answer(req, resp);
        }
    };

    Handler handleDownloadCompleted = new Handler<DownloadSummaryEvent>() {
        @Override
        public void handle(DownloadSummaryEvent event) {
            LOG.trace("{}received:{}", logPrefix, event);
            library.finishDownload(event.torrentId);
        }
    };

    Handler handleTorrentStatusRequest = new Handler<TorrentExtendedStatusEvent.Request>() {
        @Override
        public void handle(TorrentExtendedStatusEvent.Request req) {
            LOG.trace("{}received:{}", logPrefix, req);
            pendingRequests.put(req.torrentId, req);
            proxy.trigger(new StatusSummaryEvent.Request(req.eventId, req.torrentId), torrentStatusPort);
        }
    };
    Handler handleTorrentStatusResponse = new Handler<StatusSummaryEvent.Response>() {
        @Override
        public void handle(StatusSummaryEvent.Response resp) {
            LOG.trace("{}received:{}", logPrefix, resp);
            TorrentExtendedStatusEvent.Request req = pendingRequests.remove(resp.req.torrentId);
            proxy.answer(req, req.succes(resp.result));
        }
    };
}
