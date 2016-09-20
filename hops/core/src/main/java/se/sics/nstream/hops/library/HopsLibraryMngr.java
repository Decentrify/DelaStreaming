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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.security.UserGroupInformation;
import org.javatuples.Pair;
import org.javatuples.Triplet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.gvod.mngr.util.ElementSummary;
import se.sics.kompics.ComponentProxy;
import se.sics.kompics.Handler;
import se.sics.kompics.Negative;
import se.sics.kompics.Positive;
import se.sics.kompics.config.Config;
import se.sics.ktoolbox.util.identifiable.Identifier;
import se.sics.ktoolbox.util.identifiable.basic.UUIDIdentifier;
import se.sics.ktoolbox.util.network.KAddress;
import se.sics.ktoolbox.util.result.Result;
import se.sics.nstream.hops.HopsFED;
import se.sics.nstream.hops.hdfs.HDFSEndpoint;
import se.sics.nstream.hops.hdfs.HDFSHelper;
import se.sics.nstream.hops.hdfs.HDFSResource;
import se.sics.nstream.hops.library.event.core.HopsTorrentDownloadEvent;
import se.sics.nstream.hops.library.event.core.HopsTorrentStopEvent;
import se.sics.nstream.hops.library.event.core.HopsTorrentUploadEvent;
import se.sics.nstream.hops.manifest.ManifestJSON;
import se.sics.nstream.library.LibraryMngrComp;
import se.sics.nstream.library.event.torrent.HopsContentsEvent;
import se.sics.nstream.library.event.torrent.TorrentExtendedStatusEvent;
import se.sics.nstream.library.util.TorrentStatus;
import se.sics.nstream.report.ReportPort;
import se.sics.nstream.report.event.DownloadSummaryEvent;
import se.sics.nstream.report.event.StatusSummaryEvent;
import se.sics.nstream.transfer.MyTorrent;
import se.sics.nstream.transfer.Transfer;
import se.sics.nstream.transfer.TransferMngrPort;
import se.sics.nstream.util.CoreExtPorts;
import se.sics.nstream.util.FileBaseDetails;
import se.sics.nstream.util.FileExtendedDetails;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class HopsLibraryMngr {

    private static final Logger LOG = LoggerFactory.getLogger(LibraryMngrComp.class);
    private String logPrefix = "";

    private final Config config;
    private final ComponentProxy proxy;
    private final KAddress selfAdr;
    //**************************************************************************
    private final CoreExtPorts extPorts;
    private final Negative<HopsTorrentPort> torrentPort;
    //**********************INTERNAL_DO_NOT_CONNECT_TO**************************
    private final Negative<TransferMngrPort> transferMngrPort;
    private final Positive<ReportPort> reportPort;
    //**************************************************************************
    private Library library = new Library();
    private HopsTorrentCompMngr components;
    //**************************************************************************
    private Map<Identifier, HopsTorrentDownloadEvent.StartRequest> pendingExtDwnl = new HashMap<>();
    private Map<Identifier, Transfer.DownloadRequest> pendingIntDwnl = new HashMap<>();
    private Map<Identifier, TorrentExtendedStatusEvent.Request> pendingRequests = new HashMap<>();

    public HopsLibraryMngr(ComponentProxy proxy, Config config, String logPrefix, KAddress selfAdr, CoreExtPorts extPorts) {
        this.proxy = proxy;
        this.logPrefix = logPrefix;
        this.config = config;
        this.extPorts = extPorts;
        this.selfAdr = selfAdr;
        components = new HopsTorrentCompMngr(selfAdr, proxy, extPorts, logPrefix);
        torrentPort = proxy.getPositive(HopsTorrentPort.class).getPair();
        transferMngrPort = proxy.getPositive(TransferMngrPort.class).getPair();
        reportPort = proxy.getNegative(ReportPort.class).getPair();

        proxy.subscribe(handleHopsTorrentUpload, torrentPort);
        proxy.subscribe(handleHopsTorrentDownload, torrentPort);
        proxy.subscribe(handleTransferDetailsReq, transferMngrPort);
        proxy.subscribe(handleTransferDetailsResp, torrentPort);
        proxy.subscribe(handleHopsTorrentStop, torrentPort);
        proxy.subscribe(handleContentsRequest, torrentPort);
        proxy.subscribe(handleTorrentStatusRequest, torrentPort);
        proxy.subscribe(handleTorrentStatusResponse, reportPort);
        proxy.subscribe(handleDownloadCompleted, reportPort);
    }

    public void start() {
    }

    public void close() {
    }

    public void cleanAndDestroy(Identifier torrentId) {
        components.destroy(torrentId);
        library.destroyed(torrentId);
    }
    //**************************************************************************

    //**********************************UPLOAD**********************************
    Handler handleHopsTorrentUpload = new Handler<HopsTorrentUploadEvent.Request>() {
        @Override
        public void handle(HopsTorrentUploadEvent.Request req) {
            LOG.trace("{}received:{}", logPrefix, req);
            TorrentStatus status = library.getStatus(req.torrentId);
            HopsTorrentUploadEvent.Response resp;
            switch (status) {
                case UPLOADING:
                    if (compareUploadDetails(library.getTorrent(req.torrentId), Pair.with(req.hdfsEndpoint, req.manifestResource))) {
                        resp = req.alreadyExists(Result.success(true));
                    } else {
                        resp = req.alreadyExists(Result.badArgument("existing - upload details do not match"));
                    }
                    LOG.trace("{}answering:{}", logPrefix, resp);
                    proxy.answer(req, resp);
                    break;
                case DOWNLOADING:
                case DOWNLOAD_1:
                case DOWNLOAD_2:
                case DOWNLOAD_3:
                    resp = req.alreadyExists(Result.badArgument("expected NONE/UPLOADING, found:" + status));
                    LOG.trace("{}answering:{}", logPrefix, resp);
                    proxy.answer(req, resp);
                    break;
                case DESTROYED:
                case NONE:
                    UserGroupInformation ugi = UserGroupInformation.createRemoteUser(req.hdfsEndpoint.user);
                    Result<ManifestJSON> manifest = HDFSHelper.readManifest(ugi, req.hdfsEndpoint, req.manifestResource);
                    if (manifest.isSuccess()) {
                        byte[] torrentByte = HDFSHelper.getManifestByte(manifest.getValue());
                        Map<String, FileBaseDetails> baseDetails = ManifestJSON.getBaseDetails(manifest.getValue(), MyTorrent.defaultDataBlock);
                        Map<String, FileExtendedDetails> extendedDetails = getUploadExtendedDetails(req.hdfsEndpoint, req.manifestResource, baseDetails);
                        MyTorrent.Manifest torrentManifest = MyTorrent.buildDefinition(torrentByte);
                        MyTorrent torrent = new MyTorrent(torrentManifest, baseDetails, extendedDetails);
                        Library.Torrent libTorrent = new Library.Torrent(req.hdfsEndpoint, req.manifestResource, torrent);
                        library.upload(req.torrentId, req.torrentName, libTorrent);
                        components.startUpload(transferMngrPort.getPair(), req.torrentId, req.hdfsEndpoint, torrent);

                        HopsTorrentUploadEvent.Response hopsResp = req.uploading(Result.success(true));
                        LOG.trace("{}sending:{}", logPrefix, hopsResp);
                        proxy.answer(req, hopsResp);
                    } else {
                        resp = req.uploading(Result.failure(manifest.status, manifest.getException()));
                        LOG.trace("{}answering:{}", logPrefix, resp);
                        proxy.answer(req, resp);
                    }
                    break;
                default:
                    resp = req.alreadyExists(Result.internalStateFailure("missing logic"));
                    LOG.trace("{}answering:{}", logPrefix, resp);
                    proxy.answer(req, resp);
            }

        }
    };

    private Map<String, FileExtendedDetails> getUploadExtendedDetails(HDFSEndpoint hdfsEndpoint, HDFSResource manifestResource, Map<String, FileBaseDetails> baseDetails) {
        Map<String, FileExtendedDetails> extendedDetails = new HashMap<>();
        for (String fileName : baseDetails.keySet()) {
            Identifier randomlyAssignedResourceId = UUIDIdentifier.randomId();
            FileExtendedDetails fed = new HopsFED(Pair.with(hdfsEndpoint, new HDFSResource(manifestResource.dirPath, fileName, randomlyAssignedResourceId)));
            extendedDetails.put(fileName, fed);
        }
        return extendedDetails;
    }

    private boolean compareUploadDetails(Pair<String, Library.Torrent> foundTorrent, Pair<HDFSEndpoint, HDFSResource> expectedManifest) {
        //TODO Alex - what needs to match?
        return false;
    }

    //*******************************DOWNLOAD***********************************
    Handler handleHopsTorrentDownload = new Handler<HopsTorrentDownloadEvent.StartRequest>() {
        @Override
        public void handle(HopsTorrentDownloadEvent.StartRequest req) {
            LOG.trace("{}received:{}", logPrefix, req);
            HopsTorrentDownloadEvent.StartResponse hopsResp;
            TorrentStatus status = library.getStatus(req.torrentId);
            switch (status) {
                case DESTROYED:
                case NONE:
                    Library.TorrentBuilder libTorrentBuilder = new Library.TorrentBuilder(req.hdfsEndpoint, req.manifest);
                    library.download1(req.torrentId, req.torrentName, libTorrentBuilder);
                    components.startDownload(transferMngrPort.getPair(), req.torrentId, req.partners);
                    pendingExtDwnl.put(req.torrentId, req);
                    break;
                case UPLOADING:
                case DOWNLOADING:
                case DOWNLOAD_1:
                case DOWNLOAD_2:
                case DOWNLOAD_3:
                    //TODO Alex - check manifests
//                    hopsResp = req.alreadyExists(Result.success(library.getTorrent(req.torrentId)));
//                    LOG.trace("{}answering:{}", logPrefix, hopsResp);
                    hopsResp = req.alreadyExists(Result.internalStateFailure("missing logic"));
                    proxy.answer(req, hopsResp);
                    break;
                //TODO Alex - check manifests
//                    Library.TorrentBuilder torrentDetails = library.getTorrentBuilder(req.torrentId);
//                    if (torrentDetails.isPresent()) {
//                        hopsResp = req.starting(Result.success(true));
//                        LOG.trace("{}answering:{}", logPrefix, hopsResp);
//                        proxy.answer(req, hopsResp);
//                    } else {
//                        pendingExtDwnl.put(req.torrentId, req);
//                    }
                default:
                    hopsResp = req.alreadyExists(Result.internalStateFailure("missing logic"));
                    LOG.trace("{}answering:{}", logPrefix, hopsResp);
                    proxy.answer(req, hopsResp);
            }
        }
    };

    private boolean compareDownloadManifest(Triplet<String, HDFSEndpoint, HDFSResource> foundManifest, Pair<HDFSEndpoint, HDFSResource> expectedManifest) {
        //TODO Alex - what needs to match?
        return false;
    }

    Handler handleTransferDetailsReq = new Handler<Transfer.DownloadRequest>() {
        @Override
        public void handle(Transfer.DownloadRequest req) {
            LOG.trace("{}received:{}", logPrefix, req);
            HopsTorrentDownloadEvent.StartRequest hopsReq = pendingExtDwnl.remove(req.torrentId);
            if (req.manifest.isSuccess()) {
                ManifestJSON manifest = HDFSHelper.getManifestJSON(req.manifest.getValue().manifestByte);
                Map<String, FileBaseDetails> baseDetails = ManifestJSON.getBaseDetails(manifest, MyTorrent.defaultDataBlock);
                Pair<String, Library.TorrentBuilder> torrentBuilder = library.getTorrentBuilder(req.torrentId);
                String user = torrentBuilder.getValue1().hdfsEndpoint.user;
                HDFSEndpoint hdfsEndpoint = torrentBuilder.getValue1().hdfsEndpoint;
                HDFSResource manifestResource = torrentBuilder.getValue1().manifestResource;
                UserGroupInformation ugi = UserGroupInformation.createRemoteUser(user);
                Result<Boolean> manifestResult = HDFSHelper.writeManifest(ugi, hdfsEndpoint, manifestResource, manifest);
                if (manifestResult.isSuccess()) {
                    pendingIntDwnl.put(req.torrentId, req);
                    library.download2(req.torrentId, req.manifest.getValue(), baseDetails);
                    if (hopsReq != null) {
                        HopsTorrentDownloadEvent.StartResponse hopsResp = hopsReq.starting(Result.success(true));
                        LOG.trace("{}sending:{}", logPrefix, hopsResp);
                        proxy.answer(hopsReq, hopsResp);
                    }
                } else {
                    cleanAndDestroy(req.torrentId);
                    if (hopsReq != null) {
                        Result<Boolean> r = Result.failure(manifestResult.status, manifestResult.getException());
                        HopsTorrentDownloadEvent.StartResponse hopsResp = hopsReq.starting(r);
                        LOG.trace("{}sending:{}", logPrefix, hopsResp);
                        proxy.answer(hopsReq, hopsResp);
                    }
                }
            } else {
                cleanAndDestroy(req.torrentId);
                if (hopsReq != null) {
                    Result<Boolean> failure = Result.failure(req.manifest.status, req.manifest.getException());
                    HopsTorrentDownloadEvent.StartResponse hopsResp = hopsReq.starting(failure);
                    LOG.trace("{}sending:{}", logPrefix, hopsResp);
                    proxy.answer(hopsReq, hopsResp);
                }
            }
        }
    };

    Handler handleTransferDetailsResp = new Handler<HopsTorrentDownloadEvent.AdvanceRequest>() {
        @Override
        public void handle(HopsTorrentDownloadEvent.AdvanceRequest req) {
            LOG.trace("{}received:{}", logPrefix, req);
            Transfer.DownloadRequest nstreamReq = pendingIntDwnl.remove(req.torrentId);
            if (req.extendedDetails.isSuccess()) {
                MyTorrent torrent = library.download(req.torrentId, req.extendedDetails.getValue());
                if (nstreamReq != null) {
                    components.advanceDownload(req.torrentId, req.hdfsEndpoint, req.kafkaEndpoint);
                    Transfer.DownloadResponse nstreamResp = nstreamReq.answer(torrent);
                    LOG.trace("{}sending:{}", logPrefix, nstreamResp);
                    proxy.answer(nstreamReq, nstreamResp);
                    proxy.answer(req, req.answer(Result.success(true)));
                } else {
                    throw new RuntimeException("unexpected");
                }
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
            pendingRequests.put(req.eventId, req);
            proxy.trigger(new StatusSummaryEvent.Request(req.eventId, req.torrentId), reportPort);
        }
    };
    Handler handleTorrentStatusResponse = new Handler<StatusSummaryEvent.Response>() {
        @Override
        public void handle(StatusSummaryEvent.Response resp) {
            LOG.trace("{}received:{}", logPrefix, resp);
            TorrentExtendedStatusEvent.Request req = pendingRequests.remove(resp.getId());
            proxy.answer(req, req.succes(resp.result));
        }
    };
}
