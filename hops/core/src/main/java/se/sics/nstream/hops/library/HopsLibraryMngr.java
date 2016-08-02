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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.security.UserGroupInformation;
import org.javatuples.Pair;
import org.javatuples.Triplet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.gvod.mngr.util.ElementSummary;
import se.sics.gvod.stream.report.event.DownloadSummaryEvent;
import se.sics.kompics.ComponentProxy;
import se.sics.kompics.Handler;
import se.sics.kompics.Negative;
import se.sics.kompics.config.Config;
import se.sics.ktoolbox.util.identifiable.Identifier;
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
import se.sics.nstream.transfer.Transfer;
import se.sics.nstream.transfer.TransferMngrPort;
import se.sics.nstream.util.CoreExtPorts;
import se.sics.nstream.util.FileExtendedDetails;
import se.sics.nstream.util.TorrentDetails;
import se.sics.nstream.util.TransferDetails;

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
//    private final Positive<ReportPort> reportPort;
//    private final One2NChannel reportChannel;
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
//        reportPort = proxy.getNegative(ReportPort.class).getPair();
//        reportChannel = One2NChannel.getChannel("vodMngrReportChannel", (Negative<ReportPort>) reportPort.getPair(), new EventOverlayIdExtractor());

        proxy.subscribe(handleHopsTorrentUpload, torrentPort);
        proxy.subscribe(handleHopsTorrentDownload, torrentPort);
        proxy.subscribe(handleTransferDetailsReq, transferMngrPort);
        proxy.subscribe(handleTransferDetailsResp, torrentPort);
        proxy.subscribe(handleHopsTorrentStop, torrentPort);
        proxy.subscribe(handleContentsRequest, torrentPort);
//        proxy.subscribe(handleTorrentStatusResponse, reportPort);
//        proxy.subscribe(handleDownloadCompleted, reportPort);
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
                    if (compareUploadDetails(library.getManifest(req.torrentId), Pair.with(req.hdfsEndpoint, req.manifestResource))) {
                        resp = req.alreadyExists(Result.success(true));
                    } else {
                        resp = req.alreadyExists(Result.badArgument("existing - upload details do not match"));
                    }
                    LOG.trace("{}answering:{}", logPrefix, resp);
                    proxy.answer(req, resp);
                    break;
                case PENDING_DOWNLOAD:
                case DOWNLOADING:
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
                        TorrentDetails torrentDetails = ManifestJSON.getTorrentDetails(manifest.getValue());
                        Map<String, FileExtendedDetails> extendedDetails = getUploadExtendedDetails(req.hdfsEndpoint, req.manifestResource, torrentDetails);
                        TransferDetails transferDetails = new TransferDetails(torrentDetails, extendedDetails);
                        library.upload(req.torrentId, req.torrentName, req.hdfsEndpoint, req.manifestResource, transferDetails);
                        components.startUpload(transferMngrPort.getPair(), req.torrentId, req.hdfsEndpoint, Pair.with(torrentByte, transferDetails));

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

    private Map<String, FileExtendedDetails> getUploadExtendedDetails(HDFSEndpoint hdfsEndpoint, HDFSResource manifestResource, TorrentDetails torrentDetails) {
        Map<String, FileExtendedDetails> extendedDetails = new HashMap<>();
        for (String fileName : torrentDetails.baseDetails.keySet()) {
            FileExtendedDetails fed = new HopsFED(Pair.with(hdfsEndpoint, new HDFSResource(manifestResource.dirPath, fileName)));
            extendedDetails.put(fileName, fed);
        }
        return extendedDetails;
    }

    private boolean compareUploadDetails(Triplet<String, HDFSEndpoint, HDFSResource> foundManifest, Pair<HDFSEndpoint, HDFSResource> expectedManifest) {
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
                    library.startingDownload(req.torrentId, req.torrentName, req.hdfsEndpoint, req.manifest);
                    components.startDownload(transferMngrPort.getPair(), req.torrentId, req.partners);
                    pendingExtDwnl.put(req.torrentId, req);
                    break;
                case UPLOADING:
                case DOWNLOADING:
                    //TODO Alex - check manifests
                    hopsResp = req.alreadyExists(Result.success(Pair.with(library.getManifest(req.torrentId), library.getExtendedDetails(req.torrentId))));
                    LOG.trace("{}answering:{}", logPrefix, hopsResp);
                    proxy.answer(req, hopsResp);
                    break;
                case PENDING_DOWNLOAD:
                    //TODO Alex - check manifests
                    Optional<TorrentDetails> torrentDetails = library.getTorrentDetails(req.torrentId);
                    if (torrentDetails.isPresent()) {
                        hopsResp = req.starting(Result.success(true));
                        LOG.trace("{}answering:{}", logPrefix, hopsResp);
                        proxy.answer(req, hopsResp);
                    } else {
                        pendingExtDwnl.put(req.torrentId, req);
                    }
                    break;
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
            if (req.torrentByte.isSuccess()) {
                ManifestJSON manifest = HDFSHelper.getManifestJSON(req.torrentByte.getValue());
                Triplet<String, HDFSEndpoint, HDFSResource> manifestResource = library.getManifest(req.torrentId);
                UserGroupInformation ugi = UserGroupInformation.createRemoteUser(manifestResource.getValue1().user);
                Result<Boolean> manifestResult = HDFSHelper.writeManifest(ugi, manifestResource.getValue1(), manifestResource.getValue2(), manifest);
                if (manifestResult.isSuccess()) {
                    TorrentDetails torrentDetails = ManifestJSON.getTorrentDetails(manifest);
                    pendingIntDwnl.put(req.torrentId, req);
                    library.pendingDownload(req.torrentId, torrentDetails);
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
                    Result<Boolean> failure = Result.failure(req.torrentByte.status, req.torrentByte.getException());
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
                TransferDetails transferDetails = library.download(req.torrentId, req.extendedDetails.getValue());
                if (nstreamReq != null) {
                    components.advanceDownload(req.torrentId, req.hdfsEndpoint, req.kafkaEndpoint);
                    Transfer.DownloadResponse nstreamResp = nstreamReq.answer(transferDetails);
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

//    Handler handleTorrentStatusRequest = new Handler<TorrentExtendedStatusEvent.Request>() {
//        @Override
//        public void handle(TorrentExtendedStatusEvent.Request req) {
//            LOG.trace("{}received:{}", logPrefix, req);
//        }
//    };
//    Handler handleTorrentStatusResponse = new Handler<StatusSummaryEvent.Response>() {
//        @Override
//        public void handle(StatusSummaryEvent.Response resp) {
//            LOG.trace("{}received:{}", logPrefix, resp);
//            TorrentExtendedStatusEvent.Request req = pendingRequests.remove(resp.getId());
//            proxy.answer(req, req.succes(resp.value));
//        }
//    };
}
