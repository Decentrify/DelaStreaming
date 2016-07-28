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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.gvod.mngr.util.ElementSummary;
import se.sics.gvod.stream.report.event.DownloadSummaryEvent;
import se.sics.gvod.stream.report.event.StatusSummaryEvent;
import se.sics.kompics.Component;
import se.sics.kompics.ComponentProxy;
import se.sics.kompics.Handler;
import se.sics.kompics.Negative;
import se.sics.kompics.config.Config;
import se.sics.ktoolbox.util.identifiable.Identifier;
import se.sics.ktoolbox.util.network.KAddress;
import se.sics.ktoolbox.util.result.Result;
import se.sics.nstream.hops.hdfs.HDFSEndpoint;
import se.sics.nstream.hops.hdfs.HDFSHelper;
import se.sics.nstream.hops.hdfs.HDFSResource;
import se.sics.nstream.library.event.torrent.ContentsSummaryEvent;
import se.sics.nstream.hops.library.event.core.HopsTorrentDownloadEvent;
import se.sics.nstream.hops.library.event.core.HopsTorrentStopEvent;
import se.sics.nstream.hops.library.event.core.HopsTorrentUploadEvent;
import se.sics.nstream.hops.manifest.ManifestJSON;
import se.sics.nstream.library.LibraryMngrComp;
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
public class HopsTorrentMngr {

    private static final Logger LOG = LoggerFactory.getLogger(LibraryMngrComp.class);
    private String logPrefix = "";

    private final Config config;
    private final ComponentProxy proxy;
    private final KAddress selfAdr;
    //**************************************************************************
    private final CoreExtPorts extPorts;
    private final Negative<HopsTorrentPort> torrentPort;
    private final Negative<TransferMngrPort> transferMngrPort;
//    private final Positive<ReportPort> reportPort;
//    private final One2NChannel reportChannel;
    //**************************************************************************
    private Library library = new Library();
    private Map<Identifier, Component> components = new HashMap<>();
    //**************************************************************************
    private Map<Identifier, HopsTorrentDownloadEvent.StartRequest> pendingExtDwnl = new HashMap<>();
    private Map<Identifier, Transfer.DownloadRequest> pendingIntDwnl = new HashMap<>();
    private Map<Identifier, HopsTorrentUploadEvent.Request> pendingExtUpld = new HashMap<>();

    private Map<Identifier, TorrentExtendedStatusEvent.Request> pendingRequests = new HashMap<>();

    public HopsTorrentMngr(ComponentProxy proxy, Config config, String logPrefix, KAddress selfAdr, CoreExtPorts extPorts) {
        this.proxy = proxy;
        this.logPrefix = logPrefix;
        this.config = config;
        this.extPorts = extPorts;
        this.selfAdr = selfAdr;
        torrentPort = proxy.getPositive(HopsTorrentPort.class).getPair();
        transferMngrPort = proxy.getPositive(TransferMngrPort.class).getPair();
//        reportPort = proxy.getNegative(ReportPort.class).getPair();
//        reportChannel = One2NChannel.getChannel("vodMngrReportChannel", (Negative<ReportPort>) reportPort.getPair(), new EventOverlayIdExtractor());
    }

    public void subscribe() {
        proxy.subscribe(handleHopsTorrentUpload, torrentPort);
        proxy.subscribe(handleHopsTorrentDownload, torrentPort);
        proxy.subscribe(handleHopsTorrentStop, torrentPort);
        proxy.subscribe(handleContentsRequest, torrentPort);
        proxy.subscribe(handleTorrentStatusRequest, torrentPort);
//        proxy.subscribe(handleTorrentStatusResponse, reportPort);
//        proxy.subscribe(handleDownloadCompleted, reportPort);
    }

    public void start() {
    }

    public void close() {
    }

    public void cleanAndDestroy(Identifier torrentId) {
        library.destroyed(torrentId);
        //TODO Alex - destory and clean;
    }

    //**********************************UPLOAD**********************************
    Handler handleHopsTorrentUpload = new Handler<HopsTorrentUploadEvent.Request>() {
        @Override
        public void handle(HopsTorrentUploadEvent.Request req) {
            LOG.trace("{}received:{}", logPrefix, req);
            TorrentStatus status = library.getStatus(req.torrentId);
            HopsTorrentUploadEvent.Response resp;
            switch (status) {
                case UPLOADING:
                    if (compareUploadDetails(library.getManifest(req.torrentId), library.getExtendedDetails(req.torrentId), req.manifest, req.extendedDetails)) {
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
                case NONE:
                    UserGroupInformation ugi = UserGroupInformation.createRemoteUser(req.manifest.getValue0().user);
                    Result<ManifestJSON> manifest = HDFSHelper.readManifest(ugi, req.manifest.getValue0(), req.manifest.getValue1());
                    if (manifest.isSuccess()) {
                        byte[] torrentByte = HDFSHelper.getManifestByte(manifest.getValue());
                        TorrentDetails torrentDetails = ManifestJSON.getTorrentDetails(manifest.getValue());
                        TransferDetails transferDetails = new TransferDetails(torrentDetails, req.extendedDetails);
                        library.upload(req.torrentId, req.manifest, transferDetails);
                        uploadHopsTorrent(req.torrentId, torrentByte, transferDetails);
                        pendingExtUpld.put(req.torrentId, req);
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

    private boolean compareUploadDetails(Pair<HDFSEndpoint, HDFSResource> foundManifest, Map<String, FileExtendedDetails> foundDetails,
            Pair<HDFSEndpoint, HDFSResource> expectedManifest, Map<String, FileExtendedDetails> expectedDetails) {
        //TODO Alex - what needs to match?
        return false;
    }

    private void uploadHopsTorrent(Identifier torrentId, byte[] torrentByte, TransferDetails transferDetails) {
        LOG.info("{}setting up upload:{}", logPrefix, torrentId);

//        StreamHostComp.ExtPort shcExtPorts = new StreamHostComp.ExtPort(extPorts.timerPort, extPorts.networkPort);
//        Component torrentComp = proxy.create(StreamHostComp.class, new StreamHostComp.Init(shcExtPorts, selfAdr, torrentDetails, new ArrayList<KAddress>()));
//        reportChannel.addChannel(torrentId, torrentComp.getPositive(ReportPort.class));
//        components.put(torrentId, torrentComp);
//        proxy.trigger(Start.event, torrentComp.control());
    }

    Handler handleTransferDetailsInd = new Handler<Transfer.UploadIndication>() {
        @Override
        public void handle(Transfer.UploadIndication event) {
            LOG.trace("{}received:{}", logPrefix, event);
            HopsTorrentUploadEvent.Request hopsReq = pendingExtUpld.remove(event.torrentId);
            if (event.result.isSuccess()) {
                if (hopsReq != null) {
                    HopsTorrentUploadEvent.Response hopsResp = hopsReq.uploading(Result.success(true));
                    LOG.trace("{}sending:{}", logPrefix, hopsResp);
                    proxy.answer(hopsReq, hopsResp);
                }
            } else {
                cleanAndDestroy(event.torrentId);
                if (hopsReq != null) {
                    HopsTorrentUploadEvent.Response hopsResp = hopsReq.uploading(event.result);
                    LOG.trace("{}sending:{}", logPrefix, hopsResp);
                    proxy.answer(hopsReq, hopsResp);
                }
            }
        }
    };
    //*******************************DOWNLOAD***********************************
    Handler handleHopsTorrentDownload = new Handler<HopsTorrentDownloadEvent.StartRequest>() {
        @Override
        public void handle(HopsTorrentDownloadEvent.StartRequest req) {
            LOG.trace("{}received:{}", logPrefix, req);
            HopsTorrentDownloadEvent.StartResponse hopsResp;
            TorrentStatus status = library.getStatus(req.torrentId);
            switch (status) {
                case NONE:
                    library.startingDownload(req.torrentId, req.manifest);
                    downloadHopsTorrent(req.torrentId, req.partners);
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
    
    private boolean compareDownloadManifest(Pair<HDFSEndpoint, HDFSResource> foundManifest, Pair<HDFSEndpoint, HDFSResource> expectedManifest) {
        //TODO Alex - what needs to match?
        return false;
    }

    private void downloadHopsTorrent(Identifier torrentId, List<KAddress> partners) {
        LOG.info("{}setting up upload:{}", logPrefix, torrentId);
//        StreamHostComp.ExtPort shcExtPorts = new StreamHostComp.ExtPort(extPorts.timerPort, extPorts.networkPort);
//        Component torrentComp = proxy.create(StreamHostComp.class, new StreamHostComp.Init(shcExtPorts, selfAdr, torrentDetails, partners));
//        reportChannel.addChannel(torrentId, torrentComp.getPositive(ReportPort.class));
//        components.put(torrentId, torrentComp);
//        proxy.trigger(Start.event, torrentComp.control());
    }

    Handler handleTransferDetailsReq = new Handler<Transfer.DownloadRequest>() {
        @Override
        public void handle(Transfer.DownloadRequest req) {
            LOG.trace("{}received:{}", logPrefix, req);
            HopsTorrentDownloadEvent.StartRequest hopsReq = pendingExtDwnl.remove(req.torrentId);
            if (req.torrentByte.isSuccess()) {
                ManifestJSON manifest = HDFSHelper.getManifestJSON(req.torrentByte.getValue());
                Pair<HDFSEndpoint, HDFSResource> manifestResource = library.getManifest(req.torrentId);
                UserGroupInformation ugi = UserGroupInformation.createRemoteUser(manifestResource.getValue0().user);
                Result<Boolean> manifestResult = HDFSHelper.writeManifest(ugi, manifestResource.getValue0(), manifestResource.getValue1(), manifest);
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
                    if(hopsReq != null) {
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
                    Transfer.DownloadResponse nstreamResp = nstreamReq.answer(transferDetails);
                    LOG.trace("{}sending:{}", logPrefix, nstreamResp);
                    proxy.answer(nstreamReq, nstreamResp);
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

    Handler handleContentsRequest = new Handler<ContentsSummaryEvent.Request>() {
        @Override
        public void handle(ContentsSummaryEvent.Request req) {
            LOG.info("{}received:{}", logPrefix, req);
            List<ElementSummary> summary = library.getSummary();
            ContentsSummaryEvent.Response resp = req.success(summary);
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
        }
    };

    Handler handleTorrentStatusResponse = new Handler<StatusSummaryEvent.Response>() {
        @Override
        public void handle(StatusSummaryEvent.Response resp) {
            LOG.trace("{}received:{}", logPrefix, resp);
//            TorrentExtendedStatusEvent.Request req = pendingRequests.remove(resp.getId());
//            proxy.answer(req, req.succes(resp.value));
        }
    };

}
