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
package se.sics.gvod.stream.mngr.hops;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.javatuples.Pair;
import org.javatuples.Triplet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.gvod.core.util.TorrentDetails;
import se.sics.gvod.mngr.util.ElementSummary;
import se.sics.gvod.mngr.util.FileInfo;
import se.sics.gvod.mngr.util.TorrentInfo;
import se.sics.gvod.stream.StreamHostComp;
import se.sics.gvod.stream.mngr.VoDMngrComp;
import se.sics.gvod.stream.mngr.event.ContentsSummaryEvent;
import se.sics.gvod.stream.mngr.event.TorrentExtendedStatusEvent;
import se.sics.gvod.stream.mngr.hops.torrent.event.HopsTorrentDownloadEvent;
import se.sics.gvod.stream.mngr.hops.torrent.event.HopsTorrentStopEvent;
import se.sics.gvod.stream.mngr.hops.torrent.event.HopsTorrentUploadEvent;
import se.sics.gvod.stream.report.ReportPort;
import se.sics.gvod.stream.report.event.DownloadSummaryEvent;
import se.sics.gvod.stream.report.event.StatusSummaryEvent;
import se.sics.gvod.stream.report.util.TorrentStatus;
import se.sics.kompics.Component;
import se.sics.kompics.ComponentProxy;
import se.sics.kompics.Handler;
import se.sics.kompics.Kill;
import se.sics.kompics.Negative;
import se.sics.kompics.Positive;
import se.sics.kompics.Start;
import se.sics.kompics.config.Config;
import se.sics.ktoolbox.hdfs.HDFSResource;
import se.sics.ktoolbox.hops.managedStore.HopsFactory;
import se.sics.ktoolbox.kafka.KafkaResource;
import se.sics.ktoolbox.util.identifiable.Identifier;
import se.sics.ktoolbox.util.idextractor.EventOverlayIdExtractor;
import se.sics.ktoolbox.util.managedStore.core.FileMngr;
import se.sics.ktoolbox.util.managedStore.core.HashMngr;
import se.sics.ktoolbox.util.managedStore.core.TransferMngr;
import se.sics.ktoolbox.util.managedStore.core.impl.LBAOTransferMngr;
import se.sics.ktoolbox.util.managedStore.core.util.HashUtil;
import se.sics.ktoolbox.util.managedStore.core.util.Torrent;
import se.sics.ktoolbox.util.managedStore.resources.LocalDiskResource;
import se.sics.ktoolbox.util.network.KAddress;
import se.sics.ktoolbox.util.network.ports.One2NChannel;

/**
 *
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class HopsTorrentMngr {

    private static final Logger LOG = LoggerFactory.getLogger(VoDMngrComp.class);
    private String logPrefix = "";

    private final Config config;
    private final ComponentProxy proxy;
    private final KAddress selfAdr;
    //**************************************************************************
    private final VoDMngrComp.ExtPort extPorts;
    private final Negative<HopsTorrentPort> torrentPort;
    private final Positive<ReportPort> reportPort;
    private final One2NChannel reportChannel;
    //**************************************************************************
    private Map<Identifier, Pair<FileInfo, TorrentInfo>> libraryContents = new HashMap<>();
    private Map<Identifier, Component> components = new HashMap<>();
    //**************************************************************************
    private Map<Identifier, TorrentExtendedStatusEvent.Request> pendingRequests = new HashMap<>();

    public HopsTorrentMngr(ComponentProxy proxy, Config config, String logPrefix, KAddress selfAdr, VoDMngrComp.ExtPort extPorts) {
        this.proxy = proxy;
        this.logPrefix = logPrefix;
        this.config = config;
        this.extPorts = extPorts;
        this.selfAdr = selfAdr;
        torrentPort = proxy.getPositive(HopsTorrentPort.class).getPair();
        reportPort = proxy.getNegative(ReportPort.class).getPair();
        reportChannel = One2NChannel.getChannel("vodMngrReportChannel", (Negative<ReportPort>) reportPort.getPair(), new EventOverlayIdExtractor());
    }

    public void subscribe() {
        proxy.subscribe(handleHopsTorrentUpload, torrentPort);
        proxy.subscribe(handleHopsTorrentDownload, torrentPort);
        proxy.subscribe(handleHopsTorrentStop, torrentPort);
        proxy.subscribe(handleContentsRequest, torrentPort);
        proxy.subscribe(handleTorrentStatusRequest, torrentPort);
        proxy.subscribe(handleTorrentStatusResponse, reportPort);
        proxy.subscribe(handleDownloadCompleted, reportPort);
    }

    Handler handleHopsTorrentUpload = new Handler<HopsTorrentUploadEvent.Request>() {
        @Override
        public void handle(HopsTorrentUploadEvent.Request req) {
            LOG.trace("{}received:{}", logPrefix, req);
            HopsTorrentUploadEvent.Response resp;
            if (libraryContents.containsKey(req.torrentId)) {
                resp = req.badRequest("file in library");
            } else {
                Pair<FileInfo, TorrentInfo> elementInfo = libraryContents.get(req.torrentId);
                TorrentStatus status;
                if (elementInfo == null) {
                    status = TorrentStatus.NONE;
                } else {
                    status = elementInfo.getValue1().getStatus();
                }
                switch (status) {
                    case UPLOADING:
                        resp = req.success();
                        break;
                    case DOWNLOADING:
                        resp = req.badRequest("can't upload file with none status");
                        break;
                    case NONE:
                        FileInfo fileInfo = new FileInfo(LocalDiskResource.type, req.hdfsResource.fileName, "", 0, "");
                        Map<Identifier, KAddress> partners = new HashMap<>();
                        TorrentInfo torrentInfo = new TorrentInfo(TorrentStatus.UPLOADING, partners, 0, 0, 0);
                        libraryContents.put(req.torrentId, Pair.with(fileInfo, torrentInfo));
                        uploadHopsTorrent(req.hdfsResource, req.torrentId);
                        resp = req.success();
                        break;
                    default:
                        resp = req.fail("missing logic");
                }
            }
            LOG.trace("{}answering:{}", logPrefix, resp);
            proxy.answer(req, resp);
        }
    };

    private void uploadHopsTorrent(final HDFSResource hdfsResource, final Identifier torrentId) {
        TorrentDetails torrentDetails = new TorrentDetails() {
            private final Torrent torrent;
            private final Triplet<FileMngr, HashMngr, TransferMngr> mngrs;

            {
                int pieceSize = 1024;
                int piecesPerBlock = 1024;
                int blockSize = pieceSize * piecesPerBlock;
                String hashAlg = HashUtil.getAlgName(HashUtil.SHA);

                Pair<FileMngr, HashMngr> fileHashMngr = HopsFactory.getCompleteCached(config, hdfsResource, null, hashAlg, blockSize, pieceSize);
                mngrs = fileHashMngr.add((TransferMngr) null);
                long fileSize = mngrs.getValue0().length();

                se.sics.ktoolbox.util.managedStore.core.util.FileInfo fileInfo = se.sics.ktoolbox.util.managedStore.core.util.FileInfo.newFile(hdfsResource.fileName, fileSize);
                se.sics.ktoolbox.util.managedStore.core.util.TorrentInfo torrentInfo
                        = new se.sics.ktoolbox.util.managedStore.core.util.TorrentInfo(pieceSize, piecesPerBlock, hashAlg, -1);
                torrent = new Torrent(torrentId, fileInfo, torrentInfo);
            }

            @Override
            public Identifier getOverlayId() {
                return torrent.overlayId;
            }

            @Override
            public boolean download() {
                return false;
            }

            @Override
            public Torrent getTorrent() {
                return torrent;
            }

            @Override
            public Triplet<FileMngr, HashMngr, TransferMngr> torrentMngrs(Torrent torrent) {
                return mngrs;
            }
        };

        StreamHostComp.ExtPort shcExtPorts = new StreamHostComp.ExtPort(extPorts.timerPort, extPorts.networkPort);
        Component torrentComp = proxy.create(StreamHostComp.class, new StreamHostComp.Init(shcExtPorts, selfAdr, torrentDetails, new ArrayList<KAddress>()));
        reportChannel.addChannel(torrentId, torrentComp.getPositive(ReportPort.class));
        components.put(torrentId, torrentComp);
        proxy.trigger(Start.event, torrentComp.control());
    }

    Handler handleHopsTorrentDownload = new Handler<HopsTorrentDownloadEvent.Request>() {
        @Override
        public void handle(HopsTorrentDownloadEvent.Request req) {
            LOG.trace("{}received:{}", logPrefix, req);
            HopsTorrentDownloadEvent.Response resp;

            if (!libraryContents.containsKey(req.torrentId)) {
                FileInfo fileInfo = new FileInfo(LocalDiskResource.type, req.hdfsResource.fileName, "", 0, "");
                Map<Identifier, KAddress> partners = new HashMap<>();
                TorrentInfo torrentInfo = new TorrentInfo(TorrentStatus.DOWNLOADING, partners, 0, 0, 0);
                libraryContents.put(req.torrentId, Pair.with(fileInfo, torrentInfo));
                downloadHopsTorrent(req.hdfsResource, req.kafkaResource, req.torrentId, req.partners);
                resp = req.success();
            } else {
                Pair<FileInfo, TorrentInfo> elementInfo = libraryContents.get(req.torrentId);
                TorrentStatus status = elementInfo.getValue1().getStatus();
                switch (status) {
                    case UPLOADING:
                        resp = req.badRequest("can't download file with uploading status");
                        break;
                    case DOWNLOADING:
                        resp = req.success();
                        break;
                    case NONE:
                        resp = req.badRequest("can't download file with none status");
                        break;
                    default:
                        resp = req.fail("missing logic");
                }
            }
            LOG.trace("{}answering:{}", logPrefix, resp);
            proxy.answer(req, resp);
        }
    };

    private void downloadHopsTorrent(final HDFSResource hdfsResource, final KafkaResource kafkaResource, final Identifier torrentId, final List<KAddress> partners) {
        TorrentDetails torrentDetails = new TorrentDetails() {
            @Override
            public Identifier getOverlayId() {
                return torrentId;
            }

            @Override
            public boolean download() {
                return true;
            }

            @Override
            public Torrent getTorrent() {
                throw new RuntimeException("logic error");
            }

            @Override
            public Triplet<FileMngr, HashMngr, TransferMngr> torrentMngrs(Torrent torrent) {
                long fileSize = torrent.fileInfo.size;
                String hashAlg = torrent.torrentInfo.hashAlg;
                int pieceSize = torrent.torrentInfo.pieceSize;
                int blockSize = torrent.torrentInfo.piecesPerBlock * pieceSize;
                int hashSize = HashUtil.getHashSize(torrent.torrentInfo.hashAlg);

                Pair<FileMngr, HashMngr> fileHashMngr = HopsFactory.getIncompleteCached(config, hdfsResource, kafkaResource, fileSize, hashAlg, blockSize, pieceSize);
                return fileHashMngr.add((TransferMngr) new LBAOTransferMngr(torrent, fileHashMngr.getValue1(), fileHashMngr.getValue0(), 10));
            }
        };
        StreamHostComp.ExtPort shcExtPorts = new StreamHostComp.ExtPort(extPorts.timerPort, extPorts.networkPort);
        Component torrentComp = proxy.create(StreamHostComp.class, new StreamHostComp.Init(shcExtPorts, selfAdr, torrentDetails, partners));
        reportChannel.addChannel(torrentId, torrentComp.getPositive(ReportPort.class));
        components.put(torrentId, torrentComp);
        proxy.trigger(Start.event, torrentComp.control());
    }
    
    Handler handleHopsTorrentStop = new Handler<HopsTorrentStopEvent.Request>() {
        @Override
        public void handle(HopsTorrentStopEvent.Request req) {
            LOG.trace("{}received:{}", logPrefix, req);
            Pair<FileInfo, TorrentInfo> tInfo = libraryContents.remove(req.torrentId);
            Component torrentComp = components.remove(req.torrentId);

            reportChannel.removeChannel(req.torrentId, torrentComp.getPositive(ReportPort.class));
            proxy.trigger(Kill.event, torrentComp.control());
            proxy.answer(req, req.success());
        }
    };

    Handler handleContentsRequest = new Handler<ContentsSummaryEvent.Request>() {
        @Override
        public void handle(ContentsSummaryEvent.Request req) {
            LOG.info("{}received:{}", logPrefix, req);
            List<ElementSummary> esList = new ArrayList<>();
            for (Map.Entry<Identifier, Pair<FileInfo, TorrentInfo>> e : libraryContents.entrySet()) {
                ElementSummary es = new ElementSummary(e.getValue().getValue0().name, e.getKey(), e.getValue().getValue1().getStatus());
                esList.add(es);
            }
            ContentsSummaryEvent.Response resp = req.success(esList);
            LOG.info("{}answering:{}", logPrefix, resp);
            proxy.answer(req, resp);
        }
    };

    Handler handleDownloadCompleted = new Handler<DownloadSummaryEvent>() {
        @Override
        public void handle(DownloadSummaryEvent event) {
            LOG.trace("{}received:{}", logPrefix, event);
            Pair<FileInfo, TorrentInfo> torrentDetails = libraryContents.get(event.torrentId);
            if (torrentDetails == null) {
                //TODO - might happen when i stop torrent
                return;
            }
            torrentDetails.getValue1().finishDownload();
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
            proxy.answer(req, req.succes(resp.value));
        }
    };

    

}
