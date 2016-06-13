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
package se.sics.gvod.mngr;

import com.google.common.primitives.Ints;
import java.io.File;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.javatuples.Pair;
import org.javatuples.Triplet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.gvod.core.util.TorrentDetails;
import se.sics.gvod.mngr.event.TorrentExtendedStatusEvent;
import se.sics.gvod.mngr.event.ContentsSummaryEvent;
import se.sics.gvod.mngr.event.HopsFileDeleteEvent;
import se.sics.gvod.mngr.event.HopsTorrentDownloadEvent;
import se.sics.gvod.mngr.event.HopsTorrentStopEvent;
import se.sics.gvod.mngr.event.HopsTorrentUploadEvent;
import se.sics.gvod.mngr.event.LibraryAddEvent;
import se.sics.gvod.mngr.event.LibraryContentsEvent;
import se.sics.gvod.mngr.event.LibraryElementGetEvent;
import se.sics.gvod.mngr.event.TorrentDownloadEvent;
import se.sics.gvod.mngr.event.TorrentStopEvent;
import se.sics.gvod.mngr.event.TorrentUploadEvent;
import se.sics.gvod.mngr.event.VideoPlayEvent;
import se.sics.gvod.mngr.event.VideoStopEvent;
import se.sics.gvod.mngr.event.system.HopsConnectionEvent;
import se.sics.gvod.mngr.event.system.SystemAddressEvent;
import se.sics.gvod.mngr.util.ElementSummary;
import se.sics.gvod.mngr.util.FileInfo;
import se.sics.gvod.mngr.util.HDFSResource;
import se.sics.gvod.mngr.util.TorrentExtendedStatus;
import se.sics.gvod.mngr.util.TorrentInfo;
import se.sics.gvod.stream.report.util.TorrentStatus;
import se.sics.gvod.stream.StreamHostComp;
import se.sics.gvod.stream.report.event.DownloadSummaryEvent;
import se.sics.gvod.stream.report.ReportPort;
import se.sics.gvod.stream.report.event.StatusSummaryEvent;
import se.sics.kompics.Component;
import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.Handler;
import se.sics.kompics.Kill;
import se.sics.kompics.Negative;
import se.sics.kompics.Positive;
import se.sics.kompics.Start;
import se.sics.kompics.network.Network;
import se.sics.kompics.timer.Timer;
import se.sics.ktoolbox.hops.managedStore.storage.HopsFactory;
import se.sics.ktoolbox.hops.managedStore.storage.HDFSHelper;
import se.sics.ktoolbox.util.identifiable.Identifier;
import se.sics.ktoolbox.util.identifiable.basic.IntIdentifier;
import se.sics.ktoolbox.util.identifiable.basic.OverlayIdentifier;
import se.sics.ktoolbox.util.idextractor.EventOverlayIdExtractor;
import se.sics.ktoolbox.util.managedStore.core.FileMngr;
import se.sics.ktoolbox.util.managedStore.core.HashMngr;
import se.sics.ktoolbox.util.managedStore.core.TransferMngr;
import se.sics.ktoolbox.util.managedStore.core.impl.LBAOTransferMngr;
import se.sics.ktoolbox.util.managedStore.core.util.HashUtil;
import se.sics.ktoolbox.util.managedStore.core.util.Torrent;
import se.sics.ktoolbox.util.managedStore.resources.LocalDiskResource;
import se.sics.ktoolbox.util.network.KAddress;
import se.sics.ktoolbox.util.network.basic.BasicAddress;
import se.sics.ktoolbox.util.network.nat.NatAwareAddressImpl;
import se.sics.ktoolbox.util.network.ports.One2NChannel;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class VoDMngrComp extends ComponentDefinition {

    private static final Logger LOG = LoggerFactory.getLogger(VoDMngrComp.class);
    private String logPrefix = "";

    //***************************CONNECTIONS************************************
    ExtPort extPorts;
    //**********************EXTERNAL_CONNECT_TO*********************************
    Negative<SystemPort> systemPort = provides(SystemPort.class);
    Negative<LibraryPort> libraryPort = provides(LibraryPort.class);
    Negative<TorrentPort> torrentPort = provides(TorrentPort.class);
    Negative<VideoPort> videoPort = provides(VideoPort.class);
    //*******************INTERNAL_DO_NOT_CONNECT_TO*****************************
    Positive<ReportPort> reportPort = requires(ReportPort.class);
    One2NChannel reportChannel;
    //**************************EXTERNAL_STATE**********************************
    private final KAddress selfAdr;
    //**************************INTERNAL_STATE**********************************
    private Map<Identifier, Pair<FileInfo, TorrentInfo>> libraryContents = new HashMap<>();
    private Map<Identifier, Component> components = new HashMap<>();
    private Component torrentComp;
    //****************************AUX_STATE*************************************
    private Map<Identifier, TorrentExtendedStatusEvent.Request> pendingRequests = new HashMap<>();

    public VoDMngrComp(Init init) {
        LOG.info("{}initiating...", logPrefix);

        extPorts = init.extPorts;
        selfAdr = init.selfAddress;

        reportChannel = One2NChannel.getChannel("vodMngrReportChannel", (Negative<ReportPort>) reportPort.getPair(), new EventOverlayIdExtractor());

        subscribe(handleStart, control);
        subscribe(handleLibraryContent, libraryPort);
        subscribe(handleLibraryElement, libraryPort);
        subscribe(handleLibraryAdd, libraryPort);
        subscribe(handleTorrentUpload, torrentPort);
        subscribe(handleTorrentDownload, torrentPort);
        subscribe(handleTorrentStop, torrentPort);
        subscribe(handleVideoPlay, videoPort);
        subscribe(handleVideoStop, videoPort);

        subscribe(handleSystemAddress, systemPort);
        subscribe(handleHopsConnection, systemPort);
        
        subscribe(handleContentsRequest, libraryPort);
        subscribe(handleTorrentStatusRequest, libraryPort);
        subscribe(handleTorrentStatusResponse, reportPort);
        subscribe(handleHopsFileDelete, libraryPort);

        subscribe(handleHopsTorrentUpload, torrentPort);
        subscribe(handleHopsTorrentDownload, torrentPort);
        subscribe(handleDownloadCompleted, reportPort);
        subscribe(handleHopsTorrentStop, torrentPort);
    }

    Handler handleStart = new Handler<Start>() {
        @Override
        public void handle(Start event) {
            LOG.info("{}starting...", logPrefix);
        }
    };
    
    Handler handleSystemAddress = new Handler<SystemAddressEvent.Request>() {
        @Override
        public void handle(SystemAddressEvent.Request req) {
            LOG.trace("{}received:{}", logPrefix, req);
            answer(req, req.success(selfAdr));
        }
    };
    
    Handler handleHopsConnection = new Handler<HopsConnectionEvent.Request>() {
        @Override
        public void handle(HopsConnectionEvent.Request req) {
            LOG.trace("{}received:{}", logPrefix, req);
            boolean result = HDFSHelper.canConnect(req.connection.hopsIp, req.connection.hopsPort);
            if(result) {
                answer(req, req.success());
            } else {
                answer(req, req.fail("cannot connect to hops"));
            }
        }
    };

    Handler handleLibraryContent = new Handler<LibraryContentsEvent.Request>() {
        @Override
        public void handle(LibraryContentsEvent.Request req) {
            throw new UnsupportedOperationException();
//            LOG.info("{}received:{}", logPrefix, req);
//            List<LibraryElementSummary> lesList = new ArrayList<>();
//            for (Map.Entry<Identifier, Pair<FileInfo, TorrentInfo>> e : libraryContents.entrySet()) {
//                TorrentExtendedStatus les = new TorrentExtendedStatus(e.getValue().getValue0().uri,
//                        e.getValue().getValue0().name, e.getValue().getValue1().status, Optional.of(e.getKey()));
//                lesList.add(les);
//            }
//            LibraryContentsEvent.Response resp = req.success(lesList);
//            LOG.info("{}answering:{}", logPrefix, resp);
//            answer(req, resp);
        }
    };

    Handler handleLibraryElement = new Handler<LibraryElementGetEvent.Request>() {
        @Override
        public void handle(LibraryElementGetEvent.Request req) {
            throw new UnsupportedOperationException();
//            LOG.info("{}received:{}", logPrefix, req);
//            if (libraryContents.containsKey(req.les.overlayId.get())) {
//                Pair<FileInfo, TorrentInfo> elementInfo = libraryContents.get(req.les.overlayId.get());
//                LibraryElementGetEvent.Response resp = req.success(elementInfo.getValue0(), elementInfo.getValue1());
//                LOG.info("{}answering:{}", logPrefix, resp);
//                answer(req, resp);
//            } else {
//                LibraryElementGetEvent.Response resp = req.badRequest("missing library element");
//                LOG.info("{}answering:{}", logPrefix, resp);
//                answer(req, resp);
//            }
        }
    };

    Handler handleLibraryAdd = new Handler<LibraryAddEvent.Request>() {
        @Override
        public void handle(LibraryAddEvent.Request req) {
            throw new UnsupportedOperationException();
//            LOG.info("{}received:{}", logPrefix, req);
//            if (!libraryContents.containsKey(req.overlayId)) {
//                Map<Identifier, KAddress> partners = new HashMap<>();
//                TorrentInfo torrentInfo = new TorrentInfo(TorrentStatus.NONE, partners, 0, 0, 0);
//                libraryContents.put(req.overlayId, Pair.with(req.fileInfo, torrentInfo));
//                LibraryAddEvent.Response resp = req.success();
//                LOG.info("{}answering:{}", logPrefix, resp);
//                answer(req, resp);
//            } else {
//                LibraryAddEvent.Response resp = req.badRequest("element already in library");
//                LOG.info("{}answering:{}", logPrefix, resp);
//                answer(req, resp);
//            }
        }
    };

    Handler handleTorrentUpload = new Handler<TorrentUploadEvent.Request>() {
        @Override
        public void handle(TorrentUploadEvent.Request req) {
            throw new UnsupportedOperationException();
//            LOG.info("{}received:{}", logPrefix, req);
//            TorrentUploadEvent.Response resp;
//            if (libraryContents.containsKey(req.overlayId)) {
//                Pair<FileInfo, TorrentInfo> elementInfo = libraryContents.get(req.overlayId);
//                TorrentStatus status = elementInfo.getValue1().status;
//                switch (status) {
//                    case UPLOADING:
//                        resp = req.success();
//                        break;
//                    case DOWNLOADING:
//                        resp = req.badRequest("can't upload file with none status");
//                        break;
//                    case NONE:
//                        Map<Identifier, KAddress> partners = new HashMap<>();
//                        TorrentInfo torrentInfo = new TorrentInfo(TorrentStatus.UPLOADING, partners, 0, 0, 0);
//                        libraryContents.put(req.overlayId, Pair.with(elementInfo.getValue0(), torrentInfo));
//                        createMockUploadHopsTorrent();
//                        resp = req.success();
//                        break;
//                    default:
//                        resp = req.fail("missing logic");
//                }
//            } else {
//                resp = req.badRequest("no such file in library");
//            }
//            LOG.info("{}answering:{}", logPrefix, resp);
//            answer(req, resp);
        }
    };

    Handler handleTorrentDownload = new Handler<TorrentDownloadEvent.Request>() {
        @Override
        public void handle(TorrentDownloadEvent.Request req) {
            throw new UnsupportedOperationException();
//            LOG.info("{}received:{}", logPrefix, req);
//            TorrentDownloadEvent.Response resp;
//
//            if (!libraryContents.containsKey(req.overlayId)) {
//                FileInfo fileInfo = new FileInfo(LocalDiskResource.type, req.fileName, "", 0, "");
//                Map<Identifier, KAddress> partners = new HashMap<>();
//                TorrentInfo torrentInfo = new TorrentInfo(TorrentStatus.DOWNLOADING, partners, 0, 0, 0);
//                libraryContents.put(req.overlayId, Pair.with(fileInfo, torrentInfo));
//                createMockDownloadHopsTorrent();
//                resp = req.success();
//            } else {
//                Pair<FileInfo, TorrentInfo> elementInfo = libraryContents.get(req.overlayId);
//                TorrentStatus status = elementInfo.getValue1().status;
//                switch (status) {
//                    case UPLOADING:
//                        resp = req.badRequest("can't download file with uploading status");
//                        break;
//                    case DOWNLOADING:
//                        resp = req.success();
//                        break;
//                    case NONE:
//                        resp = req.badRequest("can't download file with none status");
//                        break;
//                    default:
//                        resp = req.fail("missing logic");
//                }
//            }
//            LOG.info("{}answering:{}", logPrefix, resp);
//            answer(req, resp);
        }
    };

    Handler handleTorrentStop = new Handler<TorrentStopEvent.Request>() {
        @Override
        public void handle(TorrentStopEvent.Request req) {
            throw new UnsupportedOperationException();
//            LOG.info("{}received:{}", logPrefix, req);
//            TorrentStopEvent.Response resp;
//            if (libraryContents.containsKey(req.overlayId)) {
//                Pair<FileInfo, TorrentInfo> elementInfo = libraryContents.get(req.overlayId);
//                switch (elementInfo.getValue1().status) {
//                    case NONE:
//                        resp = req.badRequest("can't stop file with none status");
//                        break;
//                    case UPLOADING:
//                        Map<Identifier, KAddress> partners = new HashMap<>();
//                        TorrentInfo torrentInfo = new TorrentInfo(TorrentStatus.NONE, partners, 0, 0, 0);
//                        libraryContents.put(req.overlayId, Pair.with(elementInfo.getValue0(), torrentInfo));
//                        resp = req.success();
//                        break;
//                    case DOWNLOADING:
//                        libraryContents.remove(req.overlayId);
//                        resp = req.success();
//                        break;
//                    default:
//                        resp = req.badRequest("missing logic");
//                }
//            } else {
//                resp = req.badRequest("no such file in library");
//            }
//            LOG.info("{}answering:{}", logPrefix, resp);
//            answer(req, resp);
        }
    };

    Handler handleVideoPlay = new Handler<VideoPlayEvent.Request>() {
        @Override
        public void handle(VideoPlayEvent.Request event) {
            throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        }
    };

    Handler handleVideoStop = new Handler<VideoStopEvent.Request>() {
        @Override
        public void handle(VideoStopEvent.Request event) {
            throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
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
            answer(req, resp);
        }
    };

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
                        String hopsURL = req.hopsIp + ":" + req.hopsPort;
                        FileInfo fileInfo = new FileInfo(LocalDiskResource.type, req.fileName, "", 0, "");
                        Map<Identifier, KAddress> partners = new HashMap<>();
                        TorrentInfo torrentInfo = new TorrentInfo(TorrentStatus.UPLOADING, partners, 0, 0, 0);
                        libraryContents.put(req.torrentId, Pair.with(fileInfo, torrentInfo));
                        uploadHopsTorrent(req.torrentId, req.fileName, req.dirPath, hopsURL);
                        resp = req.success();
                        break;
                    default:
                        resp = req.fail("missing logic");
                }
            }
            LOG.trace("{}answering:{}", logPrefix, resp);
            answer(req, resp);
        }
    };

    private void uploadHopsTorrent(final Identifier torrentId, final String fileName, final String dirPath, final String hopsURL) {
        TorrentDetails torrentDetails = new TorrentDetails() {
            private final Torrent torrent;
            private final Triplet<FileMngr, HashMngr, TransferMngr> mngrs;

            {
                int pieceSize = 1024;
                int piecesPerBlock = 1024;
                int blockSize = pieceSize * piecesPerBlock;
                String hashAlg = HashUtil.getAlgName(HashUtil.SHA);

                Pair<FileMngr, HashMngr> fileHashMngr = HopsFactory.getComplete(hopsURL, dirPath + File.separator + fileName, hashAlg, blockSize, pieceSize);
                mngrs = fileHashMngr.add((TransferMngr) null);
                long fileSize = mngrs.getValue0().length();

                se.sics.ktoolbox.util.managedStore.core.util.FileInfo fileInfo = se.sics.ktoolbox.util.managedStore.core.util.FileInfo.newFile(fileName, fileSize);
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
        torrentComp = create(StreamHostComp.class, new StreamHostComp.Init(shcExtPorts, selfAdr, torrentDetails, new ArrayList<KAddress>()));
        reportChannel.addChannel(torrentId, torrentComp.getPositive(ReportPort.class));
        components.put(torrentId, torrentComp);
        trigger(Start.event, torrentComp.control());
    }

    Handler handleHopsTorrentDownload = new Handler<HopsTorrentDownloadEvent.Request>() {
        @Override
        public void handle(HopsTorrentDownloadEvent.Request req) {
            LOG.trace("{}received:{}", logPrefix, req);
            HopsTorrentDownloadEvent.Response resp;

            if (!libraryContents.containsKey(req.torrentId)) {
                FileInfo fileInfo = new FileInfo(LocalDiskResource.type, req.fileName, "", 0, "");
                Map<Identifier, KAddress> partners = new HashMap<>();
                TorrentInfo torrentInfo = new TorrentInfo(TorrentStatus.DOWNLOADING, partners, 0, 0, 0);
                libraryContents.put(req.torrentId, Pair.with(fileInfo, torrentInfo));
                downloadHopsTorrent(req.torrentId, req.fileName, req.dirPath, req.hopsIp + ":" + req.hopsPort, req.partners);
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
            answer(req, resp);
        }
    };

    private void downloadHopsTorrent(final Identifier torrentId, final String fileName, final String dirPath,
            final String hopsURL, final List<KAddress> partners) {
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

                Pair<FileMngr, HashMngr> fileHashMngr = HopsFactory.getIncomplete(hopsURL, dirPath + File.separator + fileName, fileSize, hashAlg, blockSize, pieceSize);
                return fileHashMngr.add((TransferMngr) new LBAOTransferMngr(torrent, fileHashMngr.getValue1(), fileHashMngr.getValue0(), 10));
            }
        };
        StreamHostComp.ExtPort shcExtPorts = new StreamHostComp.ExtPort(extPorts.timerPort, extPorts.networkPort);
        torrentComp = create(StreamHostComp.class, new StreamHostComp.Init(shcExtPorts, selfAdr, torrentDetails, partners));
        reportChannel.addChannel(torrentId, torrentComp.getPositive(ReportPort.class));
        components.put(torrentId, torrentComp);
        trigger(Start.event, torrentComp.control());
    }

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
            trigger(new StatusSummaryEvent.Request(req.eventId, req.torrentId), reportPort);
        }
    };

    Handler handleTorrentStatusResponse = new Handler<StatusSummaryEvent.Response>() {
        @Override
        public void handle(StatusSummaryEvent.Response resp) {
            LOG.trace("{}received:{}", logPrefix, resp);
            TorrentExtendedStatusEvent.Request req = pendingRequests.remove(resp.getId());
            answer(req, req.succes(resp.value));
        }
    };

    Handler handleHopsTorrentStop = new Handler<HopsTorrentStopEvent.Request>() {
        @Override
        public void handle(HopsTorrentStopEvent.Request req) {
            LOG.trace("{}received:{}", logPrefix, req);
            Pair<FileInfo, TorrentInfo> tInfo = libraryContents.remove(req.torrentId);
            Component torrentComp = components.remove(req.torrentId);

            reportChannel.removeChannel(req.torrentId, torrentComp.getPositive(ReportPort.class));
            trigger(Kill.event, torrentComp.control());
            answer(req, req.success());
        }
    };

    Handler handleHopsFileDelete = new Handler<HopsFileDeleteEvent.Request>() {
        @Override
        public void handle(HopsFileDeleteEvent.Request req) {
            LOG.trace("{}received:{}", logPrefix, req);
            HDFSHelper.delete(req.file.hopsIp, req.file.hopsPort, req.file.dirPath, req.file.fileName);
            answer(req, req.success());
        }
    };

    public static class ExtPort {

        public final Positive<Timer> timerPort;
        public final Positive<Network> networkPort;

        public ExtPort(Positive<Timer> timerPort, Positive<Network> networkPort) {
            this.timerPort = timerPort;
            this.networkPort = networkPort;
        }
    }

    public static class Init extends se.sics.kompics.Init<VoDMngrComp> {

        public final ExtPort extPorts;
        public final KAddress selfAddress;

        public Init(ExtPort extPorts, KAddress selfAddress) {
            this.extPorts = extPorts;
            this.selfAddress = selfAddress;
        }
    }
}
