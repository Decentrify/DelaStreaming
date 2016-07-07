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
package se.sics.gvod.stream.mngr;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.gvod.stream.mngr.event.system.SystemAddressEvent;
import se.sics.gvod.stream.mngr.hops.HopsHelperMngr;
import se.sics.gvod.stream.mngr.hops.HopsPort;
import se.sics.gvod.stream.mngr.hops.HopsTorrentMngr;
import se.sics.gvod.stream.mngr.hops.HopsTorrentPort;
import se.sics.gvod.stream.report.ReportPort;
import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.Handler;
import se.sics.kompics.Negative;
import se.sics.kompics.Positive;
import se.sics.kompics.Start;
import se.sics.kompics.network.Network;
import se.sics.kompics.timer.Timer;
import se.sics.ktoolbox.util.network.KAddress;

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
//    Negative<LibraryPort> libraryPort = provides(LibraryPort.class);
//    Negative<TorrentPort> torrentPort = provides(TorrentPort.class);
//    Negative<VideoPort> videoPort = provides(VideoPort.class);
    Negative<HopsPort> hopsPort = provides(HopsPort.class);
    Negative<HopsTorrentPort> hopsTorrentPort = provides(HopsTorrentPort.class);
    //*******************INTERNAL_DO_NOT_CONNECT_TO*****************************
    Positive<ReportPort> reportPort = requires(ReportPort.class);
    //**************************EXTERNAL_STATE**********************************
    private final KAddress selfAdr;
    //**************************INTERNAL_STATE**********************************
    private final HopsHelperMngr hopsHelperMngr;
    private final HopsTorrentMngr hopsTorrentMngr;

    public VoDMngrComp(Init init) {
        LOG.info("{}initiating...", logPrefix);

        extPorts = init.extPorts;
        selfAdr = init.selfAddress;

        hopsHelperMngr = new HopsHelperMngr(proxy, logPrefix);
        hopsHelperMngr.subscribe();
        hopsTorrentMngr = new HopsTorrentMngr(proxy, config(), logPrefix, selfAdr, extPorts);
        hopsTorrentMngr.subscribe();
            
        subscribe(handleStart, control);
        subscribe(handleSystemAddress, systemPort);
//        subscribe(handleLibraryContent, libraryPort);
//        subscribe(handleLibraryElement, libraryPort);
//        subscribe(handleLibraryAdd, libraryPort);
//        subscribe(handleTorrentUpload, torrentPort);
//        subscribe(handleTorrentDownload, torrentPort);
//        subscribe(handleTorrentStop, torrentPort);
//        subscribe(handleVideoPlay, videoPort);
//        subscribe(handleVideoStop, videoPort);
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

    

//    Handler handleLibraryContent = new Handler<LibraryContentsEvent.Request>() {
//        @Override
//        public void handle(LibraryContentsEvent.Request req) {
//            throw new UnsupportedOperationException();
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
//        }
//    };

//    Handler handleLibraryElement = new Handler<LibraryElementGetEvent.Request>() {
//        @Override
//        public void handle(LibraryElementGetEvent.Request req) {
//            throw new UnsupportedOperationException();
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
//        }
//    };
//
//    Handler handleLibraryAdd = new Handler<LibraryAddEvent.Request>() {
//        @Override
//        public void handle(LibraryAddEvent.Request req) {
//            throw new UnsupportedOperationException();
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
//        }
//    };
//
//    Handler handleTorrentUpload = new Handler<TorrentUploadEvent.Request>() {
//        @Override
//        public void handle(TorrentUploadEvent.Request req) {
//            throw new UnsupportedOperationException();
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
//                        resp = req.badRequest("can't upload hdfsResource with none status");
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
//                resp = req.badRequest("no such hdfsResource in library");
//            }
//            LOG.info("{}answering:{}", logPrefix, resp);
//            answer(req, resp);
//        }
//    };
//
//    Handler handleTorrentDownload = new Handler<TorrentDownloadEvent.Request>() {
//        @Override
//        public void handle(TorrentDownloadEvent.Request req) {
//            throw new UnsupportedOperationException();
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
//                        resp = req.badRequest("can't download hdfsResource with uploading status");
//                        break;
//                    case DOWNLOADING:
//                        resp = req.success();
//                        break;
//                    case NONE:
//                        resp = req.badRequest("can't download hdfsResource with none status");
//                        break;
//                    default:
//                        resp = req.fail("missing logic");
//                }
//            }
//            LOG.info("{}answering:{}", logPrefix, resp);
//            answer(req, resp);
//        }
//    };
//
//    Handler handleTorrentStop = new Handler<TorrentStopEvent.Request>() {
//        @Override
//        public void handle(TorrentStopEvent.Request req) {
//            throw new UnsupportedOperationException();
//            LOG.info("{}received:{}", logPrefix, req);
//            TorrentStopEvent.Response resp;
//            if (libraryContents.containsKey(req.overlayId)) {
//                Pair<FileInfo, TorrentInfo> elementInfo = libraryContents.get(req.overlayId);
//                switch (elementInfo.getValue1().status) {
//                    case NONE:
//                        resp = req.badRequest("can't stop hdfsResource with none status");
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
//                resp = req.badRequest("no such hdfsResource in library");
//            }
//            LOG.info("{}answering:{}", logPrefix, resp);
//            answer(req, resp);
//        }
//    };
//
//    Handler handleVideoPlay = new Handler<VideoPlayEvent.Request>() {
//        @Override
//        public void handle(VideoPlayEvent.Request event) {
//            throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
//        }
//    };
//
//    Handler handleVideoStop = new Handler<VideoStopEvent.Request>() {
//        @Override
//        public void handle(VideoStopEvent.Request event) {
//            throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
//        }
//    };

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
