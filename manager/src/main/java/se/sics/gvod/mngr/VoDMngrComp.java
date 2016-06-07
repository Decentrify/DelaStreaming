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

import com.google.common.base.Optional;
import com.google.common.primitives.Ints;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.javatuples.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.gvod.mngr.event.LibraryAddEvent;
import se.sics.gvod.mngr.event.LibraryContentsEvent;
import se.sics.gvod.mngr.event.LibraryElementGetEvent;
import se.sics.gvod.mngr.event.TorrentDownloadEvent;
import se.sics.gvod.mngr.event.TorrentStopEvent;
import se.sics.gvod.mngr.event.TorrentUploadEvent;
import se.sics.gvod.mngr.event.VideoPlayEvent;
import se.sics.gvod.mngr.event.VideoStopEvent;
import se.sics.gvod.mngr.util.FileInfo;
import se.sics.gvod.mngr.util.LibraryElementSummary;
import se.sics.gvod.mngr.util.TorrentInfo;
import se.sics.gvod.mngr.util.TorrentStatus;
import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.Handler;
import se.sics.kompics.Negative;
import se.sics.kompics.Positive;
import se.sics.kompics.Start;
import se.sics.kompics.network.Network;
import se.sics.kompics.timer.Timer;
import se.sics.ktoolbox.util.identifiable.Identifier;
import se.sics.ktoolbox.util.identifiable.basic.OverlayIdentifier;
import se.sics.ktoolbox.util.managedStore.resources.LocalDiskResource;
import se.sics.ktoolbox.util.network.KAddress;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class VoDMngrComp extends ComponentDefinition {

    private static final Logger LOG = LoggerFactory.getLogger(VoDMngrComp.class);
    private String logPrefix = "";

    //***************************CONNECTIONS************************************
    
    Negative<LibraryPort> libraryPort = provides(LibraryPort.class);
    Negative<TorrentPort> torrentPort = provides(TorrentPort.class);
    Negative<VideoPort> videoPort = provides(VideoPort.class);
    //**************************INTERNAL_STATE**********************************
    private Map<Identifier, Pair<FileInfo, TorrentInfo>> libraryContents = new HashMap<>();

    public VoDMngrComp(Init init) {
        LOG.info("{}initiating...", logPrefix);

        subscribe(handleStart, control);
        subscribe(handleLibraryContent, libraryPort);
        subscribe(handleLibraryElement, libraryPort);
        subscribe(handleLibraryAdd, libraryPort);
        subscribe(handleTorrentUpload, torrentPort);
        subscribe(handleTorrentDownload, torrentPort);
        subscribe(handleTorrentStop, torrentPort);
        subscribe(handleVideoPlay, videoPort);
        subscribe(handleVideoStop, videoPort);
    }

    Handler handleStart = new Handler<Start>() {
        @Override
        public void handle(Start event) {
            LOG.info("{}starting...", logPrefix);

            libraryContents.put(new OverlayIdentifier(Ints.toByteArray(1)), Pair.with(
                    new FileInfo(LocalDiskResource.type, "test1", "/root/test1", 1024, "test file 1"),
                    TorrentInfo.none()));
            libraryContents.put(new OverlayIdentifier(Ints.toByteArray(2)), Pair.with(
                    new FileInfo(LocalDiskResource.type, "test2", "/root/test2", 2024, "test file 2"),
                    TorrentInfo.none()));
        }
    };

    Handler handleLibraryContent = new Handler<LibraryContentsEvent.Request>() {
        @Override
        public void handle(LibraryContentsEvent.Request req) {
            LOG.info("{}received:{}", logPrefix, req);
            List<LibraryElementSummary> lesList = new ArrayList<>();
            for (Map.Entry<Identifier, Pair<FileInfo, TorrentInfo>> e : libraryContents.entrySet()) {
                LibraryElementSummary les = new LibraryElementSummary(e.getValue().getValue0().uri,
                        e.getValue().getValue0().name, e.getValue().getValue1().status, Optional.of(e.getKey()));
                lesList.add(les);
            }
            LibraryContentsEvent.Response resp = req.success(lesList);
            LOG.info("{}answering:{}", logPrefix, resp);
            answer(req, resp);
        }
    };

    Handler handleLibraryElement = new Handler<LibraryElementGetEvent.Request>() {
        @Override
        public void handle(LibraryElementGetEvent.Request req) {
            LOG.info("{}received:{}", logPrefix, req);
            if (libraryContents.containsKey(req.les.overlayId.get())) {
                Pair<FileInfo, TorrentInfo> elementInfo = libraryContents.get(req.les.overlayId.get());
                LibraryElementGetEvent.Response resp = req.success(elementInfo.getValue0(), elementInfo.getValue1());
                LOG.info("{}answering:{}", logPrefix, resp);
                answer(req, resp);
            } else {
                LibraryElementGetEvent.Response resp = req.badRequest("missing library element");
                LOG.info("{}answering:{}", logPrefix, resp);
                answer(req, resp);
            }
        }
    };

    Handler handleLibraryAdd = new Handler<LibraryAddEvent.Request>() {
        @Override
        public void handle(LibraryAddEvent.Request req) {
            LOG.info("{}received:{}", logPrefix, req);
            if (!libraryContents.containsKey(req.overlayId)) {
                Map<Identifier, KAddress> partners = new HashMap<>();
                TorrentInfo torrentInfo = new TorrentInfo(TorrentStatus.NONE, partners, 0, 0, 0);
                libraryContents.put(req.overlayId, Pair.with(req.fileInfo, torrentInfo));
                LibraryAddEvent.Response resp = req.success();
                LOG.info("{}answering:{}", logPrefix, resp);
                answer(req, resp);
            } else {
                LibraryAddEvent.Response resp = req.badRequest("element already in library");
                LOG.info("{}answering:{}", logPrefix, resp);
                answer(req, resp);
            }
        }
    };

    Handler handleTorrentUpload = new Handler<TorrentUploadEvent.Request>() {
        @Override
        public void handle(TorrentUploadEvent.Request req) {
            LOG.info("{}received:{}", logPrefix, req);
            TorrentUploadEvent.Response resp;
            if (libraryContents.containsKey(req.overlayId)) {
                Pair<FileInfo, TorrentInfo> elementInfo = libraryContents.get(req.overlayId);
                TorrentStatus status = elementInfo.getValue1().status;
                switch (status) {
                    case UPLOADING:
                        resp = req.success();
                        break;
                    case DOWNLOADING:
                        resp = req.badRequest("can't upload file with none status");
                        break;
                    case NONE:
                        Map<Identifier, KAddress> partners = new HashMap<>();
                        TorrentInfo torrentInfo = new TorrentInfo(TorrentStatus.UPLOADING, partners, 1, 0, 0);
                        libraryContents.put(req.overlayId, Pair.with(elementInfo.getValue0(), torrentInfo));
                        resp = req.success();
                        break;
                    default:
                        resp = req.fail("missing logic");
                }
            } else {
                resp = req.badRequest("no such file in library");
            }
            LOG.info("{}answering:{}", logPrefix, resp);
            answer(req, resp);
        }
    };

    Handler handleTorrentDownload = new Handler<TorrentDownloadEvent.Request>() {
        @Override
        public void handle(TorrentDownloadEvent.Request req) {
            LOG.info("{}received:{}", logPrefix, req);
            TorrentDownloadEvent.Response resp;

            if (!libraryContents.containsKey(req.overlayId)) {
                FileInfo fileInfo = new FileInfo(LocalDiskResource.type, req.fileName, "", 0, "");
                Map<Identifier, KAddress> partners = new HashMap<>();
                TorrentInfo torrentInfo = new TorrentInfo(TorrentStatus.DOWNLOADING, partners, 0, 0, 0);
                libraryContents.put(req.overlayId, Pair.with(fileInfo, torrentInfo));
                resp = req.success();
            } else {
                Pair<FileInfo, TorrentInfo> elementInfo = libraryContents.get(req.overlayId);
                TorrentStatus status = elementInfo.getValue1().status;
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
            LOG.info("{}answering:{}", logPrefix, resp);
            answer(req, resp);
        }
    };

    Handler handleTorrentStop = new Handler<TorrentStopEvent.Request>() {
        @Override
        public void handle(TorrentStopEvent.Request req) {
            LOG.info("{}received:{}", logPrefix, req);
            TorrentStopEvent.Response resp;
            if (libraryContents.containsKey(req.overlayId)) {
                Pair<FileInfo, TorrentInfo> elementInfo = libraryContents.get(req.overlayId);
                switch (elementInfo.getValue1().status) {
                    case NONE:
                        resp = req.badRequest("can't stop file with none status");
                        break;
                    case UPLOADING:
                        Map<Identifier, KAddress> partners = new HashMap<>();
                        TorrentInfo torrentInfo = new TorrentInfo(TorrentStatus.NONE, partners, 0, 0, 0);
                        libraryContents.put(req.overlayId, Pair.with(elementInfo.getValue0(), torrentInfo));
                        resp = req.success();
                        break;
                    case DOWNLOADING:
                        libraryContents.remove(req.overlayId);
                        resp = req.success();
                        break;
                    default:
                        resp = req.badRequest("missing logic");
                }
            } else {
                resp = req.badRequest("no such file in library");
            }
            LOG.info("{}answering:{}", logPrefix, resp);
            answer(req, resp);
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
        
        public Init(ExtPort extPorts) {
            this.extPorts = extPorts;
        }
    }
}
