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
package se.sics.gvod.core;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.javatuples.Pair;
import org.javatuples.Triplet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.gvod.cc.VoDCaracalClientPort;
import se.sics.gvod.cc.event.CCAddOverlay;
import se.sics.gvod.cc.event.CCJoinOverlay;
import se.sics.gvod.common.event.ReqStatus;
import se.sics.gvod.common.util.FileMetadata;
import se.sics.gvod.common.utility.UtilityUpdatePort;
import se.sics.gvod.core.connMngr.ConnMngrComp;
import se.sics.gvod.core.downloadMngr.DownloadMngrComp;
import se.sics.gvod.core.downloadMngr.DownloadMngrKCWrapper;
import se.sics.gvod.core.connMngr.ConnMngrKCWrapper;
import se.sics.gvod.core.connMngr.ConnMngrPort;
import se.sics.gvod.core.event.DownloadVideo;
import se.sics.gvod.core.event.GetLibrary;
import se.sics.gvod.core.event.PlayReady;
import se.sics.gvod.core.event.UploadVideo;
import se.sics.gvod.core.libraryMngr.LibraryMngr;
import se.sics.gvod.core.libraryMngr.LibraryUtil;
import se.sics.gvod.core.util.FileStatus;
import se.sics.gvod.core.util.ResponseStatus;
import se.sics.kompics.Channel;
import se.sics.kompics.Component;
import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.Handler;
import se.sics.kompics.Negative;
import se.sics.kompics.Positive;
import se.sics.kompics.Start;
import se.sics.kompics.network.Network;
import se.sics.kompics.timer.Timer;
import se.sics.ktoolbox.cc.heartbeat.CCHeartbeatPort;
import se.sics.ktoolbox.cc.heartbeat.event.CCHeartbeat;
import se.sics.ktoolbox.cc.heartbeat.event.CCOverlaySample;
import se.sics.ktoolbox.croupier.CroupierComp;
import se.sics.ktoolbox.croupier.CroupierControlPort;
import se.sics.ktoolbox.croupier.CroupierPort;
import se.sics.ktoolbox.croupier.event.CroupierJoin;
import se.sics.ktoolbox.util.address.AddressUpdate;
import se.sics.ktoolbox.util.address.AddressUpdatePort;
import se.sics.ktoolbox.util.identifiable.Identifier;
import se.sics.ktoolbox.util.managedStore.FileMngr;
import se.sics.ktoolbox.util.managedStore.HashMngr;
import se.sics.ktoolbox.util.managedStore.HashUtil;
import se.sics.ktoolbox.util.managedStore.StorageMngrFactory;
import se.sics.ktoolbox.util.network.KAddress;
import se.sics.ktoolbox.util.network.nat.NatAwareAddress;
import se.sics.ktoolbox.util.selectors.OverlaySelector;
import se.sics.ktoolbox.util.update.view.ViewUpdatePort;
import se.sics.ktoolbox.videostream.VideoStreamManager;
import se.sics.ktoolbox.videostream.VideoStreamMngrImpl;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class VoDComp extends ComponentDefinition {

    //TODO ALEX fix resume. for the moment everyone who joins starts download from 0
    public static int noDownloadResume = 0;

    private static final Logger LOG = LoggerFactory.getLogger(VoDComp.class);

    Negative<UtilityUpdatePort> utilityUpdate = provides(UtilityUpdatePort.class);
    Negative<VoDPort> myPort = provides(VoDPort.class);
    Positive<Timer> timer = requires(Timer.class);
    Positive<Network> network = requires(Network.class);
    Positive<VoDCaracalClientPort> caracalClient = requires(VoDCaracalClientPort.class);
    Positive<CCHeartbeatPort> heartbeat = requires(CCHeartbeatPort.class);
    Positive<AddressUpdatePort> addressUpdate = requires(AddressUpdatePort.class);

    private final String logPrefix;

    private final VoDKCWrapper config;
    private KAddress self;
    private final ManagedState selfState;

    private class ManagedState {
        //<overlayId, components>
        final Map<Identifier, Triplet<Component, Component, Component>> videoComps = new HashMap<>();
        //<reqId, <<fileName, overlayId>, fileMeta>
        final Map<Identifier, Pair<Pair<String, Identifier>, FileMetadata>> pendingUploads = new HashMap<>();
        //<reqId, <<fileName, overlayId>, fileMeta>
        final Map<Identifier, Pair<Pair<String, Identifier>, FileMetadata>> pendingDownloads = new HashMap<>();
        //<reqId, <fileName, overlayId>>
        final Map<Identifier, Pair<String, Identifier>> rejoinUploads = new HashMap<>();
        private final LibraryMngr libMngr;
        
        ManagedState(String libFolder) {
            libMngr = new LibraryMngr(libFolder);
            libMngr.loadLibrary();
        }
    }

    public VoDComp(VoDInit init) {
        config = init.config;
        self = init.config.self;
        logPrefix = self.getId() + " ";
        LOG.info("{} lib folder: {}", logPrefix, config.videoLibrary);
        selfState = new ManagedState(config.videoLibrary);
        
        subscribe(handleStart, control);
        subscribe(handleGetLibraryRequest, myPort);
        subscribe(handleUploadVideoRequest, myPort);
        subscribe(handleDownloadVideoRequest, myPort);
        subscribe(handleAddOverlayResponse, caracalClient);
        subscribe(handleJoinOverlayResponse, caracalClient);
        subscribe(handleOverlaySample, heartbeat);
        subscribe(handleSelfAddressUpdate, addressUpdate);
    }

    private Handler handleStart = new Handler<Start>() {

        @Override
        public void handle(Start event) {
            LOG.info("{} starting", logPrefix);
            startUploading();
        }
    };

    Handler handleSelfAddressUpdate = new Handler<AddressUpdate.Indication>() {
        @Override
        public void handle(AddressUpdate.Indication update) {
            LOG.info("{}updating self address:{}", logPrefix, update.localAddress);
            self = update.localAddress;
        }
    };

    private void startUploading() {
        Map<String, Pair<FileStatus, Identifier>> fileStatusMap = selfState.libMngr.getLibrary();

        for (Map.Entry<String, Pair<FileStatus, Identifier>> e : fileStatusMap.entrySet()) {
            if (e.getValue().getValue0().equals(FileStatus.UPLOADING)) {
                String fileName = e.getKey();
                Identifier overlayId = e.getValue().getValue1();
                if (overlayId == null) {
                    LOG.error("{} unexpected null overlayId for video:{}", logPrefix, fileName);
//                    throw new RuntimeException("unexpected null overlayId for video:" + fileName);
                    System.exit(1);
                }

                LOG.info("{} - joining upload - fileName:{} overlay:{}", new Object[]{logPrefix, fileName, overlayId});
                CCJoinOverlay.Request req = new CCJoinOverlay.Request(overlayId);
                trigger(req, caracalClient);
                selfState.rejoinUploads.put(req.getId(), Pair.with(fileName, overlayId));
            }
        }
    }

    public Handler handleGetLibraryRequest = new Handler<GetLibrary.Request>() {

        @Override
        public void handle(GetLibrary.Request req) {
            LOG.trace("{} received get library request", logPrefix);
            selfState.libMngr.reloadLibrary();
            trigger(req.answer(ResponseStatus.SUCCESS, selfState.libMngr.getLibrary()), myPort);
        }

    };

    public Handler<UploadVideo.Request> handleUploadVideoRequest = new Handler<UploadVideo.Request>() {

        @Override
        public void handle(UploadVideo.Request req) {
            LOG.info("{} - upload videoName:{} overlay:{}", new Object[]{logPrefix, req.videoName, req.overlayId});
            String videoNameNoExt = LibraryUtil.removeExtension(req.videoName);
            String videoFilePath = config.videoLibrary + File.separator + req.videoName;
            String hashFilePath = config.videoLibrary + File.separator + videoNameNoExt + ".hash";

            File videoFile = new File(videoFilePath);
            File hashFile = new File(hashFilePath);
            if (hashFile.exists()) {
                hashFile.delete();
            }
            DownloadMngrKCWrapper downloadConfig = config.getDownloadMngrConfig(null);
            try {
                hashFile.createNewFile();
                int blockSize = downloadConfig.piecesPerBlock * downloadConfig.pieceSize;
                HashUtil.makeHashes(videoFilePath, hashFilePath, downloadConfig.hashAlg, blockSize);
                if (!selfState.libMngr.pendingUpload(req.videoName)) {
                    LOG.error("library manager - pending upload denied for file:{}", req.videoName);
//                    throw new RuntimeException("library manager - pending upload denied for file:" + req.videoName);
                    System.exit(1);
                }
            } catch (HashUtil.HashBuilderException ex) {
                LOG.error("error while hashing file:{}", req.videoName);
//                throw new RuntimeException("error while hashing file:" + req.videoName, ex);
                System.exit(1);
            } catch (IOException ex) {
                LOG.error("error writting hash file:{} to disk", req.videoName);
//                throw new RuntimeException("error writting hash file:" + req.videoName + " to disk", ex);
                System.exit(1);
            }
            FileMetadata fileMeta = new FileMetadata(req.videoName, (int) videoFile.length(), downloadConfig.pieceSize,
                    downloadConfig.hashAlg, (int) hashFile.length());
            trigger(new CCAddOverlay.Request(req.id, req.overlayId, fileMeta), caracalClient);
            selfState.pendingUploads.put(req.id, Pair.with(Pair.with(req.videoName, req.overlayId), fileMeta));
        }
    };

    public Handler<CCAddOverlay.Response> handleAddOverlayResponse = new Handler<CCAddOverlay.Response>() {

        @Override
        public void handle(CCAddOverlay.Response resp) {
            LOG.trace("{} - {}", new Object[]{logPrefix, resp});

            Pair<Pair<String, Identifier>, FileMetadata> fileInfo = selfState.pendingUploads.remove(resp.id);
            String fileName = fileInfo.getValue0().getValue0();
            Identifier overlayId = fileInfo.getValue0().getValue1();
            if (resp.status == ReqStatus.SUCCESS) {
                if (!selfState.libMngr.upload(fileName, overlayId)) {
                    LOG.error("{} library manager - upload denied for file:{}", logPrefix, fileName);
                    throw new RuntimeException("library manager - upload denied for file:" + fileName);
                }
                startUpload(resp.id, fileInfo.getValue0().getValue0(), fileInfo.getValue0().getValue1(), fileInfo.getValue1());
                trigger(new CCHeartbeat.Start(overlayId), heartbeat);
            } else {
                LOG.error("{} error in response message of upload video:{}", logPrefix, fileInfo.getValue0().getValue0());
//                throw new RuntimeException("error in response message of upload video:" + fileInfo.getValue0().getValue0());
                System.exit(1);
            }
        }
    };

    public Handler<DownloadVideo.Request> handleDownloadVideoRequest = new Handler<DownloadVideo.Request>() {

        @Override
        public void handle(DownloadVideo.Request req) {
            LOG.info("{} - {} - videoName:{} overlay:{}", new Object[]{logPrefix, req, req.videoName, req.overlayId});
            trigger(new CCJoinOverlay.Request(req.id, req.overlayId), caracalClient);
            selfState.pendingDownloads.put(req.id, Pair.with(Pair.with(req.videoName, req.overlayId), (FileMetadata) null));
            if (!selfState.libMngr.pendingDownload(req.videoName)) {
                LOG.error("{} library manager - pending download denied for file:{}", logPrefix, req.videoName);
                throw new RuntimeException("library manager - pending download denied for file:" + req.videoName);
            }
        }
    };

    public Handler<CCJoinOverlay.Response> handleJoinOverlayResponse = new Handler<CCJoinOverlay.Response>() {

        @Override
        public void handle(CCJoinOverlay.Response resp) {
            LOG.trace("{} - {}", new Object[]{config.self, resp});

            if (resp.status == ReqStatus.SUCCESS) {
                if (selfState.pendingDownloads.containsKey(resp.id)) {

                    selfState.pendingDownloads.put(resp.id, Pair.with(Pair.with(resp.fileMeta.fileName, resp.overlayId), resp.fileMeta));
                    trigger(new CCOverlaySample.Request(resp.overlayId), heartbeat);
                } else if (selfState.rejoinUploads.containsKey(resp.id)) {
                    Pair<String, Identifier> fileInfo = selfState.rejoinUploads.remove(resp.id);
                    startUpload(resp.id, fileInfo.getValue0(), fileInfo.getValue1(), resp.fileMeta);
                }
            } else {
                //TODO Alex - keep track of fileMeta so that in case caracal does not have it, we can re-upload it.
                if (resp.fileMeta != null) {
                    LOG.error("{} error in response message of upload video:{}", logPrefix, resp.fileMeta.fileName);
                    throw new RuntimeException("error in response message of upload video:" + resp.fileMeta.fileName);
                } else {
                    LOG.error("{} error in response message of upload video", logPrefix);
                    throw new RuntimeException("error in response message of upload video");
                }
            }
        }
    };

    Handler handleOverlaySample = new Handler<CCOverlaySample.Response>() {

        @Override
        public void handle(CCOverlaySample.Response resp) {
            LOG.trace("{} - {}", new Object[]{config.self, resp});

            Identifier overlayId = resp.req.overlayId;
            Iterator<Map.Entry<Identifier, Pair<Pair<String, Identifier>, FileMetadata>>> it = selfState.pendingDownloads.entrySet().iterator();
            while (it.hasNext()) {
                Map.Entry<Identifier, Pair<Pair<String, Identifier>, FileMetadata>> pd = it.next();
                if (pd.getValue().getValue0().getValue1().equals(overlayId)) {
                    it.remove();
                    startDownload(pd.getKey(), pd.getValue().getValue0().getValue0(), overlayId,
                            pd.getValue().getValue1(), new ArrayList<KAddress>(resp.overlaySample));
                }
            }
        }

    };

    private void startUpload(Identifier reqId, String fileName, Identifier overlayId, FileMetadata fileMeta) {
        try {
            Pair<FileMngr, HashMngr> videoMngrs = getUploadVideoMngrs(fileName, fileMeta);
            startVideoComp(reqId, overlayId, fileMeta, videoMngrs, false, new ArrayList<KAddress>());
            trigger(new GetLibrary.Indication(ResponseStatus.SUCCESS, selfState.libMngr.getLibrary()), myPort);
        } catch (IOException ex) {
            LOG.error("{} error writting to disk for video:{}", logPrefix, fileName);
            throw new RuntimeException("error writting to disk for video:" + fileName, ex);
        }
    }

    private void startDownload(Identifier reqId, String fileName, Identifier overlayId, FileMetadata fileMeta, List<KAddress> bootstrap) {
        try {
            if (!selfState.libMngr.startDownload(fileName, overlayId)) {
                LOG.error("{} library manager - download denied for file:{}", logPrefix, fileName);
                throw new RuntimeException("library manager - download denied for file:" + fileName);
            }
            Pair<FileMngr, HashMngr> videoMngrs = getDownloadVideoMngrs(fileName, fileMeta);
            startVideoComp(reqId, overlayId, fileMeta, videoMngrs, true, bootstrap);
            trigger(new GetLibrary.Indication(ResponseStatus.SUCCESS, selfState.libMngr.getLibrary()), myPort);
        } catch (IOException ex) {
            LOG.error("{} error writting to disk for video:{}", logPrefix, fileName);
            throw new RuntimeException("error writting to disk for video:" + fileName, ex);
        } catch (HashUtil.HashBuilderException ex) {
            LOG.error("{} error creating hash file for video:{}", logPrefix, fileName);
            throw new RuntimeException("error creating hash file for video:" + fileName, ex);
        }
    }

    private void startVideoComp(Identifier reqId, Identifier overlayId, FileMetadata fileMeta, Pair<FileMngr, HashMngr> hashedFileMngr,
            boolean download, List<KAddress> croupierBootstrap) {
        int hashSize = 0;
        hashSize = HashUtil.getHashSize(fileMeta.hashAlg);
        LOG.info("{} - videoName:{} videoFileSize:{}, hashFileSize:{}, hashSize:{}", new Object[]{config.self,
            fileMeta.fileName, fileMeta.fileSize, fileMeta.hashFileSize, hashSize});
        DownloadMngrKCWrapper downloadMngrConfig = null;
        ConnMngrKCWrapper connMngrConfig = null;

        downloadMngrConfig = config.getDownloadMngrConfig(overlayId);
        connMngrConfig = config.getConnMngrConfig(overlayId);

        AtomicInteger playPos = new AtomicInteger(0);
        Component croupier = connectVideoOverlayCroupier(overlayId, croupierBootstrap);
        Component downloadMngr = connectDownloadMngr(downloadMngrConfig, croupier, hashedFileMngr, download, playPos);
        Component connMngr = connectConnMngr(overlayId, connMngrConfig, croupier, downloadMngr);
        selfState.videoComps.put(overlayId, Triplet.with(connMngr, downloadMngr, croupier));

        VideoStreamManager vsMngr = null;
        try {
            vsMngr = new VideoStreamMngrImpl(hashedFileMngr.getValue0(), fileMeta.pieceSize, (long) fileMeta.fileSize, playPos);
        } catch (IOException ex) {
            LOG.error("{} IOException trying to read video:{}", logPrefix, fileMeta.fileName);
            throw new RuntimeException("IOException trying to read video:{}" + fileMeta.fileName, ex);
        }
        trigger(new CCHeartbeat.Start(overlayId), heartbeat);
        trigger(new PlayReady(reqId, fileMeta.fileName, overlayId, vsMngr), myPort);
    }

    private Component connectVideoOverlayCroupier(Identifier overlayId, List<KAddress> croupierBootstrap) {
        Component croupier = create(CroupierComp.class, new CroupierComp.CroupierInit(overlayId, (NatAwareAddress) config.self));
        connect(croupier.getNegative(Timer.class), timer, Channel.TWO_WAY);
        connect(croupier.getNegative(Network.class), network, new OverlaySelector(overlayId, true), Channel.TWO_WAY);
        connect(croupier.getNegative(AddressUpdatePort.class), addressUpdate, Channel.TWO_WAY);

        trigger(Start.event, croupier.control());
        trigger(new CroupierJoin(croupierBootstrap), croupier.getPositive(CroupierControlPort.class));
        return croupier;
    }

    private Component connectDownloadMngr(DownloadMngrKCWrapper downloadMngrConfig, Component croupierVideoOverlay,
            Pair<FileMngr, HashMngr> hashedFileMngr, boolean download, AtomicInteger playPos) {
        Component downloadMngr = create(DownloadMngrComp.class, new DownloadMngrComp.DownloadMngrInit(
                downloadMngrConfig, hashedFileMngr.getValue0(), hashedFileMngr.getValue1(), download, playPos));
        connect(downloadMngr.getNegative(Timer.class), timer, Channel.TWO_WAY);
        connect(downloadMngr.getPositive(UtilityUpdatePort.class), utilityUpdate, Channel.TWO_WAY);
        //TODO Alex connect croupierPort to downloadUpdate;
        trigger(Start.event, downloadMngr.control());
        return downloadMngr;
    }

    private Component connectConnMngr(Identifier overlayId, ConnMngrKCWrapper connMngrConfig, Component croupierVideoOverlay,
            Component downloadMngr) {
        Component connMngr = create(ConnMngrComp.class, new ConnMngrComp.ConnMngrInit(connMngrConfig));
        connect(connMngr.getNegative(Network.class), network, new OverlaySelector(overlayId, true), Channel.TWO_WAY);
        connect(connMngr.getNegative(Timer.class), timer, Channel.TWO_WAY);
        connect(connMngr.getNegative(AddressUpdatePort.class), addressUpdate, Channel.TWO_WAY);

        connect(connMngr.getNegative(CroupierPort.class), croupierVideoOverlay.getPositive(CroupierPort.class), Channel.TWO_WAY);
        connect(connMngr.getPositive(ViewUpdatePort.class), croupierVideoOverlay.getNegative(ViewUpdatePort.class), Channel.TWO_WAY);

        connect(connMngr.getPositive(ConnMngrPort.class), downloadMngr.getNegative(ConnMngrPort.class), Channel.TWO_WAY);
        connect(connMngr.getNegative(UtilityUpdatePort.class), downloadMngr.getPositive(UtilityUpdatePort.class), Channel.TWO_WAY);

        trigger(Start.event, connMngr.control());
        return connMngr;
    }

    private Pair<FileMngr, HashMngr> getUploadVideoMngrs(String video, FileMetadata fileMeta) throws IOException {

        String videoNoExt = LibraryUtil.removeExtension(video);
        String videoFilePath = config.videoLibrary + File.separator + video;
        String hashFilePath = config.videoLibrary + File.separator + videoNoExt + ".hash";

//        int nrHashPieces = fileMeta.hashFileSize / HashUtil.getHashSize(fileMeta.hashAlg);
//        PieceTracker hashPieceTracker = new CompletePieceTracker(nrHashPieces);
//        Storage hashStorage = StorageFactory.getExistingFile(hashFilePath);
//        HashMngr hashMngr = new CompleteFileMngr(hashStorage, hashPieceTracker);
        HashMngr hashMngr = StorageMngrFactory.getCompleteHashMngr(hashFilePath, fileMeta.hashAlg, fileMeta.hashFileSize,
                HashUtil.getHashSize(fileMeta.hashAlg));

//        int filePieces = fileMeta.fileSize / fileMeta.pieceSize + (fileMeta.fileSize % fileMeta.pieceSize == 0 ? 0 : 1);
//        Storage videoStorage = StorageFactory.getExistingFile(videoFilePath, config.pieceSize);
//        PieceTracker videoPieceTracker = new CompletePieceTracker(filePieces);
//        FileMngr fileMngr = new SimpleFileMngr(videoStorage, videoPieceTracker);
        DownloadMngrKCWrapper downloadConfig = config.getDownloadMngrConfig(null);
        int blockSize = downloadConfig.piecesPerBlock * downloadConfig.pieceSize;
        FileMngr fileMngr = StorageMngrFactory.getCompleteFileMngr(videoFilePath, fileMeta.fileSize, blockSize,
                downloadConfig.pieceSize);

        return Pair.with(fileMngr, hashMngr);
    }

    private Pair<FileMngr, HashMngr> getDownloadVideoMngrs(String video, FileMetadata fileMeta) throws IOException, HashUtil.HashBuilderException {
        LOG.info("{} lib directory {}", config.self, config.videoLibrary);
        String videoNoExt = LibraryUtil.removeExtension(video);
        String videoFilePath = config.videoLibrary + File.separator + video;
        String hashFilePath = config.videoLibrary + File.separator + videoNoExt + ".hash";

        File hashFile = new File(hashFilePath);
        if (hashFile.exists()) {
            hashFile.delete();
        }

//        int hashPieces = fileMeta.hashFileSize / HashUtil.getHashSize(fileMeta.hashAlg);
//        PieceTracker hashPieceTracker = new SimplePieceTracker(hashPieces);
//        Storage hashStorage = StorageFactory.getEmptyFile(hashFilePath, fileMeta.hashFileSize, HashUtil.getHashSize(fileMeta.hashAlg));
//        FileMngr hashMngr = new SimpleFileMngr(hashStorage, hashPieceTracker);
        HashMngr hashMngr = StorageMngrFactory.getIncompleteHashMngr(hashFilePath, fileMeta.hashAlg, fileMeta.hashFileSize,
                HashUtil.getHashSize(fileMeta.hashAlg));

//        Storage videoStorage = StorageFactory.getEmptyFile(videoFilePath, fileMeta.fileSize, fileMeta.pieceSize);
//        int filePieces = fileMeta.fileSize / fileMeta.pieceSize + (fileMeta.fileSize % fileMeta.pieceSize == 0 ? 0 : 1);
//        PieceTracker videoPieceTracker = new SimplePieceTracker(filePieces);
//        FileMngr fileMngr = new SimpleFileMngr(videoStorage, videoPieceTracker);
        DownloadMngrKCWrapper downloadConfig = config.getDownloadMngrConfig(null);
        int blockSize = downloadConfig.piecesPerBlock * downloadConfig.pieceSize;
        FileMngr fileMngr = StorageMngrFactory.getIncompleteFileMngr(videoFilePath, fileMeta.fileSize, blockSize,
                downloadConfig.pieceSize);

        return Pair.with(fileMngr, hashMngr);
    }
}
