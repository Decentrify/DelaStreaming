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
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.javatuples.Pair;
import org.javatuples.Quartet;
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
import se.sics.ktoolbox.croupier.CroupierPort;
import se.sics.ktoolbox.overlaymngr.OverlayMngrPort;
import se.sics.ktoolbox.overlaymngr.events.OMngrCroupier;
import se.sics.ktoolbox.util.identifiable.Identifier;
import se.sics.ktoolbox.util.managedStore.FileMngr;
import se.sics.ktoolbox.util.managedStore.HashMngr;
import se.sics.ktoolbox.util.managedStore.HashUtil;
import se.sics.ktoolbox.util.managedStore.StorageMngrFactory;
import se.sics.ktoolbox.util.network.KAddress;
import se.sics.ktoolbox.util.network.ports.One2NChannel;
import se.sics.ktoolbox.util.idextractor.MsgOverlayIdExtractor;
import se.sics.ktoolbox.util.idextractor.EventOverlayIdExtractor;
import se.sics.ktoolbox.util.overlays.view.OverlayViewUpdatePort;
import se.sics.ktoolbox.videostream.VideoStreamManager;
import se.sics.ktoolbox.videostream.VideoStreamMngrImpl;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class VoDComp extends ComponentDefinition {

    //TODO ALEX fix resume. for the moment everyone who joins starts download from 0
    public static int noDownloadResume = 0;

    private static final Logger LOG = LoggerFactory.getLogger(VoDComp.class);
    private final String logPrefix;
    //*****************************CONNECTIONS**********************************
    //CONNECT TO
    //external required ports
    private final Positive<OverlayMngrPort> omngrPort = requires(OverlayMngrPort.class);
    private final Positive<VoDCaracalClientPort> caracalClientPort = requires(VoDCaracalClientPort.class);
    //external providing ports        
    private final Negative<VoDPort> myPort = provides(VoDPort.class);
    private final Negative<UtilityUpdatePort> utilityUpdate = provides(UtilityUpdatePort.class);
    
    //************************INTERNAL_NO_CONNECTION****************************
    private One2NChannel<Network> networkEnd;
    private One2NChannel<CroupierPort> croupierEnd;
    private One2NChannel<OverlayViewUpdatePort> viewUpdateEnd;
    //*****************************CONFIGURATION********************************
    private final VoDKCWrapper vodConfig;
    //***************************INTERNAL_STATE*********************************
    private final ManagedState selfState;
    //***************************EXTERNAL_STATE*********************************
    private final KAddress selfAdr;
    private final ExtPort extPorts;
    //******************************AUX_STATE***********************************
    //overlayId, <vodPeerReqId, fileName, videoStreamMngr>
    private final Map<Identifier, Triplet<Identifier, String, VideoStreamManager>> pendingVideoComp = new HashMap<>();

    public VoDComp(Init init) {
        vodConfig = new VoDKCWrapper(config());
        selfAdr = init.selfAdr;
        logPrefix = "<nid:" + selfAdr.getId() + ">";
        LOG.info("{}lib folder: {}", logPrefix, vodConfig.videoLibrary);

        selfState = new ManagedState(vodConfig.videoLibrary);
        extPorts = init.extPorts;
        networkEnd = One2NChannel.getChannel("vod", extPorts.networkPort, new MsgOverlayIdExtractor());
        croupierEnd = One2NChannel.getChannel("vod", extPorts.croupierPort, new EventOverlayIdExtractor());
        viewUpdateEnd = One2NChannel.getChannel("vod", extPorts.viewUpdatePort, new EventOverlayIdExtractor());

        subscribe(handleStart, control);
        subscribe(handleGetLibraryRequest, myPort);
        subscribe(handleUploadVideoRequest, myPort);
        subscribe(handleDownloadVideoRequest, myPort);
        subscribe(handleCroupierConnected, omngrPort);
        subscribe(handleAddOverlayResponse, caracalClientPort);
        subscribe(handleJoinOverlayResponse, caracalClientPort);
    }

    private Handler handleStart = new Handler<Start>() {

        @Override
        public void handle(Start event) {
            LOG.info("{} starting", logPrefix);
            startUploading();
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

                LOG.info("{}joining upload - fileName:{} overlay:{}", new Object[]{logPrefix, fileName, overlayId});
                CCJoinOverlay.Request req = new CCJoinOverlay.Request(overlayId);
                trigger(req, caracalClientPort);
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
            String videoFilePath = vodConfig.videoLibrary + File.separator + req.videoName;
            String hashFilePath = vodConfig.videoLibrary + File.separator + videoNameNoExt + ".hash";

            File videoFile = new File(videoFilePath);
            File hashFile = new File(hashFilePath);
            if (hashFile.exists()) {
                hashFile.delete();
            }
            DownloadMngrKCWrapper downloadConfig = new DownloadMngrKCWrapper(config());
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
            trigger(new CCAddOverlay.Request(req.id, req.overlayId, fileMeta), caracalClientPort);
            selfState.pendingUploads.put(req.id, Pair.with(Pair.with(req.videoName, req.overlayId), fileMeta));
        }
    };

    public Handler<CCAddOverlay.Response> handleAddOverlayResponse = new Handler<CCAddOverlay.Response>() {

        @Override
        public void handle(CCAddOverlay.Response resp) {
            LOG.trace("{} - {}", new Object[]{logPrefix, resp});

            Pair<Pair<String, Identifier>, FileMetadata> fileAux = selfState.pendingUploads.remove(resp.id);
            Identifier overlayId = fileAux.getValue0().getValue1();
            FileMetadata fileMeta = fileAux.getValue1();
            if (resp.status == ReqStatus.SUCCESS) {
                if (!selfState.libMngr.upload(fileMeta.fileName, overlayId)) {
                    LOG.error("{} library manager - upload denied for file:{}", logPrefix, fileMeta.fileName);
                    throw new RuntimeException("library manager - upload denied for file:" + fileMeta.fileName);
                }
                startUpload(resp.id, overlayId, fileMeta);
            } else {
                LOG.error("{} error in response message of upload video:{}", logPrefix, fileMeta.fileName);
//                throw new RuntimeException("error in response message of upload video:" + fileInfo.getValue0().getValue0());
                System.exit(1);
            }
        }
    };

    public Handler<DownloadVideo.Request> handleDownloadVideoRequest = new Handler<DownloadVideo.Request>() {

        @Override
        public void handle(DownloadVideo.Request req) {
            LOG.info("{} - {} - videoName:{} overlay:{}", new Object[]{logPrefix, req, req.videoName, req.overlayId});
            trigger(new CCJoinOverlay.Request(req.id, req.overlayId), caracalClientPort);
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
            LOG.trace("{} - {}", new Object[]{logPrefix, resp});

            if (resp.status == ReqStatus.SUCCESS) {
                if (selfState.pendingDownloads.containsKey(resp.id)) {
                    selfState.pendingDownloads.put(resp.id, Pair.with(Pair.with(resp.fileMeta.fileName, resp.overlayId), resp.fileMeta));
                    startDownload(resp.id, resp.overlayId, resp.fileMeta);
                } else if (selfState.rejoinUploads.containsKey(resp.id)) {
                    selfState.rejoinUploads.remove(resp.id);
                    startUpload(resp.id, resp.overlayId, resp.fileMeta);
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

    private void startUpload(Identifier vodReqId, Identifier overlayId, FileMetadata fileMeta) {
        try {
            Pair<FileMngr, HashMngr> videoMngrs = getUploadVideoMngrs(fileMeta.fileName, fileMeta);
            startVideoComp(vodReqId, overlayId, fileMeta, videoMngrs, false);
            trigger(new GetLibrary.Indication(ResponseStatus.SUCCESS, selfState.libMngr.getLibrary()), myPort);
        } catch (IOException ex) {
            LOG.error("{} error writting to disk for video:{}", logPrefix, fileMeta.fileName);
            throw new RuntimeException("error writting to disk for video:" + fileMeta.fileName, ex);
        }
    }

    private void startDownload(Identifier vodReqId, Identifier overlayId, FileMetadata fileMeta) {
        try {
            if (!selfState.libMngr.startDownload(fileMeta.fileName, overlayId)) {
                LOG.error("{} library manager - download denied for file:{}", logPrefix, fileMeta.fileName);
                throw new RuntimeException("library manager - download denied for file:" + fileMeta.fileName);
            }
            Pair<FileMngr, HashMngr> videoMngrs = getDownloadVideoMngrs(fileMeta.fileName, fileMeta);
            startVideoComp(vodReqId, overlayId, fileMeta, videoMngrs, true);
            trigger(new GetLibrary.Indication(ResponseStatus.SUCCESS, selfState.libMngr.getLibrary()), myPort);
        } catch (IOException ex) {
            LOG.error("{} error writting to disk for video:{}", logPrefix, fileMeta.fileName);
            throw new RuntimeException("error writting to disk for video:" + fileMeta.fileName, ex);
        } catch (HashUtil.HashBuilderException ex) {
            LOG.error("{} error creating hash file for video:{}", logPrefix, fileMeta.fileName);
            throw new RuntimeException("error creating hash file for video:" + fileMeta.fileName, ex);
        }
    }

    private void startVideoComp(Identifier vodReqId, Identifier overlayId, FileMetadata fileMeta, 
            Pair<FileMngr, HashMngr> hashedFileMngr, boolean download) {
        LOG.info("{}videoName:{}, videoFileSize:{}, hashFileSize:{}", new Object[]{logPrefix,
            fileMeta.fileName, fileMeta.fileSize, fileMeta.hashFileSize});
        DownloadMngrKCWrapper downloadMngrConfig = new DownloadMngrKCWrapper(config());
        ConnMngrKCWrapper connMngrConfig = new ConnMngrKCWrapper(config());

        AtomicInteger playPos = new AtomicInteger(0);
        
        connectVideoComponents(overlayId, download, playPos, hashedFileMngr);
        trigger(new OMngrCroupier.ConnectRequest(overlayId, false), omngrPort);
        
        VideoStreamManager vsMngr = new VideoStreamMngrImpl(hashedFileMngr.getValue0(), fileMeta.pieceSize, (long) fileMeta.fileSize, playPos);
        pendingVideoComp.put(overlayId, Triplet.with(vodReqId, fileMeta.fileName, vsMngr));
        
    }
    
    private Handler handleCroupierConnected = new Handler<OMngrCroupier.ConnectResponse>() {
        @Override
        public void handle(OMngrCroupier.ConnectResponse resp) {
            LOG.info("{}croupier:{} connected", new Object[]{logPrefix, resp.req.croupierId});
            Triplet<Identifier, String, VideoStreamManager> videoAuxState = pendingVideoComp.remove(resp.req.croupierId);
            Quartet<Component, Channel[], Component, Channel[]> comp = selfState.videoComps.get(resp.req.croupierId);
            if(videoAuxState == null || comp == null) {
                throw new RuntimeException("vod comp bad state");
            }
            trigger(Start.event, comp.getValue0().control());
            trigger(Start.event, comp.getValue2().control());
            trigger(new PlayReady(videoAuxState.getValue0(), videoAuxState.getValue1(), resp.req.croupierId, videoAuxState.getValue2()), myPort);
        }
    };

    private void connectVideoComponents(Identifier overlayId, boolean download, AtomicInteger playPos, 
            Pair<FileMngr, HashMngr> hashedFileMngr) {
        Component connMngrComp = create(ConnMngrComp.class, new ConnMngrComp.Init(selfAdr, overlayId));
        Channel[] connMngrChannels = new Channel[1];
        connMngrChannels[0] = connect(connMngrComp.getNegative(Timer.class), extPorts.timerPort, Channel.TWO_WAY);
        networkEnd.addChannel(overlayId, connMngrComp.getNegative(Network.class));
        croupierEnd.addChannel(overlayId, connMngrComp.getNegative(CroupierPort.class));
        viewUpdateEnd.addChannel(overlayId, connMngrComp.getPositive(OverlayViewUpdatePort.class));

        Component dMngrComp = create(DownloadMngrComp.class, new DownloadMngrComp.Init(overlayId, 
                hashedFileMngr.getValue0(), hashedFileMngr.getValue1(), download, playPos));
        Channel[] dMngrChannels = new Channel[4];
        dMngrChannels[0] = connect(dMngrComp.getNegative(Timer.class), extPorts.timerPort, Channel.TWO_WAY);
        dMngrChannels[1] = connect(dMngrComp.getNegative(ConnMngrPort.class), connMngrComp.getPositive(ConnMngrPort.class), Channel.TWO_WAY);
        dMngrChannels[2] = connect(dMngrComp.getPositive(UtilityUpdatePort.class), connMngrComp.getNegative(UtilityUpdatePort.class), Channel.TWO_WAY);
        dMngrChannels[3] = connect(dMngrComp.getPositive(UtilityUpdatePort.class), utilityUpdate, Channel.TWO_WAY);
        //downloadmngr port not connected yet - connect to something

        selfState.videoComps.put(overlayId, Quartet.with(connMngrComp, connMngrChannels, dMngrComp, dMngrChannels));
    }

    private Pair<FileMngr, HashMngr> getUploadVideoMngrs(String video, FileMetadata fileMeta) throws IOException {

        String videoNoExt = LibraryUtil.removeExtension(video);
        String videoFilePath = vodConfig.videoLibrary + File.separator + video;
        String hashFilePath = vodConfig.videoLibrary + File.separator + videoNoExt + ".hash";

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
        DownloadMngrKCWrapper downloadConfig = new DownloadMngrKCWrapper(config());
        int blockSize = downloadConfig.piecesPerBlock * downloadConfig.pieceSize;
        FileMngr fileMngr = StorageMngrFactory.getCompleteFileMngr(videoFilePath, fileMeta.fileSize, blockSize,
                downloadConfig.pieceSize);

        return Pair.with(fileMngr, hashMngr);
    }

    private Pair<FileMngr, HashMngr> getDownloadVideoMngrs(String video, FileMetadata fileMeta) throws IOException, HashUtil.HashBuilderException {
        LOG.info("{}lib directory {}", logPrefix, vodConfig.videoLibrary);
        String videoNoExt = LibraryUtil.removeExtension(video);
        String videoFilePath = vodConfig.videoLibrary + File.separator + video;
        String hashFilePath = vodConfig.videoLibrary + File.separator + videoNoExt + ".hash";

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
        DownloadMngrKCWrapper downloadConfig = new DownloadMngrKCWrapper(config());
        int blockSize = downloadConfig.piecesPerBlock * downloadConfig.pieceSize;
        FileMngr fileMngr = StorageMngrFactory.getIncompleteFileMngr(videoFilePath, fileMeta.fileSize, blockSize,
                downloadConfig.pieceSize);

        return Pair.with(fileMngr, hashMngr);
    }

    public static class Init extends se.sics.kompics.Init<VoDComp> {

        public final KAddress selfAdr;
        public final ExtPort extPorts;

        public Init(KAddress selfAdr, ExtPort extPorts) {
            this.selfAdr = selfAdr;
            this.extPorts = extPorts;
        }
    }

    public static class ExtPort {

        public final Positive<Timer> timerPort;
        //network ports
        public final Positive<Network> networkPort;
        //overlay ports
        public final Positive<CroupierPort> croupierPort;
        public final Negative<OverlayViewUpdatePort> viewUpdatePort;

        public ExtPort(Positive<Timer> timerPort, Positive<Network> networkPort,
                Positive<CroupierPort> croupierPort, Negative<OverlayViewUpdatePort> viewUpdatePort) {
            this.timerPort = timerPort;
            this.networkPort = networkPort;
            this.croupierPort = croupierPort;
            this.viewUpdatePort = viewUpdatePort;
        }
    }

    private class ManagedState {

        //<overlayId, components>

        final Map<Identifier, Quartet<Component, Channel[], Component, Channel[]>> videoComps = new HashMap<>();
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
}
