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

import com.google.common.primitives.Ints;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import org.javatuples.Pair;
import org.javatuples.Triplet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.gvod.cc.VoDCaracalClientPort;
import se.sics.gvod.cc.msg.CCAddOverlay;
import se.sics.gvod.cc.msg.CCJoinOverlay;
import se.sics.gvod.common.msg.ReqStatus;
import se.sics.gvod.common.util.FileMetadata;
import se.sics.gvod.common.util.GVoDConfigException;
import se.sics.gvod.common.utility.UtilityUpdatePort;
import se.sics.gvod.core.connMngr.ConnMngrComp;
import se.sics.gvod.core.downloadMngr.DownloadMngrComp;
import se.sics.gvod.core.downloadMngr.DownloadMngrConfig;
import se.sics.gvod.core.connMngr.ConnMngrConfig;
import se.sics.gvod.core.connMngr.ConnMngrPort;
import se.sics.gvod.core.libraryMngr.LibraryMngr;
import se.sics.gvod.core.msg.DownloadVideo;
import se.sics.gvod.core.msg.GetLibrary;
import se.sics.gvod.core.msg.PlayReady;
import se.sics.gvod.core.msg.UploadVideo;
import se.sics.gvod.core.util.FileStatus;
import se.sics.gvod.core.util.ResponseStatus;
import se.sics.kompics.Component;
import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.Handler;
import se.sics.kompics.Negative;
import se.sics.kompics.Positive;
import se.sics.kompics.Start;
import se.sics.kompics.network.Network;
import se.sics.kompics.timer.Timer;
import se.sics.ktoolbox.cc.heartbeat.CCHeartbeatPort;
import se.sics.ktoolbox.cc.heartbeat.msg.CCHeartbeat;
import se.sics.ktoolbox.cc.heartbeat.msg.CCOverlaySample;
import se.sics.p2ptoolbox.croupier.CroupierComp;
import se.sics.p2ptoolbox.croupier.CroupierConfig;
import se.sics.p2ptoolbox.croupier.CroupierControlPort;
import se.sics.p2ptoolbox.croupier.CroupierPort;
import se.sics.p2ptoolbox.croupier.msg.CroupierJoin;
import se.sics.p2ptoolbox.util.config.SystemConfig;
import se.sics.p2ptoolbox.util.filters.IntegerOverlayFilter;
import se.sics.p2ptoolbox.util.managedStore.FileMngr;
import se.sics.p2ptoolbox.util.managedStore.HashMngr;
import se.sics.p2ptoolbox.util.managedStore.HashUtil;
import se.sics.p2ptoolbox.util.managedStore.StorageMngrFactory;
import se.sics.p2ptoolbox.util.network.impl.DecoratedAddress;
import se.sics.p2ptoolbox.videostream.VideoStreamManager;
import se.sics.p2ptoolbox.videostream.VideoStreamMngrImpl;

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

    private final String logPrefix;

    private final VoDConfig config;
    private final SystemConfig systemConfig;
    private final CroupierConfig croupierConfig;

    private final Map<Integer, Triplet<Component, Component, Component>> videoComps;

    private final Map<UUID, Pair<Pair<String, Integer>, FileMetadata>> pendingUploads;
    private final Map<UUID, Pair<Pair<String, Integer>, FileMetadata>> pendingDownloads;
    private final Map<UUID, Pair<String, Integer>> rejoinUploads;
    private final LibraryMngr libMngr;

    public VoDComp(VoDInit init) {
        this.config = init.config;
        this.systemConfig = init.systemConfig;
        this.croupierConfig = init.croupierConfig;
        this.logPrefix = config.getSelf().toString();
        LOG.info("{} lib folder: {}", logPrefix, config.getVideoLibrary());
        this.videoComps = new HashMap<Integer, Triplet<Component, Component, Component>>();
        this.pendingDownloads = new HashMap<UUID, Pair<Pair<String, Integer>, FileMetadata>>();
        this.pendingUploads = new HashMap<UUID, Pair<Pair<String, Integer>, FileMetadata>>();
        this.rejoinUploads = new HashMap<UUID, Pair<String, Integer>>();
        this.libMngr = new LibraryMngr(config.getVideoLibrary());
        libMngr.loadLibrary();

        subscribe(handleStart, control);
        subscribe(handleGetLibraryRequest, myPort);
        subscribe(handleUploadVideoRequest, myPort);
        subscribe(handleDownloadVideoRequest, myPort);
        subscribe(handleAddOverlayResponse, caracalClient);
        subscribe(handleJoinOverlayResponse, caracalClient);
        subscribe(handleOverlaySample, heartbeat);
    }

    private Handler handleStart = new Handler<Start>() {

        @Override
        public void handle(Start event) {
            LOG.info("{} starting", logPrefix);
            startUploading();
        }
    };

    private void startUploading() {
        Map<String, Pair<FileStatus, Integer>> fileStatusMap = libMngr.getLibrary();

        for (Map.Entry<String, Pair<FileStatus, Integer>> e : fileStatusMap.entrySet()) {
            if (e.getValue().getValue0().equals(FileStatus.UPLOADING)) {
                String fileName = e.getKey();
                Integer overlayId = e.getValue().getValue1();
                if (overlayId == null) {
                    LOG.error("{} unexpected null overlayId for video:{}", logPrefix, fileName);
//                    throw new RuntimeException("unexpected null overlayId for video:" + fileName);
                    System.exit(1);
                }

                LOG.info("{} - joining upload - fileName:{} overlay:{}", new Object[]{logPrefix, fileName, overlayId});
                CCJoinOverlay.Request req = new CCJoinOverlay.Request(UUID.randomUUID(), overlayId);
                trigger(req, caracalClient);
                rejoinUploads.put(req.id, Pair.with(fileName, overlayId));
            }
        }
    }

    public Handler handleGetLibraryRequest = new Handler<GetLibrary.Request>() {

        @Override
        public void handle(GetLibrary.Request req) {
            LOG.trace("{} received get library request", logPrefix);
            libMngr.reloadLibrary();
            trigger(new GetLibrary.Response(req.reqId, ResponseStatus.SUCCESS, libMngr.getLibrary()), myPort);
        }

    };

    public Handler<UploadVideo.Request> handleUploadVideoRequest = new Handler<UploadVideo.Request>() {

        @Override
        public void handle(UploadVideo.Request req) {
            LOG.info("{} - uploa videoName:{} overlay:{}", new Object[]{logPrefix, req.videoName, req.overlayId});
            String videoNameNoExt = req.videoName.substring(0, req.videoName.indexOf("."));
            String videoFilePath = config.getVideoLibrary() + File.separator + req.videoName;
            String hashFilePath = config.getVideoLibrary() + File.separator + videoNameNoExt + ".hash";

            File videoFile = new File(videoFilePath);
            File hashFile = new File(hashFilePath);
            if (hashFile.exists()) {
                hashFile.delete();
            }
            try {
                hashFile.createNewFile();
                int blockSize = config.getPiecesPerBlock() * config.getPieceSize();
                HashUtil.makeHashes(videoFilePath, hashFilePath, config.getHashAlg(), blockSize);
                if (!libMngr.pendingUpload(req.videoName)) {
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
            FileMetadata fileMeta = new FileMetadata(req.videoName, (int) videoFile.length(), config.getPieceSize(), config.getHashAlg(), (int) hashFile.length());
            trigger(new CCAddOverlay.Request(req.id, req.overlayId, fileMeta), caracalClient);
            pendingUploads.put(req.id, Pair.with(Pair.with(req.videoName, req.overlayId), fileMeta));
        }
    };

    public Handler<CCAddOverlay.Response> handleAddOverlayResponse = new Handler<CCAddOverlay.Response>() {

        @Override
        public void handle(CCAddOverlay.Response resp) {
            LOG.trace("{} - {}", new Object[]{logPrefix, resp});

            Pair<Pair<String, Integer>, FileMetadata> fileInfo = pendingUploads.remove(resp.id);
            String fileName = fileInfo.getValue0().getValue0();
            Integer overlayId = fileInfo.getValue0().getValue1();
            if (resp.status == ReqStatus.SUCCESS) {
                if (!libMngr.upload(fileName, overlayId)) {
                    LOG.error("{} library manager - upload denied for file:{}", logPrefix, fileName);
                    throw new RuntimeException("library manager - upload denied for file:" + fileName);
                }
                startUpload(resp.id, fileInfo.getValue0().getValue0(), fileInfo.getValue0().getValue1(), fileInfo.getValue1());
                trigger(new CCHeartbeat.Start(Ints.toByteArray(overlayId)), heartbeat);
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
            pendingDownloads.put(req.id, Pair.with(Pair.with(req.videoName, req.overlayId), (FileMetadata)null));
            if (!libMngr.pendingDownload(req.videoName)) {
                LOG.error("{} library manager - pending download denied for file:{}", logPrefix, req.videoName);
                throw new RuntimeException("library manager - pending download denied for file:" + req.videoName);
            }
        }
    };

    public Handler<CCJoinOverlay.Response> handleJoinOverlayResponse = new Handler<CCJoinOverlay.Response>() {

        @Override
        public void handle(CCJoinOverlay.Response resp) {
            LOG.trace("{} - {}", new Object[]{config.getSelf(), resp});

            if (resp.status == ReqStatus.SUCCESS) {
                if (pendingDownloads.containsKey(resp.id)) {
                    
                    pendingDownloads.put(resp.id, Pair.with(Pair.with(resp.fileMeta.fileName, resp.overlayId), resp.fileMeta));
                    trigger(new CCOverlaySample.Request(Ints.toByteArray(resp.overlayId)), heartbeat);
                } else if (rejoinUploads.containsKey(resp.id)) {
                    Pair<String, Integer> fileInfo = rejoinUploads.remove(resp.id);
                    startUpload(resp.id, fileInfo.getValue0(), fileInfo.getValue1(), resp.fileMeta);
                }
            } else {
                LOG.error("{} error in response message of upload video:{}", logPrefix, resp.fileMeta.fileName);
                throw new RuntimeException("error in response message of upload video:" + resp.fileMeta.fileName);
            }
        }
    };
    
    Handler handleOverlaySample = new Handler<CCOverlaySample.Response>() {

        @Override
        public void handle(CCOverlaySample.Response resp) {
            LOG.trace("{} - {}", new Object[]{config.getSelf(), resp});

            int overlayId = Ints.fromByteArray(resp.overlayId);
            Iterator<Map.Entry<UUID, Pair<Pair<String, Integer>, FileMetadata>>> it = pendingDownloads. entrySet().iterator();
            while(it.hasNext()) {
                Map.Entry<UUID, Pair<Pair<String, Integer>, FileMetadata>> pd = it.next();
                if(pd.getValue().getValue0().getValue1().equals(overlayId)) {
                    it.remove();
                    startDownload(pd.getKey(), pd.getValue().getValue0().getValue0(), overlayId, pd.getValue().getValue1(), resp.overlaySample);
                }
            }
        }
        
    };

    private void startUpload(UUID reqId, String fileName, Integer overlayId, FileMetadata fileMeta) {
        try {
            Pair<FileMngr, HashMngr> videoMngrs = getUploadVideoMngrs(fileName, fileMeta);
            startVideoComp(reqId, overlayId, fileMeta, videoMngrs, false, new HashSet<DecoratedAddress>());
            trigger(new GetLibrary.Response(UUID.randomUUID(), ResponseStatus.SUCCESS, libMngr.getLibrary()), myPort);
        } catch (IOException ex) {
            LOG.error("{} error writting to disk for video:{}", logPrefix, fileName);
            throw new RuntimeException("error writting to disk for video:" + fileName, ex);
        } catch (GVoDConfigException.Missing ex) {
            LOG.error("{} configuration problem in VoDComp:{}", logPrefix, ex.getMessage());
            throw new RuntimeException(ex);
        }
    }

    private void startDownload(UUID reqId, String fileName, Integer overlayId, FileMetadata fileMeta, Set<DecoratedAddress> bootstrap) {
        try {
            if (!libMngr.startDownload(fileName, overlayId)) {
                LOG.error("{} library manager - download denied for file:{}", logPrefix, fileName);
                throw new RuntimeException("library manager - download denied for file:" + fileName);
            }
            Pair<FileMngr, HashMngr> videoMngrs = getDownloadVideoMngrs(fileName, fileMeta);
            startVideoComp(reqId, overlayId, fileMeta, videoMngrs, true, bootstrap);
            trigger(new GetLibrary.Response(UUID.randomUUID(), ResponseStatus.SUCCESS, libMngr.getLibrary()), myPort);
        } catch (IOException ex) {
            LOG.error("{} error writting to disk for video:{}", logPrefix, fileName);
            throw new RuntimeException("error writting to disk for video:" + fileName, ex);
        } catch (HashUtil.HashBuilderException ex) {
            LOG.error("{} error creating hash file for video:{}", logPrefix, fileName);
            throw new RuntimeException("error creating hash file for video:" + fileName, ex);
        } catch (GVoDConfigException.Missing ex) {
            LOG.error("{} configuration problem in VoDComp:{}", logPrefix, ex.getMessage());
            throw new RuntimeException(ex);
        }
    }

    private void startVideoComp(UUID reqId, int overlayId, FileMetadata fileMeta, Pair<FileMngr, HashMngr> hashedFileMngr, boolean download, Set<DecoratedAddress> bootstrap) {
        int hashSize = 0;
        hashSize = HashUtil.getHashSize(fileMeta.hashAlg);
        LOG.info("{} - videoName:{} videoFileSize:{}, hashFileSize:{}, hashSize:{}", new Object[]{config.getSelf(), fileMeta.fileName, fileMeta.fileSize, fileMeta.hashFileSize, hashSize});
        DownloadMngrConfig downloadMngrConfig = null;
        ConnMngrConfig connMngrConfig = null;

        try {
            downloadMngrConfig = config.getDownloadMngrConfig(overlayId).finalise();
            connMngrConfig = config.getConnMngrConfig(overlayId).finalise();
        } catch (GVoDConfigException.Missing ex) {
            LOG.error("configuration problem in VoDComp " + ex.getMessage());
            System.exit(1);
        }

        Component connMngr = create(ConnMngrComp.class, new ConnMngrComp.ConnMngrInit(connMngrConfig));
        AtomicInteger playPos = new AtomicInteger(0);
        Component downloadMngr = create(DownloadMngrComp.class, new DownloadMngrComp.DownloadMngrInit(downloadMngrConfig, hashedFileMngr.getValue0(), hashedFileMngr.getValue1(), download, playPos));
        Component croupier = create(CroupierComp.class, new CroupierComp.CroupierInit(systemConfig, croupierConfig, overlayId));
        videoComps.put(overlayId, Triplet.with(connMngr, downloadMngr, croupier));

        connect(croupier.getNegative(Timer.class), timer);
        connect(croupier.getNegative(Network.class), network, new IntegerOverlayFilter(overlayId));

        connect(connMngr.getNegative(Network.class), network, new IntegerOverlayFilter(overlayId));
        connect(connMngr.getNegative(Timer.class), timer);
        connect(connMngr.getNegative(CroupierPort.class), croupier.getPositive(CroupierPort.class));

        connect(downloadMngr.getNegative(Timer.class), timer);
        connect(downloadMngr.getNegative(ConnMngrPort.class), connMngr.getPositive(ConnMngrPort.class));

        connect(connMngr.getNegative(UtilityUpdatePort.class), downloadMngr.getPositive(UtilityUpdatePort.class));
        connect(utilityUpdate, downloadMngr.getPositive(UtilityUpdatePort.class));

        trigger(Start.event, croupier.control());
        trigger(Start.event, connMngr.control());
        trigger(Start.event, downloadMngr.control());
        
        trigger(new CroupierJoin(bootstrap), croupier.getPositive(CroupierControlPort.class));
        trigger(new CCHeartbeat.Start(Ints.toByteArray(overlayId)), heartbeat);

        VideoStreamManager vsMngr = null;
        try {
            vsMngr = new VideoStreamMngrImpl(hashedFileMngr.getValue0(), fileMeta.pieceSize, (long) fileMeta.fileSize, playPos);
        } catch (IOException ex) {
            LOG.error("{} IOException trying to read video:{}", logPrefix, fileMeta.fileName);
            throw new RuntimeException("IOException trying to read video:{}" + fileMeta.fileName, ex);
        }
        trigger(new PlayReady(reqId, fileMeta.fileName, overlayId, vsMngr), myPort);
    }

    private Pair<FileMngr, HashMngr> getUploadVideoMngrs(String video, FileMetadata fileMeta) throws IOException, GVoDConfigException.Missing {

        String videoName = video.substring(0, video.indexOf("."));
        String videoExt = video.substring(video.indexOf("."));
        String videoFilePath = config.getVideoLibrary() + File.separator + video;
        String hashFilePath = config.getVideoLibrary() + File.separator + videoName + ".hash";

//        int nrHashPieces = fileMeta.hashFileSize / HashUtil.getHashSize(fileMeta.hashAlg);
//        PieceTracker hashPieceTracker = new CompletePieceTracker(nrHashPieces);
//        Storage hashStorage = StorageFactory.getExistingFile(hashFilePath);
//        HashMngr hashMngr = new CompleteFileMngr(hashStorage, hashPieceTracker);
        HashMngr hashMngr = StorageMngrFactory.getCompleteHashMngr(hashFilePath, fileMeta.hashAlg, fileMeta.hashFileSize, HashUtil.getHashSize(fileMeta.hashAlg));

//        int filePieces = fileMeta.fileSize / fileMeta.pieceSize + (fileMeta.fileSize % fileMeta.pieceSize == 0 ? 0 : 1);
//        Storage videoStorage = StorageFactory.getExistingFile(videoFilePath, config.pieceSize);
//        PieceTracker videoPieceTracker = new CompletePieceTracker(filePieces);
//        FileMngr fileMngr = new SimpleFileMngr(videoStorage, videoPieceTracker);
        int blockSize = config.getPiecesPerBlock() * config.getPieceSize();
        FileMngr fileMngr = StorageMngrFactory.getCompleteFileMngr(videoFilePath, fileMeta.fileSize, blockSize, config.getPieceSize());

        return Pair.with(fileMngr, hashMngr);
    }

    private Pair<FileMngr, HashMngr> getDownloadVideoMngrs(String video, FileMetadata fileMeta) throws IOException, HashUtil.HashBuilderException, GVoDConfigException.Missing {
        LOG.info("{} lib directory {}", config.getSelf(), config.getVideoLibrary());
        String videoName = video.substring(0, video.indexOf("."));
        String videoExt = video.substring(video.indexOf("."));
        String videoFilePath = config.getVideoLibrary() + File.separator + video;
        String hashFilePath = config.getVideoLibrary() + File.separator + videoName + ".hash";

        File hashFile = new File(hashFilePath);
        if (hashFile.exists()) {
            hashFile.delete();
        }

//        int hashPieces = fileMeta.hashFileSize / HashUtil.getHashSize(fileMeta.hashAlg);
//        PieceTracker hashPieceTracker = new SimplePieceTracker(hashPieces);
//        Storage hashStorage = StorageFactory.getEmptyFile(hashFilePath, fileMeta.hashFileSize, HashUtil.getHashSize(fileMeta.hashAlg));
//        FileMngr hashMngr = new SimpleFileMngr(hashStorage, hashPieceTracker);
        HashMngr hashMngr = StorageMngrFactory.getIncompleteHashMngr(hashFilePath, fileMeta.hashAlg, fileMeta.hashFileSize, HashUtil.getHashSize(fileMeta.hashAlg));

//        Storage videoStorage = StorageFactory.getEmptyFile(videoFilePath, fileMeta.fileSize, fileMeta.pieceSize);
//        int filePieces = fileMeta.fileSize / fileMeta.pieceSize + (fileMeta.fileSize % fileMeta.pieceSize == 0 ? 0 : 1);
//        PieceTracker videoPieceTracker = new SimplePieceTracker(filePieces);
//        FileMngr fileMngr = new SimpleFileMngr(videoStorage, videoPieceTracker);
        int blockSize = config.getPiecesPerBlock() * config.getPieceSize();
        FileMngr fileMngr = StorageMngrFactory.getIncompleteFileMngr(videoFilePath, fileMeta.fileSize, blockSize, config.getPieceSize());

        return Pair.with(fileMngr, hashMngr);
    }
}
