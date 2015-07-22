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
package se.sics.gvod.manager;

import com.google.common.util.concurrent.SettableFuture;
import java.io.IOException;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import org.javatuples.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.gvod.common.utility.UtilityUpdatePort;
import se.sics.gvod.core.VoDPort;
import se.sics.gvod.core.msg.DownloadVideo;
import se.sics.gvod.core.msg.GetLibrary;
import se.sics.gvod.core.msg.PlayReady;
import se.sics.gvod.core.msg.UploadVideo;
import se.sics.gvod.core.util.ResponseStatus;
import se.sics.gvod.manager.toolbox.GVoDSyncI;
import se.sics.gvod.manager.toolbox.Result;
import se.sics.gvod.manager.toolbox.VideoInfo;
import se.sics.gvod.manager.util.FileStatus;
import se.sics.kompics.Component;
import se.sics.kompics.Component.State;
import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.Handler;
import se.sics.kompics.Init;
import se.sics.kompics.Positive;
import se.sics.kompics.Start;
import se.sics.kompics.Stop;
import se.sics.p2ptoolbox.videostream.VideoStreamManager;
import se.sics.p2ptoolbox.videostream.http.BaseHandler;
import se.sics.p2ptoolbox.videostream.http.JwHttpServer;
import se.sics.p2ptoolbox.videostream.http.RangeCapableMp4Handler;

/**
 *
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class VoDManagerImpl extends ComponentDefinition implements GVoDSyncI {

    private static final Logger LOG = LoggerFactory.getLogger(VoDManager.class);

    private final Positive<VoDPort> vodPort = requires(VoDPort.class);
    private final Positive<UtilityUpdatePort> utilityPort = requires(UtilityUpdatePort.class);

    private final VoDManagerConfig config;
    private final Random rand = new Random();
    private final String logPrefix;

    private final Map<String, VideoStreamManager> vsMngrs;
    private final Integer videoPort;
    private final Set<String> videoPaths;

    private Map<UUID, SettableFuture> pendingJobs;
    private Set<String> pendingUploads;
    private Map<String, FileStatus> videos;

    public VoDManagerImpl(VoDManagerInit init) {
        this.config = init.config;
        this.logPrefix = config.getSelf().toString();
        LOG.info("{} initiating...", logPrefix);

        this.vsMngrs = new ConcurrentHashMap<String, VideoStreamManager>();

        Integer httpPlayPort = -1;
        do {
            httpPlayPort = tryPort(10000 + rand.nextInt(40000));
        } while (httpPlayPort == -1);
        this.videoPort = httpPlayPort;
        this.videoPaths = new HashSet<String>();

        this.pendingJobs = new HashMap<UUID, SettableFuture>();
        this.pendingUploads = new HashSet<String>();
        this.videos = new HashMap<String, FileStatus>();

        subscribe(handleStart, control);
        subscribe(handleStop, control);
        subscribe(handlePlayReady, vodPort);
        subscribe(handleGetLibraryResponse, vodPort);
    }

    private Handler handleStart = new Handler<Start>() {
        @Override
        public void handle(Start event) {
            LOG.info("{} starting...", logPrefix);
        }
    };

    private Handler handleStop = new Handler<Stop>() {
        @Override
        public void handle(Stop event) {
            LOG.info("{} stopping...", logPrefix);
        }
    };

    @Override
    public boolean isReady() {
        return this.getComponentCore().state().equals(State.ACTIVE);
    }

    @Override
    public void getFiles(SettableFuture<Result<Map<String, FileStatus>>> opFuture) {
        LOG.info("{} get files request", logPrefix);
        GetLibrary.Request req = new GetLibrary.Request(UUID.randomUUID());
        pendingJobs.put(req.reqId, opFuture);
        trigger(req, vodPort);
    }

    private Handler handleGetLibraryResponse = new Handler<GetLibrary.Response>() {
        @Override
        public void handle(GetLibrary.Response resp) {
            SettableFuture opFuture = pendingJobs.remove(resp.reqId);
            if (opFuture == null) {
                return;
            }
            if (resp.respStatus.equals(ResponseStatus.SUCCESS)) {
                videos = convert(resp.fileStatusMap);
                for (String pendingFile : pendingUploads) {
                    videos.put(pendingFile, FileStatus.PENDING);
                }
                LOG.info("{} get files completed", logPrefix);
                opFuture.set(Result.ok(videos));
                return;
            } else {
                LOG.warn("{} get library:{}", logPrefix, resp.respStatus);
                opFuture.set(Result.internalError("failed"));
                return;
            }
        }
    };

    private Map<String, FileStatus> convert(Map<String, Pair<se.sics.gvod.core.util.FileStatus, Integer>> fileStatusMap) {
        Map<String, FileStatus> convertedFileStatusMap = new HashMap<String, FileStatus>();
        for (Map.Entry<String, Pair<se.sics.gvod.core.util.FileStatus, Integer>> e : fileStatusMap.entrySet()) {
            FileStatus fileStatus = null;
            switch (e.getValue().getValue0()) {
                case PAUSED:
                    fileStatus = FileStatus.DOWNLOADING;
                    break;
                case PENDING_DOWNLOAD:
                    fileStatus = FileStatus.DOWNLOADING;
                    break;
                case DOWNLOADING:
                    fileStatus = FileStatus.DOWNLOADING;
                    break;
                case PENDING_UPLOAD:
                    fileStatus = FileStatus.PENDING;
                    break;
                case UPLOADING:
                    fileStatus = FileStatus.UPLOADING;
                    break;
                case NONE:
                    fileStatus = FileStatus.NONE;
                    break;
                default:
                    break;
            }
            convertedFileStatusMap.put(e.getKey(), fileStatus);
        }
        return convertedFileStatusMap;
    }

    @Override
    public void pendingUpload(VideoInfo videoInfo, SettableFuture<Result<Boolean>> opFuture) {
        LOG.info("{} pending upload:{} request", logPrefix, videoInfo.getName());
        FileStatus videoStatus = videos.get(videoInfo.getName());
        if (videoStatus == null) {
            LOG.warn("{} video:{} not found in library:{}", new Object[]{logPrefix, videoInfo.getName(), config.getVideoLibrary()});
            opFuture.set(Result.internalError("video not found"));
            return;
        }
        if (!videoStatus.equals(FileStatus.NONE)) {
            LOG.warn("{} video:{} has status:{} - cannot upload", new Object[]{logPrefix, videoInfo.getName(), videoStatus});
            opFuture.set(Result.internalError("video - bad state"));
            return;
        }
        videos.put(videoInfo.getName(), FileStatus.PENDING);
        pendingUploads.add(videoInfo.getName());
        LOG.info("{} pending upload:{} completed", logPrefix, videoInfo.getName());
        opFuture.set(new Result(true));
    }

    @Override
    public void upload(VideoInfo videoInfo, SettableFuture<Result<Boolean>> opFuture) {
        LOG.info("{} upload:{} request", logPrefix, videoInfo.getName());
        if (pendingUploads.remove(videoInfo.getName() == null)) {
            LOG.warn("{} video not pending upload - cannot initiate upload", new Object[]{config.getSelf(), videoInfo.getName(), config.getVideoLibrary()});
            opFuture.set(Result.internalError("video - bad state"));
            return;
        }
        videos.put(videoInfo.getName(), FileStatus.PENDING);
        UploadVideo.Request req = new UploadVideo.Request(videoInfo.getName(), videoInfo.getOverlayId());
        trigger(req, vodPort);
        pendingJobs.put(req.id, opFuture);
    }

    @Override
    public void download(VideoInfo videoInfo, SettableFuture<Result<Boolean>> opFuture) {
        LOG.info("{} download:{} request", logPrefix, videoInfo.getName());
        FileStatus videoStatus = videos.get(videoInfo.getName());
        if (videoStatus == FileStatus.DOWNLOADING || videoStatus == FileStatus.UPLOADING || videoStatus == FileStatus.PENDING) {
            opFuture.set(Result.ok(true));
            return;
        }
        videos.put(videoInfo.getName(), FileStatus.PENDING);
        DownloadVideo.Request req = new DownloadVideo.Request(videoInfo.getName(), videoInfo.getOverlayId());
        trigger(req, vodPort);
        pendingJobs.put(req.id, opFuture);
    }

    private Handler<PlayReady> handlePlayReady = new Handler<PlayReady>() {

        @Override
        public void handle(PlayReady resp) {
            LOG.info("{} video:{} ready to play", logPrefix, resp.videoName);
            vsMngrs.put(resp.videoName, resp.vsMngr);

            SettableFuture myFuture = pendingJobs.remove(resp.id);
            if (myFuture == null) {
                return;
            }
            myFuture.set(Result.ok(true));
        }
    };

    @Override
    public void play(VideoInfo videoInfo, SettableFuture<Result<Integer>> opFuture) {
        LOG.info("{} play video:{} request", logPrefix, videoInfo.getName());

        VideoStreamManager videoPlayer = vsMngrs.get(videoInfo.getName());
        if (videoPlayer == null) {
            SettableFuture<Result<Boolean>> downloadFuture = SettableFuture.create();
            download(videoInfo, downloadFuture);
            try {
                Result<Boolean> downloadResult = downloadFuture.get();
                if (!downloadResult.ok()) {
                    LOG.error("{} video:{} - {}", new Object[]{logPrefix, videoInfo.getName(), downloadResult.getDetails()});
                    opFuture.set(Result.failed(downloadResult.status, downloadResult.getDetails()));
                    return;
                }
            } catch (InterruptedException ex) {
                LOG.error("future error");
                opFuture.set(Result.internalError("future error"));
                return;
            } catch (ExecutionException ex) {
                LOG.error("future error");
                opFuture.set(Result.internalError("future error"));
                return;
            }
            videoPlayer = vsMngrs.get(videoInfo.getName());
            if (videoPlayer == null) {
                LOG.error("no video player");
                opFuture.set(Result.internalError("no video player"));
                return;
            }
        }

        if (!videoPaths.contains(videoInfo.getName())) {
            LOG.info("{} setting up player for video:{}", logPrefix, videoInfo.getName());
            setupPlayerHttpConnection(videoPlayer, videoInfo.getName());
            videoPaths.add(videoInfo.getName());
        }

        LOG.info("{} return play for video:{}", logPrefix, videoInfo.getName());
        opFuture.set(Result.ok(videoPort));
    }

    @Override
    public void stop(VideoInfo videoInfo, SettableFuture<Result<Boolean>> opFuture) {
        LOG.info("{} stop video:{} request", logPrefix, videoInfo.getName());
        VideoStreamManager vsMngr = vsMngrs.get(videoInfo.getName());
        if (vsMngr == null) {
            LOG.warn("{} player for video:{} is not ready yet - weird stop message", logPrefix, videoInfo.getName());
            opFuture.set(Result.ok(true));
            return;
        } else {
            LOG.info("{} stop video:{} completed", logPrefix, videoInfo.getName());
            vsMngr.stop();
            opFuture.set(Result.ok(true));
            return;
        }
    }

    private void setupPlayerHttpConnection(VideoStreamManager vsMngr, String videoName) {
        LOG.info("{} starting player http connection http://127.0.0.1:{}/{}/{}", new Object[]{config.getSelf(), videoPort, videoName, videoName});
        String fileName = "/" + videoName + "/";
        BaseHandler handler = new RangeCapableMp4Handler(vsMngr);
        try {
            InetSocketAddress httpAddr = new InetSocketAddress(videoPort);
            JwHttpServer.startOrUpdate(httpAddr, fileName, handler);
        } catch (IOException ex) {
            //TODO Alex - check if this is a recovarable state
            LOG.error("{} http server error", logPrefix);
            throw new RuntimeException(ex);
        }
    }

    private int tryPort(int port) {
        ServerSocket ss = null;
        DatagramSocket ds = null;
        try {
            ss = new ServerSocket(port);
            ss.setReuseAddress(true);
            ds = new DatagramSocket(port);
            ds.setReuseAddress(true);
        } catch (IOException e) {
            return -1;
        } finally {
            if (ds != null) {
                ds.close();
            }

            if (ss != null) {
                try {
                    ss.close();
                } catch (IOException e) {
                    /* should not be thrown */
                    LOG.error("error while picking port");
                    System.exit(1);
                }
            }
        }
        return port;
    }

    public static class VoDManagerInit extends Init<VoDManagerImpl> {

        public final VoDManagerConfig config;

        public VoDManagerInit(VoDManagerConfig config) {
            this.config = config;
        }
    }
}
