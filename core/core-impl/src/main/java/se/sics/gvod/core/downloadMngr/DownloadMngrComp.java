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
package se.sics.gvod.core.downloadMngr;

import com.google.common.base.Optional;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import org.javatuples.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.gvod.common.event.vod.Download;
import se.sics.gvod.core.connMngr.ConnMngrPort;
import se.sics.gvod.core.connMngr.event.Ready;
import se.sics.gvod.common.utility.UtilityUpdate;
import se.sics.gvod.common.utility.UtilityUpdatePort;
import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.Handler;
import se.sics.kompics.Init;
import se.sics.kompics.Negative;
import se.sics.kompics.Positive;
import se.sics.kompics.Start;
import se.sics.kompics.Stop;
import se.sics.kompics.timer.CancelPeriodicTimeout;
import se.sics.kompics.timer.SchedulePeriodicTimeout;
import se.sics.kompics.timer.Timeout;
import se.sics.kompics.timer.Timer;
import se.sics.ktoolbox.util.managedStore.FileMngr;
import se.sics.ktoolbox.util.managedStore.HashMngr;

/**
 * @author Alex Ormenisan <aaor@sics.se>
 */
public class DownloadMngrComp extends ComponentDefinition {

    private static final Logger LOG = LoggerFactory.getLogger(DownloadMngrComp.class);
    private final String logPrefix;

    private Negative<UtilityUpdatePort> utilityUpdate = provides(UtilityUpdatePort.class);
    private Negative<DownloadMngrPort> dataPort = provides(DownloadMngrPort.class);
    private Positive<Timer> timer = requires(Timer.class);
    private Positive<ConnMngrPort> connMngr = requires(ConnMngrPort.class);

    private final DownloadMngrKCWrapper config;
    public final DownloadMngr downloadMngr;
    private boolean downloading;
    private final AtomicInteger playPos; //set by videoStreamManager, read here

    private UUID speedUpTId = null;
    private UUID periodicUpdateSelfTId = null;

    public DownloadMngrComp(DownloadMngrInit init) {
        this.config = init.config;
        this.logPrefix = config.selfAddress.getId() + "<" + config.overlayId + ">";
        LOG.info("{} initiating", logPrefix);

        this.playPos = init.playPos;
        this.downloadMngr = new DownloadMngr(config, init.hashMngr, init.fileMngr);
        this.downloading = init.downloader;

        subscribe(handleStart, control);
        subscribe(handleStop, control);
        subscribe(handleUpdateSelf, timer);
        subscribe(handleDataRequest, dataPort);
        subscribe(handleConnReady, connMngr);
        subscribe(handleHashRequest, connMngr);
        subscribe(handleHashResponse, connMngr);
        subscribe(handleDownloadDataRequest, connMngr);
        subscribe(handleDownloadDataResponse, connMngr);
    }

    private Handler handleStart = new Handler<Start>() {

        @Override
        public void handle(Start event) {
            LOG.info("{} starting...", logPrefix);
            schedulePeriodicUpdateSelf();

            Integer downloadPos = downloadMngr.contiguousBlocks(playPos.get());
            trigger(new UtilityUpdate(config.overlayId, downloading, downloadPos), utilityUpdate);
        }
    };

    private Handler handleStop = new Handler<Stop>() {
        @Override
        public void handle(Stop e) {
            LOG.info("{} stopping", logPrefix);
        }
    };

    private Handler handleConnReady = new Handler<Ready>() {
        @Override
        public void handle(Ready ready) {
            if (downloading) {
                LOG.info("{} ready {} download slots", logPrefix, ready.slots);
                for (int i = 0; i < ready.slots; i++) {
                    download();
                }
            } else {
                LOG.warn("{} weird ready", logPrefix);
            }
        }
    };

    private Handler<PeriodicUtilityUpdate> handleUpdateSelf = new Handler<PeriodicUtilityUpdate>() {

        @Override
        public void handle(PeriodicUtilityUpdate event) {
            checkCompleteBlocks();

            LOG.info("{} download mngr status\n {}", logPrefix, downloadMngr);
            int playPieceNr = playPos.get();
            playPieceNr = (playPieceNr == -1 ? 0 : playPieceNr);
            int playBlockNr = PieceBlockHelper.pieceIdToBlockNrPieceNr(playPieceNr, config.piecesPerBlock).getValue0();
            int downloadBlockNr = downloadMngr.contiguousBlocks(playBlockNr);
            LOG.info("{} playBlockPos:{} downloadBlockPos:{}",
                    new Object[]{logPrefix, playBlockNr, downloadBlockNr});
            trigger(new UtilityUpdate(config.overlayId, downloading, downloadBlockNr), utilityUpdate);

            if (!downloading) {
                cancelUpdateSelf();
            }
        }
    };

    private Handler handleDataRequest = new Handler<Data.Request>() {

        @Override
        public void handle(Data.Request req) {
            LOG.trace("{} received local data request for readPos:{} readSize:{}",
                    new Object[]{logPrefix, req.readPos, req.readBlockSize});

            Optional<byte[]> result = downloadMngr.dataRequest(req.readPos, req.readBlockSize);
            if(!result.isPresent() && checkCompleteBlocks()) {
                result = downloadMngr.dataRequest(req.readPos, req.readBlockSize);
            }
            if (result.isPresent()) {
                LOG.trace("{} sending local data - readPos:{} readSize:{}",
                        new Object[]{logPrefix, req.readPos, req.readBlockSize});
                answer(req, req.success(result.get()));
            } else {
                LOG.debug("{} local data missing - readPos:{} , readSize:{}",
                        new Object[]{logPrefix, req.readPos, req.readBlockSize});
            }
        }
    };

    private Handler handleHashRequest = new Handler<Download.HashRequest>() {

        @Override
        public void handle(Download.HashRequest req) {
            LOG.trace("{} received hash request:{}", logPrefix, req.hashes);

            Pair<Map<Integer, byte[]>, Set<Integer>> result = downloadMngr.hashRequest(req.hashes);
            LOG.trace("{} sending hashes:{} missing hashes:{}", new Object[]{logPrefix, result.getValue0().keySet(), result.getValue1()});
            trigger(req.success(result.getValue0(), result.getValue1()), connMngr);
        }
    };

    private Handler handleHashResponse = new Handler<Download.HashResponse>() {

        @Override
        public void handle(Download.HashResponse resp) {
            switch (resp.status) {
                case SUCCESS:
                    LOG.trace("{} SUCCESS hashes:{} missing hashes:{}", new Object[]{logPrefix, resp.hashes.keySet(), resp.missingHashes});
                    downloadMngr.hashResponse(resp.hashes, resp.missingHashes);
                    download();
                    return;
                case TIMEOUT:
                case BUSY:
                    LOG.debug("{} BUSY/TIMEOUT hashes:{}", logPrefix, resp.missingHashes);
                    downloadMngr.hashResponse(resp.hashes, resp.missingHashes);
                    return;
                default:
                    LOG.warn("{} illegal status:{}, ignoring", new Object[]{logPrefix, resp.status});
            }
        }
    };

    private Handler handleDownloadDataRequest = new Handler<Download.DataRequest>() {

        @Override
        public void handle(Download.DataRequest req) {
            LOG.trace("{} received data request:{}", logPrefix, req.pieceId);

            Optional<byte[]> piece = downloadMngr.dataRequest(req.pieceId);
            if (piece.isPresent()) {
                LOG.trace("{} sending data:{}", new Object[]{logPrefix, req.pieceId});
                trigger(req.success(piece.get()), connMngr);
            } else {
                LOG.debug("{} missing data:{}", new Object[]{logPrefix, req.pieceId});
                trigger(req.missingPiece(), connMngr);
            }
        }
    };

    private Handler handleDownloadDataResponse = new Handler<Download.DataResponse>() {

        @Override
        public void handle(Download.DataResponse resp) {
            switch (resp.status) {
                case SUCCESS:
                    LOG.trace("{} SUCCESS piece:{}", new Object[]{logPrefix, resp.pieceId});
                    downloadMngr.dataResponse(resp.pieceId, Optional.fromNullable(resp.piece));
                    download();
                    return;
                case TIMEOUT:
                case MISSING:
                case BUSY:
                    LOG.debug("{} MISSING/TIMEOUT/BUSY piece:{}", new Object[]{logPrefix, resp.pieceId});
                    downloadMngr.dataResponse(resp.pieceId, Optional.fromNullable(resp.piece));
                    return;
                default:
                    LOG.warn("{} illegal status:{} ignoring", new Object[]{logPrefix, resp.status});
            }
        }
    };

    private boolean checkCompleteBlocks() {
        //TODO Alex might need to move it to its own timeout
        Pair<Set<Integer>, Map<Integer, byte[]>> blocksInfo = downloadMngr.checkCompleteBlocks();
        LOG.info("{} completed blocks:{} reset blocks:{}", new Object[]{logPrefix, blocksInfo.getValue0(), blocksInfo.getValue1().keySet()});

        if (downloadMngr.isComplete()) {
            finishDownload();
        }
        if(blocksInfo.getValue0().isEmpty()) {
            return false;
        }
        return true;
    }

    private boolean download() {
        int currentPlayPiece = playPos.get();
        int currentPlayBlock = PieceBlockHelper.getBlockNr(currentPlayPiece, config.piecesPerBlock);
        downloadMngr.download(currentPlayBlock);
        if (!downloadHash()) {
            if (!downloadData()) {
                return false;
            }
        }
        return true;
    }

    private boolean downloadData() {
        Optional<Integer> nextPieceId = downloadMngr.downloadData();
        if (!nextPieceId.isPresent()) {
            return false;
        }
        LOG.trace("{} downloading piece:{}", logPrefix, nextPieceId);
        trigger(new Download.DataRequest(config.overlayId, nextPieceId.get()), connMngr);
        return true;
    }

    private boolean downloadHash() {
        Optional<Set<Integer>> nextHashes = downloadMngr.downloadHash();
        if (!nextHashes.isPresent()) {
            return false;
        }
        int targetPos = Collections.min(nextHashes.get());
        LOG.trace("{} downloading hashes:{} targetPos:{}", new Object[]{logPrefix, nextHashes.get(), targetPos});
        trigger(new Download.HashRequest(targetPos, nextHashes.get()), connMngr);
        return true;
    }

    private void finishDownload() {
        downloading = false;
        LOG.info("{} finished download", logPrefix);
    }

    //**************************Timeouts****************************************
    private void schedulePeriodicUpdateSelf() {
        if (periodicUpdateSelfTId != null) {
            LOG.warn("{} double schedule of periodic UpdateSelf timeout", logPrefix);
        } else {
            LOG.trace("{} scheduling periodic UpdateSelf timeout", logPrefix);
        }
        SchedulePeriodicTimeout spt = new SchedulePeriodicTimeout(config.descriptorUpdate, config.descriptorUpdate);
        Timeout t = new PeriodicUtilityUpdate(spt);
        periodicUpdateSelfTId = t.getTimeoutId();
        spt.setTimeoutEvent(t);
        trigger(spt, timer);
    }

    private void cancelUpdateSelf() {
        if (periodicUpdateSelfTId == null) {
            LOG.warn("{} double cancelation of periodic UpdateSelf timeout", logPrefix);
        } else {
            LOG.trace("{} canceling periodic UpdateSelf timeout", logPrefix);
        }
        CancelPeriodicTimeout cpt = new CancelPeriodicTimeout(periodicUpdateSelfTId);
        trigger(cpt, timer);
        periodicUpdateSelfTId = null;
    }

    public class PeriodicUtilityUpdate extends Timeout {

        public PeriodicUtilityUpdate(SchedulePeriodicTimeout spt) {
            super(spt);
        }
    }
    //**************************************************************************

    public static class DownloadMngrInit extends Init<DownloadMngrComp> {

        public final DownloadMngrKCWrapper config;
        public final HashMngr hashMngr;
        public final FileMngr fileMngr;
        public final boolean downloader;
        public final AtomicInteger playPos;

        public DownloadMngrInit(DownloadMngrKCWrapper config, FileMngr fileMngr, HashMngr hashMngr, boolean downloader, AtomicInteger playPos) {
            this.config = config;
            this.hashMngr = hashMngr;
            this.fileMngr = fileMngr;
            this.downloader = downloader;
            this.playPos = playPos;
        }
    }
}
