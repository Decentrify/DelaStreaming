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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import org.javatuples.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.gvod.common.msg.ReqStatus;
import se.sics.gvod.core.connMngr.ConnMngrPort;
import se.sics.gvod.core.connMngr.msg.Ready;
import se.sics.gvod.common.msg.vod.Download;
import se.sics.gvod.common.util.HashUtil;
import se.sics.gvod.common.utility.UtilityUpdate;
import se.sics.gvod.common.utility.UtilityUpdatePort;
import se.sics.gvod.core.downloadMngr.msg.ScheduledSpeedUp;
import se.sics.gvod.core.downloadMngr.msg.ScheduledUtilityUpdate;
import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.Handler;
import se.sics.kompics.Init;
import se.sics.kompics.Negative;
import se.sics.kompics.Positive;
import se.sics.kompics.Start;
import se.sics.kompics.timer.CancelPeriodicTimeout;
import se.sics.kompics.timer.SchedulePeriodicTimeout;
import se.sics.kompics.timer.Timeout;
import se.sics.kompics.timer.Timer;
import se.sics.p2ptoolbox.util.managedStore.BlockMngr;
import se.sics.p2ptoolbox.util.managedStore.FileMngr;
import se.sics.p2ptoolbox.util.managedStore.HashMngr;
import se.sics.p2ptoolbox.util.managedStore.StorageMngrFactory;

/**
 * @author Alex Ormenisan <aaor@sics.se>
 */
public class DownloadMngrComp extends ComponentDefinition {

    private static final Logger log = LoggerFactory.getLogger(DownloadMngrComp.class);

    private Negative<UtilityUpdatePort> utilityUpdate = provides(UtilityUpdatePort.class);
    private Negative<DownloadMngrPort> dataPort = provides(DownloadMngrPort.class);
    private Positive<Timer> timer = requires(Timer.class);
    private Positive<ConnMngrPort> connMngr = requires(ConnMngrPort.class);

    private final DownloadMngrConfig config;
    private final AtomicInteger playPos; //set by videoStreamManager, read here

    private final HashMngr hashMngr;
    private final FileMngr fileMngr;
    private boolean downloading;

    private final Map<Integer, BlockMngr> queuedBlocks;

    private Set<Integer> pendingPieces;
    private List<Integer> nextPieces;
    private Set<Integer> pendingHashes;
    private List<Integer> nextHashes;

    private UUID speedUpTId = null;
    private UUID updateSelfTId;

    public DownloadMngrComp(DownloadMngrInit init) {
        this.config = init.config;
        log.info("{}:{} video component init", config.getSelf(), config.overlayId);

        this.playPos = init.playPos;
        this.hashMngr = init.hashMngr;
        this.fileMngr = init.fileMngr;
        this.downloading = init.downloader;

        this.queuedBlocks = new HashMap<Integer, BlockMngr>();
        this.pendingPieces = new HashSet<Integer>();
        this.nextPieces = new ArrayList<Integer>();
        this.pendingHashes = new HashSet<Integer>();
        this.nextHashes = new ArrayList<Integer>();

        subscribe(handleStart, control);
    }

    private Handler<Start> handleStart = new Handler<Start>() {

        @Override
        public void handle(Start event) {
            log.info("{} {} starting...", config.getSelf(), config.overlayId);

            Integer downloadPos = fileMngr.contiguous(0);
            Integer hashPos = hashMngr.contiguous(0);
            log.info("{} {} video pos:{}, hash pos:{}", new Object[]{config.getSelf(), config.overlayId, downloadPos, hashPos});
            trigger(new UtilityUpdate(config.overlayId, downloading, downloadPos), utilityUpdate);

            subscribe(handleConnReady, connMngr);
            subscribe(handleDataRequest, dataPort);
            subscribe(handleHashRequest, connMngr);
            subscribe(handleHashResponse, connMngr);
            subscribe(handleDownloadDataRequest, connMngr);
            subscribe(handleDownloadDataResponse, connMngr);
        }
    };

    private Handler<Data.DRequest> handleDataRequest = new Handler<Data.DRequest>() {

        @Override
        public void handle(Data.DRequest req) {
            log.trace("{} received local data request for readPos:{} readSize:{}", new Object[]{config.getSelf(), req.readPos, req.readBlockSize});

            if (!fileMngr.has(req.readPos, req.readBlockSize)) {
                Set<Integer> targetedBlocks = posToBlockNr(req.readPos, req.readBlockSize);
                for (Integer blockNr : targetedBlocks) {
                    if (queuedBlocks.containsKey(blockNr)) {
                        checkCompleteBlocks();
                        break;
                    }
                }
                if (!fileMngr.has(req.readPos, req.readBlockSize)) {
                    log.debug("{} data missing - readPos:{} , readSize:{}", new Object[]{config.getSelf(), req.readPos, req.readBlockSize});
                    trigger(new Data.DResponse(req, ReqStatus.MISSING, null), dataPort);
                    return;
                }
            }
            log.debug("{} sending data - readPos:{} , readSize:{}", new Object[]{config.getSelf(), req.readPos, req.readBlockSize});
            byte data[] = fileMngr.read(req.readPos, req.readBlockSize);
            trigger(new Data.DResponse(req, ReqStatus.SUCCESS, data), dataPort);
        }

    };

    private Handler<Ready> handleConnReady = new Handler<Ready>() {
        @Override
        public void handle(Ready event) {

            if (downloading) {
                log.info("{} {} downloading video", config.getSelf(), config.overlayId);
                subscribe(handleDownloadSpeedUp, timer);
                subscribe(handleUpdateSelf, timer);

                for (int i = 0; i < config.startPieces; i++) {
                    download();
                }
                scheduleSpeedUp(1);

                scheduleUpdateSelf();

            } else {
                log.info("{} {} seeding file", config.getSelf(), config.overlayId);
            }
        }
    };

    private Handler<Download.HashRequest> handleHashRequest = new Handler<Download.HashRequest>() {

        @Override
        public void handle(Download.HashRequest req) {
            log.trace("{} handle {}", config.getSelf(), req);

            Map<Integer, byte[]> hashes = new HashMap<Integer, byte[]>();
            Set<Integer> missingHashes = new HashSet<Integer>();

            for (Integer hash : req.hashes) {
                if (hashMngr.hasHash(hash)) {
                    hashes.put(hash, hashMngr.readHash(hash));
                } else {
                    missingHashes.add(hash);
                }
            }
            log.debug("{} sending hashes{} missing hashes:{}", new Object[]{config.getSelf(), hashes.keySet(), missingHashes});
            trigger(req.success(hashes, missingHashes), connMngr);
        }

    };

    private Handler<Download.HashResponse> handleHashResponse = new Handler<Download.HashResponse>() {

        @Override
        public void handle(Download.HashResponse resp) {
            log.trace("{} handle {} {}", new Object[]{config.getSelf(), resp, resp.status});

            switch (resp.status) {
                case SUCCESS:
                    log.debug("{} SUCCESS hashes:{} missing hashes:{}", new Object[]{config.getSelf(), resp.hashes.keySet(), resp.missingHashes});

                    for (Map.Entry<Integer, byte[]> hash : resp.hashes.entrySet()) {
                        hashMngr.writeHash(hash.getKey(), hash.getValue());
                    }

                    pendingHashes.removeAll(resp.hashes.keySet());
                    nextHashes.addAll(0, resp.missingHashes);

                    download();
                    return;
                case TIMEOUT:
                    log.debug("{} TIMEOUT hashes:{}", config.getSelf(), resp.missingHashes);
                    pendingHashes.removeAll(resp.missingHashes);
                    nextHashes.addAll(0, resp.missingHashes);
                    download();
                    return;
                case BUSY:
                    log.debug("{} BUSY hashes:{}", config.getSelf(), resp.missingHashes);
                    pendingHashes.removeAll(resp.missingHashes);
                    nextHashes.addAll(0, resp.missingHashes);
                    cancelSpeedUp();
                    scheduleSpeedUp(10);
                    return;
                default:
                    log.warn("{} {} illegal status {}", new Object[]{config.getSelf(), resp, resp.status});
            }
        }
    };

    private Handler<Download.DataRequest> handleDownloadDataRequest = new Handler<Download.DataRequest>() {

        @Override
        public void handle(Download.DataRequest req) {
            log.trace("{} handle {}", config.getSelf(), req);

            if (fileMngr.hasPiece(req.pieceId)) {
                byte[] piece = fileMngr.readPiece(req.pieceId);
                log.debug("{} sending piece {}", new Object[]{config.getSelf(), req.pieceId});
                trigger(req.success(piece), connMngr);
            } else {
                log.debug("{} do not have piece {}", new Object[]{config.getSelf(), req.pieceId});
                trigger(req.missingPiece(), connMngr);
            }
        }
    };

    private Handler<Download.DataResponse> handleDownloadDataResponse = new Handler<Download.DataResponse>() {

        @Override
        public void handle(Download.DataResponse resp) {
            log.trace("{} handle {} {}", new Object[]{config.getSelf(), resp, resp.status});

            switch (resp.status) {
                case SUCCESS:
                    log.debug("{} SUCCESS piece:{}", new Object[]{config.getSelf(), resp.pieceId});

                    Pair<Integer, Integer> pieceIdToBlockNr = pieceIdToBlockNrPieceNr(resp.pieceId);
                    BlockMngr block = queuedBlocks.get(pieceIdToBlockNr.getValue0());
                    if (block == null) {
                        log.error("logic exception block is null");
                        System.exit(1);
                    }
                    block.writePiece(pieceIdToBlockNr.getValue1(), resp.piece);
                    pendingPieces.remove(resp.pieceId);
                    download();
                    return;
                case TIMEOUT:
                case MISSING:
                    log.debug("{} MISSING/TIMEOUTE piece:{} {}", new Object[]{config.getSelf(), resp.pieceId, resp.status});
                    pendingPieces.remove(resp.pieceId);
                    nextPieces.add(0, resp.pieceId);
                    download();
                    return;
                case BUSY:
                    log.debug("{} BUSY download slow down");
                    pendingPieces.remove(resp.pieceId);
                    nextPieces.add(0, resp.pieceId);
                    cancelSpeedUp();
                    scheduleSpeedUp(10);
                    return;
                default:
                    log.warn("{} {} illegal status {}", new Object[]{config.getSelf(), resp, resp.status});
            }

        }
    };

    private void checkCompleteBlocks() {
        Set<Integer> completedBlocks = new HashSet<Integer>();
        for (Map.Entry<Integer, BlockMngr> block : queuedBlocks.entrySet()) {
            int blockNr = block.getKey();
            if (!block.getValue().isComplete()) {
                continue;
            }
            if (!hashMngr.hasHash(blockNr)) {
                continue;
            }
            byte[] blockBytes = block.getValue().getBlock();
            byte[] blockHash = hashMngr.readHash(blockNr);
            if (HashUtil.checkHash(config.hashAlg, blockBytes, blockHash)) {
                fileMngr.writeBlock(blockNr, blockBytes);
                completedBlocks.add(blockNr);
            } else {
                //TODO Alex - might need to re-download hash as well
                log.info("{} piece:{} - hash problem, dropping block:{}", config.getSelf(), blockNr);

                int blockSize = fileMngr.blockSize(blockNr);
                BlockMngr blankBlock = StorageMngrFactory.getSimpleBlockMngr(blockSize, config.pieceSize);
                queuedBlocks.put(blockNr, blankBlock);
                for (int i = 0; i < blankBlock.nrPieces(); i++) {
                    int pieceId = blockNr * config.piecesPerBlock + i;
                    nextPieces.add(0, pieceId);
                }
            }
        }
        for (Integer blockNr : completedBlocks) {
            queuedBlocks.remove(blockNr);
        }
    }

    private Set<Integer> posToBlockNr(long pos, int size) {
        Set<Integer> result = new HashSet<Integer>();
        int blockNr = (int) (pos / (config.piecesPerBlock * config.pieceSize));
        result.add(blockNr);
        size -= config.piecesPerBlock * config.pieceSize;
        while (size > 0) {
            blockNr++;
            result.add(blockNr);
            size -= config.piecesPerBlock * config.pieceSize;
        }
        return result;
    }

    private Pair<Integer, Integer> pieceIdToBlockNrPieceNr(int pieceId) {
        int blockNr = pieceId / config.piecesPerBlock;
        int inBlockNr = pieceId % config.piecesPerBlock;
        return Pair.with(blockNr, inBlockNr);
    }

    private Handler<ScheduledSpeedUp> handleDownloadSpeedUp = new Handler<ScheduledSpeedUp>() {

        @Override
        public void handle(ScheduledSpeedUp event) {
            log.trace("{} handling speedup {}", config.getSelf(), event.getTimeoutId());

            if (speedUpTId == null) {
                log.debug("{} late timeout {}", config.getSelf(), speedUpTId);
                return;
            }
            int speedup = config.startPieces / 20 + 1;
            for (int i = 0; i < speedup; i++) {
                if (!download()) {
                    break;
                }
            }

            scheduleSpeedUp(1);
        }

    };

    private Handler<ScheduledUtilityUpdate> handleUpdateSelf = new Handler<ScheduledUtilityUpdate>() {

        @Override
        public void handle(ScheduledUtilityUpdate event) {
            log.trace("{} handle {}", config.getSelf(), event);
            log.info("{} hashComplete:{} fileComplete:{}", new Object[]{config.overlayId, hashMngr.isComplete(0), fileMngr.isComplete(0)});
            log.info("{} pending pieces:{} pendingHashes:{} pendingBlocks:{}", new Object[]{config.overlayId, pendingPieces.size(), pendingHashes.size(), queuedBlocks.keySet()});
            log.info("{} nextPieces:{} nextHashes:{}", new Object[]{config.overlayId, nextPieces.size(), nextHashes.size()});
            //TODO Alex might need to move it to its own timeout
            checkCompleteBlocks();
            if (hashMngr.isComplete(0) && fileMngr.isComplete(0)) {
                finishDownload();
            }
            int playPieceNr = playPos.get();
            playPieceNr = (playPieceNr == -1 ? 0 : playPieceNr);
            int playBlockNr = pieceIdToBlockNrPieceNr(playPieceNr).getValue0();
            int playPos = fileMngr.contiguous(playBlockNr);
            log.info("{} play pos:{} videoPPos:{} videoDPos:{} hashDPos:{}",
                    new Object[]{config.overlayId, playBlockNr, playPos, fileMngr.contiguous(0), hashMngr.contiguous(0)});
            trigger(new UtilityUpdate(config.overlayId, downloading, playPos), utilityUpdate);
        }
    };

    private boolean download() {
        int currentPlayPiece = playPos.get();
        int currentPlayBlock = pieceIdToBlockNrPieceNr(currentPlayPiece).getValue0();
        if (nextHashes.isEmpty() && nextPieces.isEmpty()) {
            if (fileMngr.isComplete(currentPlayBlock)) {
                currentPlayPiece = 0;
                playPos.set(0);
            }
            if (fileMngr.isComplete(0)) {
                finishDownload();
                return false;
            } else {
                int blockNr = pieceIdToBlockNrPieceNr(currentPlayPiece).getValue0();
                if (!getNewPieces(blockNr)) {
                    if (!getNewPieces(0)) {
                        return false;
                    }
                }
            }
        }

        if (!downloadHash()) {
            if (!downloadData()) {
                cancelSpeedUp();
                scheduleSpeedUp(10);
                return false;
            }
        }
        return true;
    }

    private boolean getNewPieces(int currentBlockNr) {
        log.debug("getting new pieces from block:{}", currentBlockNr);
        int filePos = fileMngr.contiguous(currentBlockNr);
        int hashPos = hashMngr.contiguous(0);

        if (filePos + 5 * config.minHashAhead > hashPos + pendingHashes.size()) {
            Set<Integer> except = new HashSet<Integer>();
            except.addAll(pendingHashes);
            except.addAll(nextHashes);
            Set<Integer> newNextHashes = hashMngr.nextHashes(config.hashesPerMsg, 0, except);
            log.debug("hashPos:{} pendingHashes:{} nextHashes:{} newNextHashes:{}", new Object[]{hashPos, pendingHashes, nextHashes, newNextHashes});
            nextHashes.addAll(newNextHashes);
            if (!nextHashes.isEmpty()) {
                return true;
            }
        }

        Integer nextBlockNr = fileMngr.nextBlock(currentBlockNr, queuedBlocks.keySet());
        if (nextBlockNr == null) {
            log.debug("next block is null, blockNr:{}", filePos);
            return false;
        }
        //last block might have less nr of pieces than default
        int blockSize = fileMngr.blockSize(nextBlockNr);
        BlockMngr blankBlock = StorageMngrFactory.getSimpleBlockMngr(blockSize, config.pieceSize);
        queuedBlocks.put(nextBlockNr, blankBlock);
        for (int i = 0; i < blankBlock.nrPieces(); i++) {
            int pieceId = nextBlockNr * config.piecesPerBlock + i;
            nextPieces.add(pieceId);
        }
        return !nextPieces.isEmpty();
    }

    private boolean downloadData() {
        if (nextPieces.isEmpty()) {
            return false;
        }
        int nextPieceId = nextPieces.remove(0);
        log.debug("{} downloading piece:{}", config.getSelf(), nextPieceId);
        trigger(new Download.DataRequest(UUID.randomUUID(), config.overlayId, nextPieceId), connMngr);
        pendingPieces.add(nextPieceId);
        return true;
    }

    private boolean downloadHash() {
        if (nextHashes.isEmpty()) {
            return false;
        }
        Set<Integer> hashesToDownload = new HashSet<Integer>();
        for (int i = 0; i < config.hashesPerMsg && !nextHashes.isEmpty(); i++) {
            hashesToDownload.add(nextHashes.remove(0));
        }
        int targetPos = Collections.min(hashesToDownload);
        log.debug("{} downloading hashes:{} targetPos:{}", new Object[]{config.getSelf(), hashesToDownload, targetPos});
        trigger(new Download.HashRequest(UUID.randomUUID(), targetPos, hashesToDownload), connMngr);
        pendingHashes.addAll(hashesToDownload);
        return true;
    }

    private void scheduleSpeedUp(int periodFactor) {
//        ScheduleTimeout st = new ScheduleTimeout(periodFactor * config.speedupPeriod);
//        Timeout t = new ScheduledSpeedUp(st);
//        st.setTimeoutEvent(t);
//        speedUpTId = t.getTimeoutId();
//        log.debug("{} scheduling speedup timeout {}", config.getSelf(), speedUpTId);
//        trigger(st, timer);
    }

    private void cancelSpeedUp() {
//        log.debug("{} canceling speedup timeout {}", config.getSelf(), speedUpTId);
//        CancelTimeout cancelSpeedUp = new CancelTimeout(speedUpTId);
//        trigger(cancelSpeedUp, timer);
    }

    private void scheduleUpdateSelf() {
        long updateSelfPeriod = config.descriptorUpdate; //get proper value later
        SchedulePeriodicTimeout spt = new SchedulePeriodicTimeout(updateSelfPeriod, updateSelfPeriod);
        Timeout t = new ScheduledUtilityUpdate(spt);
        updateSelfTId = t.getTimeoutId();
        spt.setTimeoutEvent(t);
        log.debug("{} scheduling periodic timeout {}", config.getSelf(), t);
        trigger(spt, timer);
    }

    private void cancelUpdateSelf() {
        log.debug("{} canceling periodic timeout {}", config.getSelf(), updateSelfTId);
        CancelPeriodicTimeout cpt = new CancelPeriodicTimeout(updateSelfTId);
        trigger(cpt, timer);
    }

    private void finishDownload() {
//        Integer downloadPos = fileMngr.contiguous(0);
//        Integer hashPos = hashMngr.contiguous(0);
//        log.info("{} {} video pos:{}, hash pos:{}", new Object[]{config.getSelf(), config.overlayId, downloadPos, hashPos});
//
//        //one last update
//        trigger(new UtilityUpdate(config.overlayId, downloading, downloadPos), utilityUpdate);

        //cancel timeouts
        cancelSpeedUp();
        cancelUpdateSelf();
//        unsubscribe(handleUpdateSelf, timer);
//        unsubscribe(handleDownloadSpeedUp, timer);

        downloading = false;
        log.info("{} {} finished download", config.getSelf(), config.overlayId);
    }

    public static class DownloadMngrInit extends Init<DownloadMngrComp> {

        public final DownloadMngrConfig config;
        public final HashMngr hashMngr;
        public final FileMngr fileMngr;
        public final boolean downloader;
        public final AtomicInteger playPos;

        public DownloadMngrInit(DownloadMngrConfig config, FileMngr fileMngr, HashMngr hashMngr, boolean downloader, AtomicInteger playPos) {
            this.config = config;
            this.hashMngr = hashMngr;
            this.fileMngr = fileMngr;
            this.downloader = downloader;
            this.playPos = playPos;
        }
    }
}
