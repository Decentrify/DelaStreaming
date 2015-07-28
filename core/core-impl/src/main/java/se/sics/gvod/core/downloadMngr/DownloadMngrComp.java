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
import se.sics.p2ptoolbox.util.managedStore.BlockMngr;
import se.sics.p2ptoolbox.util.managedStore.FileMngr;
import se.sics.p2ptoolbox.util.managedStore.HashMngr;
import se.sics.p2ptoolbox.util.managedStore.StorageMngrFactory;

/**
 * @author Alex Ormenisan <aaor@sics.se>
 */
public class DownloadMngrComp extends ComponentDefinition {

    private static final Logger LOG = LoggerFactory.getLogger(DownloadMngrComp.class);
    private final DownloadMngrConfig config;
    private final String logPrefix;

    private Negative<UtilityUpdatePort> utilityUpdate = provides(UtilityUpdatePort.class);
    private Negative<DownloadMngrPort> dataPort = provides(DownloadMngrPort.class);
    private Positive<Timer> timer = requires(Timer.class);
    private Positive<ConnMngrPort> connMngr = requires(ConnMngrPort.class);

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
    private UUID periodicUpdateSelfTId = null;

    public DownloadMngrComp(DownloadMngrInit init) {
        this.config = init.config;
        this.logPrefix = config.getSelf().getBase() + "<" + config.overlayId + ">";
        LOG.info("{} initiating", logPrefix);

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

            Integer downloadPos = fileMngr.contiguous(0);
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
            //TODO Alex might need to move it to its own timeout
            checkCompleteBlocks();
            if (hashMngr.isComplete(0) && fileMngr.isComplete(0)) {
                finishDownload();
            }
            
            LOG.info("{} hashComplete:{} fileComplete:{}", new Object[]{logPrefix, hashMngr.isComplete(0), fileMngr.isComplete(0)});
            LOG.info("{} pending pieces:{} pendingHashes:{} pendingBlocks:{}", new Object[]{logPrefix, pendingPieces.size(), pendingHashes.size(), queuedBlocks.keySet()});
            LOG.info("{} nextPieces:{} nextHashes:{}", new Object[]{logPrefix, nextPieces.size(), nextHashes.size()});
            
            int playPieceNr = playPos.get();
            playPieceNr = (playPieceNr == -1 ? 0 : playPieceNr);
            int playBlockNr = pieceIdToBlockNrPieceNr(playPieceNr).getValue0();
            int downloadBlockNr = fileMngr.contiguous(playBlockNr);
            LOG.info("{} playBlockPos:{} blockDPos:{} 0DPos:{} 0DPos:{}",
                    new Object[]{logPrefix, playBlockNr, downloadBlockNr, fileMngr.contiguous(0), hashMngr.contiguous(0)});
            trigger(new UtilityUpdate(config.overlayId, downloading, downloadBlockNr), utilityUpdate);
            
            if(!downloading) {
                cancelUpdateSelf();
            }
        }
    };

    private Handler handleDataRequest = new Handler<Data.Req>() {

        @Override
        public void handle(Data.Req req) {
            LOG.debug("{} received local data request for readPos:{} readSize:{}", new Object[]{logPrefix, req.readPos, req.readBlockSize});

            if (!fileMngr.has(req.readPos, req.readBlockSize)) {
                Set<Integer> targetedBlocks = posToBlockNr(req.readPos, req.readBlockSize);
                for (Integer blockNr : targetedBlocks) {
                    if (queuedBlocks.containsKey(blockNr)) {
                        checkCompleteBlocks();
                        break;
                    }
                }
                if (!fileMngr.has(req.readPos, req.readBlockSize)) {
                    LOG.debug("{} local data missing - readPos:{} , readSize:{}", new Object[]{logPrefix, req.readPos, req.readBlockSize});
                    trigger(new Data.Resp(req, ReqStatus.MISSING, null), dataPort);
                    return;
                }
            }
            LOG.debug("{} sending local data - readPos:{} readSize:{}", new Object[]{logPrefix, req.readPos, req.readBlockSize});
            byte data[] = fileMngr.read(req.readPos, req.readBlockSize);
            trigger(new Data.Resp(req, ReqStatus.SUCCESS, data), dataPort);
        }
    };

    private Handler handleHashRequest = new Handler<Download.HashRequest>() {

        @Override
        public void handle(Download.HashRequest req) {
            LOG.debug("{} received hash request:{}", logPrefix, req.hashes);

            Map<Integer, byte[]> hashes = new HashMap<Integer, byte[]>();
            Set<Integer> missingHashes = new HashSet<Integer>();

            for (Integer hash : req.hashes) {
                if (hashMngr.hasHash(hash)) {
                    hashes.put(hash, hashMngr.readHash(hash));
                } else {
                    missingHashes.add(hash);
                }
            }
            LOG.debug("{} sending hashes:{} missing hashes:{}", new Object[]{logPrefix, hashes.keySet(), missingHashes});
            trigger(req.success(hashes, missingHashes), connMngr);
        }
    };

    private Handler handleHashResponse = new Handler<Download.HashResponse>() {

        @Override
        public void handle(Download.HashResponse resp) {
            switch (resp.status) {
                case SUCCESS:
                    LOG.debug("{} SUCCESS hashes:{} missing hashes:{}", new Object[]{logPrefix, resp.hashes.keySet(), resp.missingHashes});

                    for (Map.Entry<Integer, byte[]> hash : resp.hashes.entrySet()) {
                        hashMngr.writeHash(hash.getKey(), hash.getValue());
                    }

                    pendingHashes.removeAll(resp.hashes.keySet());
                    pendingHashes.removeAll(resp.missingHashes);
                    nextHashes.addAll(resp.missingHashes);
                    download();
                    return;
                case TIMEOUT:
                    LOG.info("{} TIMEOUT hashes:{}", logPrefix, resp.missingHashes);
                    pendingHashes.removeAll(resp.missingHashes);
                    nextHashes.addAll(resp.missingHashes);
                    download();
                    return;
                case BUSY:
                    LOG.info("{} BUSY hashes:{}", logPrefix, resp.missingHashes);
                    pendingHashes.removeAll(resp.missingHashes);
                    nextHashes.addAll(resp.missingHashes);
                    return;
                default:
                    LOG.warn("{} illegal status:{}, ignoring", new Object[]{logPrefix, resp.status});
            }
        }
    };

    private Handler handleDownloadDataRequest = new Handler<Download.DataRequest>() {

        @Override
        public void handle(Download.DataRequest req) {
            LOG.debug("{} received data request:{}", logPrefix, req.pieceId);

            if (fileMngr.hasPiece(req.pieceId)) {
                byte[] piece = fileMngr.readPiece(req.pieceId);
                LOG.debug("{} sending data:{}", new Object[]{logPrefix, req.pieceId});
                trigger(req.success(piece), connMngr);
            } else {
                LOG.warn("{} missing data:{}", new Object[]{logPrefix, req.pieceId});
                trigger(req.missingPiece(), connMngr);
            }
        }
    };

    private Handler handleDownloadDataResponse = new Handler<Download.DataResponse>() {

        @Override
        public void handle(Download.DataResponse resp) {
            switch (resp.status) {
                case SUCCESS:
                    LOG.debug("{} SUCCESS piece:{}", new Object[]{logPrefix, resp.pieceId});

                    Pair<Integer, Integer> pieceIdToBlockNr = pieceIdToBlockNrPieceNr(resp.pieceId);
                    BlockMngr block = queuedBlocks.get(pieceIdToBlockNr.getValue0());
                    if (block == null) {
                        LOG.warn("{} block is null - inconsistency", logPrefix);
                        return;
                    }
                    block.writePiece(pieceIdToBlockNr.getValue1(), resp.piece);
                    pendingPieces.remove(resp.pieceId);
                    download();
                    return;
                case TIMEOUT:
                case MISSING:
                    LOG.info("{} MISSING/TIMEOUT piece:{}", new Object[]{logPrefix, resp.pieceId});
                    pendingPieces.remove(resp.pieceId);
                    nextPieces.add(resp.pieceId);
                    return;
                case BUSY:
                    LOG.info("{} BUSY piece:{}", new Object[]{logPrefix, resp.pieceId});
                    pendingPieces.remove(resp.pieceId);
                    nextPieces.add(resp.pieceId);
                    return;
                default:
                    LOG.warn("{} illegal status:{} ignoring", new Object[]{logPrefix, resp.status});
            }
        }
    };

    private void checkCompleteBlocks() {
        Set<Integer> completedBlocks = new HashSet<Integer>();
        Set<Integer> resetBlocks = new HashSet<Integer>();
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
                LOG.warn("{} hash problem, dropping block:{}", logPrefix, blockNr);
                resetBlocks.add(blockNr);
            }
        }
        for (Integer blockNr : completedBlocks) {
            queuedBlocks.remove(blockNr);
        }
        for (Integer blockNr : resetBlocks) {
            int blockSize = fileMngr.blockSize(blockNr);
            BlockMngr blankBlock = StorageMngrFactory.getSimpleBlockMngr(blockSize, config.pieceSize);
            queuedBlocks.put(blockNr, blankBlock);
            for (int i = 0; i < blankBlock.nrPieces(); i++) {
                int pieceId = blockNr * config.piecesPerBlock + i;
                nextPieces.add(pieceId);
            }
        }
    }

    private Integer posToPieceId(long pos) {
        Integer pieceId = (int) (pos / config.pieceSize);
        return pieceId;
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

    private boolean download() {
        int currentPlayPiece = playPos.get();
        int currentPlayBlock = pieceIdToBlockNrPieceNr(currentPlayPiece).getValue0();
        if (nextHashes.isEmpty() && nextPieces.isEmpty()) {
            if (fileMngr.isComplete(currentPlayBlock)) {
                currentPlayPiece = 0;
                playPos.set(0);
            }
            if (fileMngr.isComplete(0)) {
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
                return false;
            }
        }
        return true;
    }

    private boolean getNewPieces(int currentBlockNr) {
        LOG.debug("{} getting new pieces from block:{}", logPrefix, currentBlockNr);
        int filePos = fileMngr.contiguous(currentBlockNr);
        int hashPos = hashMngr.contiguous(0);

        if (filePos + 5 * config.minHashAhead > hashPos + pendingHashes.size()) {
            Set<Integer> except = new HashSet<Integer>();
            except.addAll(pendingHashes);
            except.addAll(nextHashes);
            Set<Integer> newNextHashes = hashMngr.nextHashes(config.hashesPerMsg, 0, except);
            LOG.debug("{} hashPos:{} pendingHashes:{} nextHashes:{} newNextHashes:{}", 
                    new Object[]{logPrefix, hashPos, pendingHashes, nextHashes, newNextHashes});
            nextHashes.addAll(newNextHashes);
            if (!nextHashes.isEmpty()) {
                return true;
            }
        }

        Integer nextBlockNr = fileMngr.nextBlock(currentBlockNr, queuedBlocks.keySet());
        if (nextBlockNr == null) {
            LOG.debug("{} last blockNr:{}", logPrefix, currentBlockNr);
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
        LOG.debug("{} downloading piece:{}", logPrefix, nextPieceId);
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
        LOG.debug("{} downloading hashes:{} targetPos:{}", new Object[]{logPrefix, hashesToDownload, targetPos});
        trigger(new Download.HashRequest(UUID.randomUUID(), targetPos, hashesToDownload), connMngr);
        pendingHashes.addAll(hashesToDownload);
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
