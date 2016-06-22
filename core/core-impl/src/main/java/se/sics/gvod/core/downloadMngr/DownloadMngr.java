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
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.javatuples.Pair;
import org.javatuples.Quintet;
import se.sics.ktoolbox.util.managedStore.core.BlockMngr;
import se.sics.ktoolbox.util.managedStore.core.FileMngr;
import se.sics.ktoolbox.util.managedStore.core.HashMngr;
import se.sics.ktoolbox.util.managedStore.core.impl.StorageMngrFactory;
import se.sics.ktoolbox.util.managedStore.core.util.HashUtil;
/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class DownloadMngr {

    private final DownloadMngrKCWrapper config;

    private final HashMngr hashMngr;
    private final FileMngr fileMngr;

    private final Map<Integer, BlockMngr> queuedBlocks = new HashMap<>();
    private final Set<Integer> pendingPieces = new HashSet<>();
    private final ArrayList<Integer> nextPieces = new ArrayList<>();
    private final Set<Integer> pendingHashes = new HashSet<>();
    private final ArrayList<Integer> nextHashes = new ArrayList<>();

    public DownloadMngr(DownloadMngrKCWrapper config, HashMngr hashMngr, FileMngr fileMngr) {
        this.config = config;
        this.hashMngr = hashMngr;
        this.fileMngr = fileMngr;
    }

    //TODO Alex - fix to return max known block and not just first block
    public Optional<ByteBuffer> dataRequest(long readPos, int size) {
        if (!fileMngr.has(readPos, size)) {
            Set<Integer> targetedBlocks = posToBlockNr(readPos, size);
            for (Integer blockNr : targetedBlocks) {
                if (queuedBlocks.containsKey(blockNr)) {
                    checkCompleteBlocks();
                    break;
                }
            }
            if (!fileMngr.has(readPos, size)) {
                return Optional.absent();
            }
        }
        ByteBuffer data = fileMngr.read(null, readPos, size, null);
        return Optional.of(data);
    }
    
    public Optional<ByteBuffer> dataRequest(int pieceId) {
        if (fileMngr.hasPiece(pieceId)) {
            return Optional.of(fileMngr.readPiece(null, pieceId, null));
        }
        return Optional.absent();
    }

    public void dataResponse(int pieceId, Optional<ByteBuffer> piece) {
        pendingPieces.remove(pieceId);
        if(!piece.isPresent()) {
            nextPieces.add(pieceId);
            return;
        }
        Pair<Integer, Integer> blockPiece = PieceBlockHelper.pieceIdToBlockNrPieceNr(pieceId, config.piecesPerBlock);
        BlockMngr block = queuedBlocks.get(blockPiece.getValue0());
        if (block == null) {
            //TODO Alex - logic inconsistency - how to deal with
            throw new RuntimeException("block logic error");
        }
        block.writePiece(blockPiece.getValue1(), piece.get().array());
    }

    public Pair<Map<Integer, ByteBuffer>, Set<Integer>> hashRequest(Set<Integer> requestedHashes) {
        Map<Integer, ByteBuffer> hashes = new HashMap<>();
        Set<Integer> missingHashes = new HashSet<>();
        for (Integer hash : requestedHashes) {
            if (hashMngr.hasHash(hash)) {
                hashes.put(hash, hashMngr.readHash(hash));
            } else {
                missingHashes.add(hash);
            }
        }
        return Pair.with(hashes, missingHashes);
    }

    public void hashResponse(Map<Integer, ByteBuffer> hashes, Set<Integer> missingHashes) {
        for (Map.Entry<Integer, ByteBuffer> hash : hashes.entrySet()) {
            hashMngr.writeHash(hash.getKey(), hash.getValue().array());
        }

        pendingHashes.removeAll(hashes.keySet());
        pendingHashes.removeAll(missingHashes);
        nextHashes.addAll(missingHashes);
    }

    private Set<Integer> posToBlockNr(long pos, int size) {
        Set<Integer> result = new HashSet<>();
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

    public Pair<Set<Integer>, Map<Integer, ByteBuffer>> checkCompleteBlocks() {
        Set<Integer> completedBlocks = new HashSet<>();
        Map<Integer, ByteBuffer> resetBlocks = new HashMap<>();
        for (Map.Entry<Integer, BlockMngr> block : queuedBlocks.entrySet()) {
            int blockNr = block.getKey();
            if (!block.getValue().isComplete()) {
                continue;
            }
            if (!hashMngr.hasHash(blockNr)) {
                continue;
            }
            ByteBuffer blockBytes = ByteBuffer.wrap(block.getValue().getBlock());
            ByteBuffer blockHash = hashMngr.readHash(blockNr);
            if (HashUtil.checkHash(config.hashAlg, blockBytes.array(), blockHash.array())) {
                fileMngr.writeBlock(blockNr, blockBytes);
                completedBlocks.add(blockNr);
            } else {
                //TODO Alex - might need to re-download hash as well
                resetBlocks.put(blockNr, blockBytes);
            }
        }
        for (Integer blockNr : completedBlocks) {
            queuedBlocks.remove(blockNr);
        }
        for (Integer blockNr : resetBlocks.keySet()) {
            int blockSize = fileMngr.blockSize(blockNr);
            BlockMngr blankBlock = StorageMngrFactory.inMemoryBlockMngr(blockSize, config.pieceSize);
            queuedBlocks.put(blockNr, blankBlock);
            for (int i = 0; i < blankBlock.nrPieces(); i++) {
                int pieceId = blockNr * config.piecesPerBlock + i;
                nextPieces.add(pieceId);
            }
        }
        return Pair.with(completedBlocks, resetBlocks);
    }

    public boolean download(int blockNr) {
        if (nextHashes.isEmpty() && nextPieces.isEmpty()) {
            if (fileMngr.isComplete(blockNr)) {
                if (fileMngr.isComplete(0)) {
                    return false;
                }
                blockNr = 0;
            }
            if (!getNewPieces(blockNr)) {
                if (!getNewPieces(0)) {
                    return false;
                }
            }
        }
        return true;
    }

    private boolean getNewPieces(int currentBlockNr) {
        int filePos = fileMngr.contiguous(currentBlockNr);
        int hashPos = hashMngr.contiguous(0);

        if (filePos + 5 * config.minHashAhead > hashPos + pendingHashes.size()) {
            Set<Integer> except = new HashSet<>();
            except.addAll(pendingHashes);
            except.addAll(nextHashes);
            Set<Integer> newNextHashes = hashMngr.nextHashes(config.hashesPerMsg, 0, except);
            nextHashes.addAll(newNextHashes);
            if (!nextHashes.isEmpty()) {
                return true;
            }
        }

        Integer nextBlockNr = fileMngr.nextBlock(currentBlockNr, queuedBlocks.keySet());
        if (nextBlockNr == null) {
            return false;
        }
        //last block might have less nr of pieces than default
        int blockSize = fileMngr.blockSize(nextBlockNr);
        BlockMngr blankBlock = StorageMngrFactory.inMemoryBlockMngr(blockSize, config.pieceSize);
        queuedBlocks.put(nextBlockNr, blankBlock);
        for (int i = 0; i < blankBlock.nrPieces(); i++) {
            int pieceId = nextBlockNr * config.piecesPerBlock + i;
            nextPieces.add(pieceId);
        }
        return !nextPieces.isEmpty();
    }

    public Optional<Integer> downloadData() {
        if (nextPieces.isEmpty()) {
            return Optional.absent();
        }
        Integer nextPiece = nextPieces.remove(0);
        pendingPieces.add(nextPiece);
        return Optional.of(nextPiece);
    }

    public Optional<Set<Integer>> downloadHash() {
        if (nextHashes.isEmpty()) {
            return Optional.absent();
        }
        Set<Integer> downloadHashes = new HashSet<>();
        for (int i = 0; i < config.hashesPerMsg && !nextHashes.isEmpty(); i++) {
            downloadHashes.add(nextHashes.remove(0));
        }
        pendingHashes.addAll(downloadHashes);
        return Optional.of(downloadHashes);
    }

    public int contiguousBlocks(int fromBlockNr) {
        return fileMngr.contiguous(fromBlockNr);
    }

    public boolean isComplete() {
        return hashMngr.isComplete(0) && fileMngr.isComplete(0);
    }

    @Override
    public String toString() {
        String status = "";
        status += "hash complete:" + hashMngr.isComplete(0) + " file complete:" + fileMngr.isComplete(0) + "\n";
        status += "pending hashes:" + pendingHashes.size() + " pending pieces:" + pendingPieces.size() + "\n";
        status += "next hashes:" + nextHashes.size() + " next pieces:" + nextPieces.size() + "\n";
        status += "queued blocks:" + queuedBlocks.keySet();
        return status;
    }
    
    /**
     * @return <blocks, pendingHashes, nextHashes, pendingPieces, nextPieces>
     */
    public Quintet publishState() {
        return Quintet.with(queuedBlocks.size(), pendingHashes.size(), nextHashes.size(), pendingPieces.size(), nextPieces.size());
    }
}
