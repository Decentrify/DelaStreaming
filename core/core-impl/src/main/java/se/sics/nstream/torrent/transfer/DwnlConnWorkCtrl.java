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
package se.sics.nstream.torrent.transfer;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import org.javatuples.Pair;
import se.sics.nstream.storage.cache.KHint;
import se.sics.nstream.transfer.BlockMngr;
import se.sics.nstream.transfer.InMemoryBlockMngr;
import se.sics.nstream.util.BlockDetails;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class DwnlConnWorkCtrl {

    private static final int BATCHED_HASHES = 10;

    /**
     * withHashes set to false skips stages 3 and 4
     */
    private final boolean withHashes;
    //**************************************************************************
    private KHint.Summary oldHint = new KHint.Summary(0, new TreeSet<Integer>());
    private boolean cacheHintChanged = false; //new work or finishing a block changes this to true
    private boolean cacheConfirmed = true;
    //**************************************************************************
    /**
     * stage 1. blocks the application wants me to get and I didn't get a change
     * to even ask the seeder if it has them
     */
    private final LinkedList<Integer> nextBlocks = new LinkedList<>();
    /**
     * stage 2. asked for caching - in flight - no answer yet
     */
    private final LinkedList<Integer> pendingCacheBlocks = new LinkedList<>();
    /**
     * stage 3. cache confirmed on seeder side
     */
    private final LinkedList<Integer> awaitingHashes = new LinkedList<>();
    /**
     * stage 4. asked for hash - in flight - no answer yet
     */
    private final Set<Integer> pendingHashes = new HashSet<>();
    /**
     * stage 5. blocks prepared for downloading
     */
    private final Set<Integer> awaitingBlocks = new TreeSet<>();
    /**
     * stage 6 - blocks being downloaded
     */
    private final Map<Integer, BlockMngr> ongoingBlocks = new HashMap<>();
    private final TreeMap<Integer, LinkedList<Integer>> awaitingPieces = new TreeMap<>();
    private final TreeMap<Integer, Set<Integer>> pendingPieces = new TreeMap<>();
    //**************************************************************************
    private final Map<Integer, byte[]> completedHashes = new HashMap<>();
    private final Map<Integer, BlockMngr> completedBlocks = new HashMap<>();
    //**************************************************************************
    private final BlockDetails defaultBlockDetails;
    private final Map<Integer, BlockDetails> irregularBlockDetails = new HashMap<>();
    //**************************************************************************
    private int indicatedPotentialSlots = 0;

    public DwnlConnWorkCtrl(BlockDetails defaultBlocksDetails, boolean withHashes) {
        this.defaultBlockDetails = defaultBlocksDetails;
        this.withHashes = withHashes;
    }

    public int potentialSlots(int perSecondWindowSize) {
        if (indicatedPotentialSlots > 0) {
            return 0;
        }
        int stagedWork1 = ongoingBlocks.size();
        int stagedWork2 = stagedWork1 + pendingHashes.size() + awaitingHashes.size();
        int stagedWork3 = stagedWork2 + pendingCacheBlocks.size() + nextBlocks.size();

        int potentialSlots = 0;
        if (stagedWork3 < 2 * perSecondWindowSize) {
            potentialSlots += BATCHED_HASHES;
        }
        if (stagedWork1 < perSecondWindowSize) {
            potentialSlots *= 2;
        }
        indicatedPotentialSlots += potentialSlots;
        return potentialSlots;
    }

    public void nextBlocks(Set<Integer> newBlocks, Map<Integer, BlockDetails> newIrregularBlocks) {
        indicatedPotentialSlots -= newBlocks.size();
        cacheHintChanged = true;
        nextBlocks.addAll(newBlocks);
        irregularBlockDetails.putAll(newIrregularBlocks);
    }

    public void cacheConfirmed(long ts) {
        if (oldHint.lStamp != ts) {
            return;
        }
        cacheConfirmed = true;
        if (withHashes) {
            awaitingHashes.addAll(pendingCacheBlocks);
        } else {
            awaitingBlocks.addAll(pendingCacheBlocks);
        }
        pendingCacheBlocks.clear();
    }

    public boolean hasNewHint() {
        return cacheConfirmed && cacheHintChanged;
    }

    public KHint.Summary newHint() {
        cacheConfirmed = false;
        return rebuildCacheHint();
    }

    public boolean hasHashes() {
        return !awaitingHashes.isEmpty();
    }

    public Set<Integer> nextHashes() {
        Set<Integer> nextHash = new TreeSet<>();
        int nrH = BATCHED_HASHES;
        Iterator<Integer> it = awaitingHashes.iterator();
        while (it.hasNext() && nrH-- > 0) {
            int hash = it.next();
            nextHash.add(hash);
            pendingHashes.add(hash);
            it.remove();
        }
        return nextHash;
    }

    public void hashes(Map<Integer, byte[]> hashes) {
        pendingHashes.removeAll(hashes.keySet());
        completedHashes.putAll(hashes);
        awaitingBlocks.addAll(hashes.keySet());
    }

    public void hashTimeout(Set<Integer> hashes) {
        if (pendingHashes.removeAll(hashes)) {
            awaitingHashes.addAll(hashes);
        }
    }

    public void lateHashes(Map<Integer, byte[]> lateHashes) {
        //wait for next batch
    }

    public boolean hasPiece() {
        if (awaitingPieces.isEmpty()) {
            newWorkPieces();
        }
        return !awaitingPieces.isEmpty();
    }

    public Pair<Integer, Integer> nextPiece() {
        Pair<Integer, Integer> nextPiece = removeNextPiece();
        addPendingPiece(nextPiece);
        return nextPiece;
    }

    public void pieceTimeout(Pair<Integer, Integer> piece) {
        if (removePendingPiece(piece)) {
            addNextPiece(piece);
        }
    }

    public void piece(Pair<Integer, Integer> piece, byte[] val) {
        if (removePendingPiece(piece)) {
            addToBlock(piece, val);
        }
    }

    public void latePiece(Pair<Integer, Integer> piece, byte[] val) {
        if (removePendingPiece(piece)) {
            addToBlock(piece, val);
        }
        if (removeNextPiece(piece)) {
            addToBlock(piece, val);
        }
    }

    public boolean hasComplete() {
        return !completedHashes.isEmpty() || !completedBlocks.isEmpty();
    }

    //<hashes, blocks>
    public Pair<Map<Integer, byte[]>, Map<Integer, byte[]>> getComplete() {
        Map<Integer, byte[]> hResult = new HashMap<>(completedHashes);
        completedHashes.clear();

        Map<Integer, byte[]> bResult = new HashMap<>();
        for (Map.Entry<Integer, BlockMngr> completedBlock : completedBlocks.entrySet()) {
            byte[] blockValue = completedBlock.getValue().getBlock();
            bResult.put(completedBlock.getKey(), blockValue);
        }
        completedBlocks.clear();
        return Pair.with(hResult, bResult);
    }

    //**************************************************************************
    private void newWorkPieces() {
        if (!awaitingBlocks.isEmpty()) {
            newPendingBlock(awaitingHashes.removeFirst());
        }
    }

    private void newPendingBlock(int blockNr) {
        BlockDetails bd = irregularBlockDetails.containsKey(blockNr) ? irregularBlockDetails.remove(blockNr) : defaultBlockDetails;
        BlockMngr blockMngr = new InMemoryBlockMngr(bd);
        ongoingBlocks.put(blockNr, blockMngr);
        LinkedList<Integer> pieceList = new LinkedList<>();
        for (int i = 0; i < bd.nrPieces; i++) {
            pieceList.add(i);
        }
        awaitingPieces.put(blockNr, pieceList);
    }

    private KHint.Summary rebuildCacheHint() {
        pendingCacheBlocks.addAll(nextBlocks);
        nextBlocks.clear();
        
        Set<Integer> hint = new TreeSet<>();
        hint.addAll(ongoingBlocks.keySet());
        hint.addAll(awaitingBlocks);
        hint.addAll(awaitingHashes);
        hint.addAll(pendingHashes);
        hint.addAll(pendingCacheBlocks);
        oldHint = new KHint.Summary(oldHint.lStamp + 1, hint);
        cacheHintChanged = false;
        return oldHint.copy();
    }

    private void addNextPiece(Pair<Integer, Integer> piece) {
        LinkedList<Integer> nextBlock = awaitingPieces.get(piece.getValue0());
        if (nextBlock == null) {
            nextBlock = new LinkedList<>();
            awaitingPieces.put(piece.getValue0(), nextBlock);
        }
        nextBlock.add(piece.getValue1());
    }

    private Pair<Integer, Integer> removeNextPiece() {
        Map.Entry<Integer, LinkedList<Integer>> nextBlock = awaitingPieces.firstEntry();
        Integer nextPiece = nextBlock.getValue().removeFirst();
        if (nextBlock.getValue().isEmpty()) {
            awaitingPieces.remove(nextBlock.getKey());
        }
        return Pair.with(nextBlock.getKey(), nextPiece);
    }

    private boolean removeNextPiece(Pair<Integer, Integer> piece) {
        boolean result = false;
        LinkedList<Integer> block = awaitingPieces.get(piece.getValue0());
        if (block != null) {
            result = block.remove(piece.getValue1());
            if (block.isEmpty()) {
                awaitingPieces.remove(piece.getValue0());
            }
        }
        return result;
    }

    private void addPendingPiece(Pair<Integer, Integer> piece) {
        Set<Integer> pendingBlock = pendingPieces.get(piece.getValue0());
        if (pendingBlock == null) {
            pendingBlock = new HashSet<>();
            pendingPieces.put(piece.getValue0(), pendingBlock);
        }
        pendingBlock.add(piece.getValue1());
    }

    private Boolean removePendingPiece(Pair<Integer, Integer> piece) {
        Set<Integer> pendingBlock = pendingPieces.get(piece.getValue0());
        if (pendingBlock == null) {
            return false;
        }
        boolean result = pendingBlock.remove(piece.getValue1());
        if (pendingBlock.isEmpty()) {
            pendingPieces.remove(piece.getValue0());
        }
        return result;
    }

    private void addToBlock(Pair<Integer, Integer> piece, byte[] val) {
        BlockMngr bm = ongoingBlocks.get(piece.getValue0());
        if (bm == null) {
            return;
        }
        if (bm.hasPiece(piece.getValue1())) {
            return;
        }
        bm.writePiece(piece.getValue1(), val);
        if (bm.isComplete()) {
            cacheHintChanged = true;
            ongoingBlocks.remove(piece.getValue0());
            completedBlocks.put(piece.getValue0(), bm);
        }
    }

    public int workloadSize() {
        return ongoingBlocks.size() + awaitingBlocks.size() + awaitingHashes.size() + pendingHashes.size() + pendingCacheBlocks.size() + nextBlocks.size() + completedBlocks.size();
    }
}
