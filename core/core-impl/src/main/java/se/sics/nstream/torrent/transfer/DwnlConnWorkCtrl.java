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

    //**************************************************************************
    private final Map<Integer, BlockMngr> completedBlocks = new HashMap<>();
    /**
     * cachedBlocks - blocks that have been confirmed as being available and
     * cached on the seeder side
     */
    private final LinkedList<Integer> cachedBlocks = new LinkedList<>();
    /**
     * pendingCacheBlocks - blocks that I would like, but not sure yet if the
     * seeder has it available in cache (or at all)
     */
    private final LinkedList<Integer> pendingCacheBlocks = new LinkedList<>();
    /**
     * nextBlocks - blocks the application wants me to get and I didn't get a
     * change to even ask the seeder if it has them
     */
    private final LinkedList<Integer> nextBlocks = new LinkedList<>();
    private final BlockDetails defaultBlockDetails;
    private final Map<Integer, BlockDetails> irregularBlockDetails = new HashMap<>();
    //**************************************************************************
    private KHint.Summary oldHint = new KHint.Summary(0, new TreeSet<Integer>());
    private boolean cacheHintChanged = false; //new work or finishing a block changes this to true
    private boolean cacheConfirmed = true;
    //**************************************************************************
    private final Map<Integer, BlockMngr> ongoingBlocks = new HashMap<>();
    private final TreeMap<Integer, LinkedList<Integer>> nextPieces = new TreeMap<>();
    private final TreeMap<Integer, Set<Integer>> pendingPieces = new TreeMap<>();

    public DwnlConnWorkCtrl(BlockDetails defaultBlocksDetails) {
        this.defaultBlockDetails = defaultBlocksDetails;
    }

    public void add(Set<Integer> newBlocks, Map<Integer, BlockDetails> newIrregularBlocks) {
        cacheHintChanged = true;
        nextBlocks.addAll(newBlocks);
        irregularBlockDetails.putAll(newIrregularBlocks);
    }

    public void cacheConfirmed() {
        cacheConfirmed = true;
        cachedBlocks.addAll(oldHint.blocks);
        pendingCacheBlocks.removeAll(oldHint.blocks);
    }

    public boolean hasNewHint() {
        return cacheConfirmed && cacheHintChanged;
    }

    public KHint.Summary newHint() {
        cacheConfirmed = false;
        return rebuildCacheHint();
    }

    public boolean hasNextPiece() {
        if (nextPieces.isEmpty()) {
            newWorkPieces();
        }
        return !nextPieces.isEmpty();
    }

    public Pair<Integer, Integer> next() {
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
        if(removeNextPiece(piece)) {
            addToBlock(piece, val);
        }
    }

    public boolean hasComplete() {
        return !completedBlocks.isEmpty();
    }

    public Map<Integer, byte[]> getCompleteBlocks() {
        Map<Integer, byte[]> result = new HashMap<>();
        for (Map.Entry<Integer, BlockMngr> completedBlock : completedBlocks.entrySet()) {
            byte[] blockValue = completedBlock.getValue().getBlock();
            result.put(completedBlock.getKey(), blockValue);
        }
        return result;
    }

    //**************************************************************************
    private void newWorkPieces() {
        if (!cachedBlocks.isEmpty()) {
            newPendingBlock(cachedBlocks.removeFirst());
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
        nextPieces.put(blockNr, pieceList);
    }

    private KHint.Summary rebuildCacheHint() {
        if (!nextBlocks.isEmpty()) {
            pendingCacheBlocks.addAll(nextBlocks);
            nextBlocks.clear();
        }
        Set<Integer> hint = new TreeSet<>();
        hint.addAll(ongoingBlocks.keySet());
        hint.addAll(cachedBlocks);
        hint.addAll(pendingCacheBlocks);
        oldHint = new KHint.Summary(oldHint.lStamp + 1, hint);
        cacheHintChanged = false;
        return oldHint;
    }

    private void addNextPiece(Pair<Integer, Integer> piece) {
        LinkedList<Integer> nextBlock = nextPieces.get(piece.getValue0());
        if (nextBlock == null) {
            nextBlock = new LinkedList<>();
            nextPieces.put(piece.getValue0(), nextBlock);
        }
        nextBlock.add(piece.getValue1());
    }

    private Pair<Integer, Integer> removeNextPiece() {
        Map.Entry<Integer, LinkedList<Integer>> nextBlock = nextPieces.firstEntry();
        Integer nextPiece = nextBlock.getValue().removeFirst();
        if (nextBlock.getValue().isEmpty()) {
            nextPieces.remove(nextBlock.getKey());
        }
        return Pair.with(nextBlock.getKey(), nextPiece);
    }

    private boolean removeNextPiece(Pair<Integer, Integer> piece) {
        boolean result = false;
        LinkedList<Integer> block = nextPieces.get(piece.getValue0());
        if (block != null) {
            result = block.remove(piece.getValue1());
            if (block.isEmpty()) {
                nextPieces.remove(piece.getValue0());
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

    private boolean removePendingPiece(Pair<Integer, Integer> piece) {
        Set<Integer> pendingBlock = pendingPieces.get(piece.getValue0());
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
}
