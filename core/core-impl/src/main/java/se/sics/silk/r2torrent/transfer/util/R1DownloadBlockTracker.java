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
package se.sics.silk.r2torrent.transfer.util;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import org.javatuples.Pair;
import se.sics.nstream.storage.cache.KHint;
import se.sics.nstream.transfer.BlockMngr;
import se.sics.nstream.transfer.InMemoryBlockMngr;
import se.sics.nstream.util.BlockDetails;
import se.sics.silk.r2torrent.transfer.R1DownloadComp;

/**
 *
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class R1DownloadBlockTracker {

  private final boolean withHashes;
  //**************************************************************************
  private final Map<Integer, BlockMngr> completedBlocks = new HashMap<>();
  /**
   * hasReadyBlocks - locally have the hashes - so I can now download the blocks themselves.
   */
  private final TreeSet<Integer> hashReadyBlocks = new TreeSet<>();
  /**
   * pendingCacheBlocks - blocks that I would like, but not sure yet if the
   * seeder has it available in cache (or at all)
   */
  private final Set<Integer> pendingCacheBlocks = new TreeSet<>();
  /**
   * nextBlocks - blocks the application wants me to get and I didn't get a
   * change to even ask the seeder if it has them
   */
  private final Set<Integer> nextBlocks = new TreeSet<>();
  private final BlockDetails defaultBlockDetails;
  private final Map<Integer, BlockDetails> irregularBlockDetails = new HashMap<>();
  //**************************************************************************
  private final TreeSet<Integer> cachedHashes = new TreeSet<>();
  private final Set<Integer> pendingHashes = new TreeSet<>();
  private final Map<Integer, byte[]> completedHashes = new HashMap<>();
  //**************************************************************************
  private KHint.Summary oldHint = new KHint.Summary(0, new TreeSet<Integer>());
  private boolean cacheHintChanged = false; //new work or finishing a block changes this to true
  private boolean cacheConfirmed = true;
  //**************************************************************************
  private final Map<Integer, BlockMngr> ongoingBlocks = new HashMap<>();
  private final TreeMap<Integer, TreeSet<Integer>> missedPieces = new TreeMap<>();
  private final TreeMap<Integer, Set<Integer>> pendingPieces = new TreeMap<>();

  public R1DownloadBlockTracker(BlockDetails defaultBlocksDetails, boolean withHashes) {
    this.defaultBlockDetails = defaultBlocksDetails;
    this.withHashes = withHashes;
  }

  public void add(Set<Integer> newBlocks, Map<Integer, BlockDetails> newIrregularBlocks) {
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
      cachedHashes.addAll(pendingCacheBlocks);
    } else {
      hashReadyBlocks.addAll(pendingCacheBlocks);
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
    return !cachedHashes.isEmpty();
  }

  public Set<Integer> nextHashes() {
    Set<Integer> nextHash = new TreeSet<>();
    int nrH = R1DownloadComp.HardCodedConfig.BATCHED_HASHES;
    Iterator<Integer> it = cachedHashes.iterator();
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
    hashReadyBlocks.addAll(hashes.keySet());
  }

  public void hashTimeout(Set<Integer> hashes) {
    if (pendingHashes.removeAll(hashes)) {
      cachedHashes.addAll(hashes);
    }
  }

  public void lateHashes(Map<Integer, byte[]> hashes) {
//    for(Map.Entry<Integer, byte[]> hash : hashes.entrySet()) {
//      if(pendingHashes.remove(hash.getKey())) {
//        completedHashes.put(hash.getKey(), hash.getValue());
//      }
//    }
  }

  public boolean hasMissedPiece() {
    return !missedPieces.isEmpty();
  }

  public Pair<Integer, Integer> nextMissedPiece() {
    Map.Entry<Integer, TreeSet<Integer>> nextPiece = missedPieces.firstEntry();
    int block = nextPiece.getKey();
    int piece = nextPiece.getValue().pollFirst();
    addPendingPiece(block, piece);
    if(nextPiece.getValue().isEmpty()) {
      missedPieces.remove(block);
    }
    return Pair.with(block, piece);
  }
  
  public boolean hasBlock() {
    return !hashReadyBlocks.isEmpty();
  }
  
  public Pair<Integer,Integer> nextBlock() {
    int block = hashReadyBlocks.pollFirst();
    BlockDetails bd = irregularBlockDetails.containsKey(block) ? irregularBlockDetails.remove(block)
      : defaultBlockDetails;
    ongoingBlocks.put(block, new InMemoryBlockMngr(bd));
    TreeSet<Integer> pieces = new TreeSet<>();
    for (int i = 0; i < bd.nrPieces; i++) {
      pieces.add(i);
    }
    pendingPieces.put(block, pieces);
    return Pair.with(block, bd.nrPieces);
  }

  public void pieceTimeout(Pair<Integer, Integer> piece) {
    TreeSet<Integer> pieces = missedPieces.get(piece.getValue0());
    if(pieces == null) {
      pieces = new TreeSet<>();
      missedPieces.put(piece.getValue0(), pieces);
    }
    pieces.add(piece.getValue1());
    removePendingPiece(piece);
  }

  public void piece(Pair<Integer, Integer> piece, byte[] val) {
    if (removePendingPiece(piece)) {
      addToBlock(piece, val);
    }
  }

  public void latePiece(Pair<Integer, Integer> piece, byte[] val) {
    if (removePendingPiece(piece)) {
      addToBlock(piece, val);
      return;
    }
    TreeSet<Integer> pieces = missedPieces.get(piece.getValue0());
    if(pieces != null) { 
      if(pieces.remove(piece.getValue1())) {
        addToBlock(piece, val);
      }
      if(pieces.isEmpty()) {
        missedPieces.remove(piece.getValue0());
      }
    }
  }

  public boolean hasComplete() {
    return !completedBlocks.isEmpty();
  }

  //<hashes, blocks>
  public Map<Integer, Pair<byte[], byte[]>> getComplete() {
    Map<Integer, Pair<byte[], byte[]>> result = new HashMap<>();
    completedBlocks.entrySet().stream().
      filter((completedBlock) -> (completedHashes.containsKey(completedBlock.getKey()))).
      forEach((completedBlock) -> {
        byte[] value = completedBlock.getValue().getBlock();
        byte[] hash = completedHashes.get(completedBlock.getKey());
        result.put(completedBlock.getKey(), Pair.with(value, hash));
    });
    result.keySet().stream().forEach((blockNr) -> {
        completedHashes.remove(blockNr);
        completedBlocks.remove(blockNr);
    });
    return result;
  }

  //**************************************************************************
  private KHint.Summary rebuildCacheHint() {
    if (!nextBlocks.isEmpty()) {
      pendingCacheBlocks.addAll(nextBlocks);
      nextBlocks.clear();
    }
    Set<Integer> hint = new TreeSet<>();
    hint.addAll(pendingCacheBlocks);
    hint.addAll(cachedHashes);
    hint.addAll(pendingHashes);
    hint.addAll(hashReadyBlocks);
    hint.addAll(ongoingBlocks.keySet());

    oldHint = new KHint.Summary(oldHint.lStamp + 1, hint);
    cacheHintChanged = false;
    return oldHint.copy();
  }
  
  private void addPendingPiece(Integer block, Integer piece) {
    Set<Integer> pendingBlock = pendingPieces.get(block);
    if (pendingBlock == null) {
      pendingBlock = new HashSet<>();
      pendingPieces.put(block, pendingBlock);
    }
    pendingBlock.add(piece);
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

  public int blockSize() {
    return nextBlocks.size() + pendingCacheBlocks.size() + cachedHashes.size() + pendingHashes.size()
      + hashReadyBlocks.size() + ongoingBlocks.size();
  }
}
