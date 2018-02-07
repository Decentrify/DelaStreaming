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
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Consumer;
import org.javatuples.Pair;
import se.sics.kompics.KompicsEvent;
import se.sics.ktoolbox.util.identifiable.basic.IntIdFactory;
import se.sics.ktoolbox.util.reference.KReference;
import se.sics.ktoolbox.util.reference.KReferenceException;
import se.sics.nstream.util.BlockDetails;
import se.sics.nstream.util.BlockHelper;
import se.sics.nstream.util.range.KPiece;
import se.sics.nstream.util.range.RangeKReference;
import se.sics.silk.r2torrent.transfer.msgs.R1TransferMsgs;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class R1UpldCwnd {

  public final BlockDetails defaultBlock;
  public final int cwndSize;
  public final IntIdFactory intIdFactory;

  private final Map<Integer, BlockDetails> irregularBlocks = new HashMap<>();
  private final Map<Integer, KReference<byte[]>> servedBlocks = new HashMap<>();
  private final Map<Integer, byte[]> servedHashes = new HashMap<>();

  private final List<R1TransferMsgs.PieceReq> pendingPieces = new LinkedList<>();
  private final Consumer<KompicsEvent> sendMsg;

  public R1UpldCwnd(BlockDetails defaultBlock, int cwndSize, Consumer<KompicsEvent> sendMsg, IntIdFactory intIdFactory) {
    this.defaultBlock = defaultBlock;
    this.cwndSize = cwndSize;
    this.sendMsg = sendMsg;
    this.intIdFactory = intIdFactory;
  }

  public void pendingBlock(R1TransferMsgs.BlockReq req) {
    req.pieces(addPending, intIdFactory);
  }

  public void pendingPiece(R1TransferMsgs.PieceReq req) {
    pendingPieces.add(req);
  }

  public void send() {
    if (pendingPieces.isEmpty()) {
      return;
    }
    Iterator<R1TransferMsgs.PieceReq> it = pendingPieces.iterator();
    int batch = cwndSize;
    while (batch > 0 && it.hasNext()) {
      batch--;
      R1TransferMsgs.PieceReq req = it.next();
      it.remove();
      RangeKReference pieceVal = getPiece(req.piece);
      sendMsg.accept(req.accept(pieceVal));
    }
  }

  public Map<Integer, byte[]> getHashes(Set<Integer> hashes) {
    Map<Integer, byte[]> hashValues = new TreeMap<>();
    for (Integer hashNr : hashes) {
      byte[] hashVal = servedHashes.get(hashNr);
      if (hashVal == null) {
        throw new RuntimeException("bad request");
      }
      hashValues.put(hashNr, hashVal);
    }
    return hashValues;
  }

  public Set<Integer> servedBlocks() {
    return servedBlocks.keySet();
  }

  public void serveBlocks(int blockNr, KReference<byte[]> block, byte[] hash, Optional<BlockDetails> irregularBlock) {
    servedBlocks.put(blockNr, block);
    servedHashes.put(blockNr, hash);
    if (irregularBlock.isPresent()) {
      irregularBlocks.put(blockNr, irregularBlock.get());
    }
  }

  public void releaseBlock(Set<Integer> blocks) {
    for (Integer blockNr : blocks) {
      KReference<byte[]> block = servedBlocks.remove(blockNr);
      servedHashes.remove(blockNr);
      irregularBlocks.remove(blockNr);
      silentRelease(block);
    }
  }

  public void clear() {
    for (KReference<byte[]> block : servedBlocks.values()) {
      silentRelease(block);
    }
    servedBlocks.clear();
  }

  private final Consumer<R1TransferMsgs.PieceReq> addPending = new Consumer<R1TransferMsgs.PieceReq>() {

    @Override
    public void accept(R1TransferMsgs.PieceReq pieceReq) {
      pendingPieces.add(pieceReq);
    }
  };

  private RangeKReference getPiece(Pair<Integer, Integer> piece) {
    int blockNr = piece.getValue0();
    int pieceNr = piece.getValue1();
    BlockDetails blockDetails = irregularBlocks.containsKey(blockNr) ? irregularBlocks.get(blockNr) : defaultBlock;
    KReference<byte[]> block = servedBlocks.get(blockNr);
    if (block == null) {
      return null;
    }
    KPiece pieceRange = BlockHelper.getPieceRange(piece, blockDetails, defaultBlock);
    //retain block here(range create) - release in serializer
    RangeKReference pieceVal
      = RangeKReference.createInstance(block, BlockHelper.getBlockPos(blockNr, defaultBlock), pieceRange);
    return pieceVal;
  }

  private void silentRelease(KReference<byte[]> ref) {
    try {
      ref.release();
    } catch (KReferenceException ex) {
      throw new RuntimeException(ex);
    }
  }
}
