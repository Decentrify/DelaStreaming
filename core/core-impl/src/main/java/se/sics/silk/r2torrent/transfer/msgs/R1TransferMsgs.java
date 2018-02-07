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
package se.sics.silk.r2torrent.transfer.msgs;

import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import org.javatuples.Pair;
import se.sics.kompics.util.Identifier;
import se.sics.ktoolbox.util.Either;
import se.sics.ktoolbox.util.identifiable.BasicBuilders;
import se.sics.ktoolbox.util.identifiable.BasicIdentifiers;
import se.sics.ktoolbox.util.identifiable.basic.IntIdFactory;
import se.sics.ktoolbox.util.identifiable.basic.PairIdentifier;
import se.sics.ktoolbox.util.identifiable.overlay.OverlayId;
import se.sics.ktoolbox.util.reference.KReference;
import se.sics.nstream.storage.cache.KHint;
import se.sics.nstream.util.range.RangeKReference;
import se.sics.silk.event.SilkEvent;
import se.sics.silk.r2torrent.transfer.events.R1TransferMsg;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public interface R1TransferMsgs {

  public static class CacheHintReq extends SilkEvent.E4 implements R1TransferMsg.Upld {

    public final KHint.Summary cacheHint;

    CacheHintReq(Identifier eventId, OverlayId torrentId, Identifier fileId, KHint.Summary cacheHint) {
      super(eventId, torrentId, fileId);
      this.cacheHint = cacheHint;
    }

    public CacheHintReq(OverlayId torrentId, Identifier fileId, KHint.Summary cacheHint) {
      this(BasicIdentifiers.msgId(), torrentId, fileId, cacheHint);
    }

    public CacheHintAcc accept() {
      return new CacheHintAcc(eventId, torrentId, fileId, cacheHint);
    }
  }

  public static class CacheHintAcc extends SilkEvent.E4 implements R1TransferMsg.Dwnl {

    public final KHint.Summary cacheHint;

    public CacheHintAcc(Identifier eventId, OverlayId torrentId, Identifier fileId, KHint.Summary cacheHint) {
      super(eventId, torrentId, fileId);
      this.cacheHint = cacheHint;
    }
  }

  public static class BlockReq extends SilkEvent.E4 implements R1TransferMsg.Upld {

    public final int block;
    public final int nrPieces;

    BlockReq(Identifier eventId, OverlayId torrentId, Identifier fileId, int block, int nrPieces) {
      super(eventId, torrentId, fileId);
      this.block = block;
      this.nrPieces = nrPieces;
    }

    public BlockReq(OverlayId torrentId, Identifier fileId, int block, int nrPieces) {
      this(BasicIdentifiers.msgId(), torrentId, fileId, block, nrPieces);
    }

    public void pieces(Consumer<PieceReq> consumer, IntIdFactory intIdFactory) {
      for (int piece = 0; piece < nrPieces; piece++) {
        consumer.accept(new PieceReq(eventId, torrentId, fileId, Pair.with(block, piece), intIdFactory));
      }
    }

    public PieceResp pieceResp(int pieceNr, byte[] pieceVal, IntIdFactory intIdFactory) {
      Identifier pieceId = new PairIdentifier(intIdFactory.id(new BasicBuilders.IntBuilder(block)),
        intIdFactory.id(new BasicBuilders.IntBuilder(pieceNr)));
      return new PieceResp(eventId, torrentId, fileId, pieceId, Pair.with(block, pieceNr), Either.right(pieceVal));
    }
  }

  public static class PieceReq extends SilkEvent.E5 implements R1TransferMsg.Upld {

    public final Identifier pieceId;
    public final Pair<Integer, Integer> piece;

    public PieceReq(Identifier eventId, OverlayId torrentId, Identifier fileId, Pair<Integer, Integer> piece,
      IntIdFactory intIdFactory) {
      super(eventId, torrentId, fileId);
      this.pieceId = new PairIdentifier(intIdFactory.id(new BasicBuilders.IntBuilder(piece.getValue0())),
        intIdFactory.id(new BasicBuilders.IntBuilder(piece.getValue1())));
      this.piece = piece;
    }

    public PieceReq(OverlayId torrentId, Identifier fileId, Pair<Integer, Integer> piece,
      IntIdFactory intIdFactory) {
      this(BasicIdentifiers.msgId(), torrentId, fileId, piece, intIdFactory);
    }

    public PieceResp accept(RangeKReference val) {
      return new PieceResp(eventId, torrentId, fileId, pieceId, piece, Either.left(val));
    }

    @Override
    public Identifier getId() {
      return new PairIdentifier(eventId, pieceId);
    }
  }

  public static class PieceResp extends SilkEvent.E5 implements R1TransferMsg.Dwnl {

    public final Identifier pieceId;
    public final Pair<Integer, Integer> piece;
    public final Either<KReference<byte[]>, byte[]> val;

    public PieceResp(Identifier msgId, OverlayId torrentId, Identifier fileId, Identifier pieceId,
      Pair<Integer, Integer> piece, Either<KReference<byte[]>, byte[]> val) {
      super(msgId, torrentId, fileId);
      this.pieceId = pieceId;
      this.piece = piece;
      this.val = val;
    }

    @Override
    public Identifier getId() {
      return new PairIdentifier(eventId, pieceId);
    }
    
    public static PieceResp instance(Identifier msgId, OverlayId torrentId, Identifier fileId,
      Pair<Integer, Integer> piece, Either<KReference<byte[]>, byte[]> val, IntIdFactory intIdFactory) {
      Identifier pieceId = new PairIdentifier(intIdFactory.id(new BasicBuilders.IntBuilder(piece.getValue0())),
          intIdFactory.id(new BasicBuilders.IntBuilder(piece.getValue1())));
      return new PieceResp(msgId, torrentId, fileId, pieceId, piece, val);
    }
  }

  public static class HashReq extends SilkEvent.E4 implements R1TransferMsg.Upld {

    public final Set<Integer> hashes;

    HashReq(Identifier eventId, OverlayId torrentId, Identifier fileId, Set<Integer> hashes) {
      super(eventId, torrentId, fileId);
      this.hashes = hashes;
    }

    public HashReq(OverlayId torrentId, Identifier fileId, Set<Integer> hashes) {
      this(BasicIdentifiers.msgId(), torrentId, fileId, hashes);
    }

    public HashResp accept(Map<Integer, byte[]> values) {
      return new HashResp(eventId, torrentId, fileId, values);
    }
  }

  public static class HashResp extends SilkEvent.E4 implements R1TransferMsg.Dwnl {

    public final Map<Integer, byte[]> hashValues;

    public HashResp(Identifier eventId, OverlayId torrentId, Identifier fileId, Map<Integer, byte[]> hashValues) {
      super(eventId, torrentId, fileId);
      this.hashValues = hashValues;
    }
  }
}
