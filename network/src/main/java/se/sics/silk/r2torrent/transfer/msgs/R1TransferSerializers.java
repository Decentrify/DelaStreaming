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

import com.google.common.base.Optional;
import io.netty.buffer.ByteBuf;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import org.javatuples.Pair;
import org.javatuples.Triplet;
import se.sics.kompics.network.netty.serialization.Serializer;
import se.sics.kompics.network.netty.serialization.Serializers;
import se.sics.kompics.util.Identifier;
import se.sics.ktoolbox.util.Either;
import se.sics.ktoolbox.util.identifiable.basic.IntIdFactory;
import se.sics.ktoolbox.util.identifiable.overlay.OverlayId;
import se.sics.ktoolbox.util.reference.KReferenceException;
import se.sics.nstream.storage.cache.KHint;
import se.sics.silk.event.SilkEvent;
import se.sics.silk.r2torrent.torrent.R1Torrent;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class R1TransferSerializers {
  private static final IntIdFactory intIdFactory = new IntIdFactory(new Random(R1Torrent.HardCodedConfig.seed));
  public static abstract class Base implements Serializer {

    private final int id;

    Base(int id) {
      this.id = id;
    }

    @Override
    public int identifier() {
      return id;
    }

    public void toBin(Object o, ByteBuf buf) {
      SilkEvent.E5 obj = (SilkEvent.E5) o;
      Serializers.toBinary(obj.eventId, buf);
      Serializers.toBinary(obj.torrentId, buf);
      Serializers.toBinary(obj.fileId, buf);
    }

    public Triplet<Identifier, OverlayId, Identifier> fromBin(ByteBuf buf, Optional<Object> hint) {
      Identifier eventId = (Identifier)Serializers.fromBinary(buf, hint);
      OverlayId torrentId = (OverlayId)Serializers.fromBinary(buf, hint);
      Identifier fileId = (Identifier)Serializers.fromBinary(buf, hint);
      return Triplet.with(eventId, torrentId, fileId);
    }
  }
  
  public static class CacheHintReq extends Base {

    public CacheHintReq(int id) {
      super(id);
    }

    @Override
    public void toBinary(Object o, ByteBuf buf) {
      R1TransferMsgs.CacheHintReq obj = (R1TransferMsgs.CacheHintReq)o;
      toBin(obj, buf);
      Serializers.lookupSerializer(KHint.Summary.class).toBinary(obj.cacheHint, buf);
    }

    @Override
    public Object fromBinary(ByteBuf buf, Optional<Object> hint) {
      Triplet<Identifier, OverlayId, Identifier> base = fromBin(buf, hint);
      KHint.Summary cacheHint = (KHint.Summary)Serializers.lookupSerializer(KHint.Summary.class).fromBinary(buf, hint);
      return new R1TransferMsgs.CacheHintReq(base.getValue0(), base.getValue1(), base.getValue2(), cacheHint);
    }
  }
  
  public static class CacheHintAcc extends Base {

    public CacheHintAcc(int id) {
      super(id);
    }

    @Override
    public void toBinary(Object o, ByteBuf buf) {
      R1TransferMsgs.CacheHintAcc obj = (R1TransferMsgs.CacheHintAcc)o;
      toBin(obj, buf);
      Serializers.lookupSerializer(KHint.Summary.class).toBinary(obj.cacheHint, buf);
    }

    @Override
    public Object fromBinary(ByteBuf buf, Optional<Object> hint) {
      Triplet<Identifier, OverlayId, Identifier> base = fromBin(buf, hint);
      KHint.Summary cacheHint = (KHint.Summary)Serializers.lookupSerializer(KHint.Summary.class).fromBinary(buf, hint);
      return new R1TransferMsgs.CacheHintAcc(base.getValue0(), base.getValue1(), base.getValue2(), cacheHint);
    }
  }
  
  public static class BlockReq extends Base {

    public BlockReq(int id) {
      super(id);
    }

    @Override
    public void toBinary(Object o, ByteBuf buf) {
      R1TransferMsgs.BlockReq obj = (R1TransferMsgs.BlockReq)o;
      toBin(obj, buf);
      buf.writeInt(obj.block);
      buf.writeInt(obj.nrPieces);
    }

    @Override
    public Object fromBinary(ByteBuf buf, Optional<Object> hint) {
      Triplet<Identifier, OverlayId, Identifier> base = fromBin(buf, hint);
      int block = buf.readInt();
      int nrPieces = buf.readInt();
      return new R1TransferMsgs.BlockReq(base.getValue0(), base.getValue1(), base.getValue2(), block, nrPieces);
    }
  }
  
  public static class PieceReq extends Base {
    
    public PieceReq(int id) {
      super(id);
    }

    @Override
    public void toBinary(Object o, ByteBuf buf) {
      R1TransferMsgs.PieceReq obj = (R1TransferMsgs.PieceReq)o;
      toBin(obj, buf);
      buf.writeInt(obj.piece.getValue0());
      buf.writeInt(obj.piece.getValue1());
    }

    @Override
    public Object fromBinary(ByteBuf buf, Optional<Object> hint) {
      Triplet<Identifier, OverlayId, Identifier> base = fromBin(buf, hint);
      int blockNr = buf.readInt();
      int pieceNr = buf.readInt();
      return new R1TransferMsgs.PieceReq(base.getValue0(), base.getValue1(), base.getValue2(), 
        Pair.with(blockNr, pieceNr), intIdFactory);
    }
  }
  
  public static class PieceResp extends Base {

    public PieceResp(int id) {
      super(id);
    }

    @Override
    public void toBinary(Object o, ByteBuf buf) {
      R1TransferMsgs.PieceResp obj = (R1TransferMsgs.PieceResp)o;
      toBin(obj, buf);
      buf.writeInt(obj.piece.getValue0());
      buf.writeInt(obj.piece.getValue1());
      byte[] pieceVal = obj.val.getLeft().getValue().get();
      try {
        obj.val.getLeft().release();
      } catch (KReferenceException ex) {
        throw new RuntimeException(ex);
      }
      buf.writeInt(pieceVal.length);
      buf.writeBytes(pieceVal);
    }

    @Override
    public Object fromBinary(ByteBuf buf, Optional<Object> hint) {
      Triplet<Identifier, OverlayId, Identifier> base = fromBin(buf, hint);
      int blockNr = buf.readInt();
      int pieceNr = buf.readInt();
      int pieceSize = buf.readInt();
      byte[] pieceVal = new byte[pieceSize];
      buf.readBytes(pieceVal);
      return R1TransferMsgs.PieceResp.instance(base.getValue0(), base.getValue1(), base.getValue2(), 
        Pair.with(blockNr, pieceNr), Either.right(pieceVal), intIdFactory);
    }
  }
  
  public static class HashReq extends Base {

    public HashReq(int id) {
      super(id);
    }

    @Override
    public void toBinary(Object o, ByteBuf buf) {
      R1TransferMsgs.HashReq obj = (R1TransferMsgs.HashReq)o;
      toBin(obj, buf);
      buf.writeInt(obj.hashes.size());
      obj.hashes.stream().forEach((h) -> buf.writeInt(h));
    }

    @Override
    public Object fromBinary(ByteBuf buf, Optional<Object> hint) {
      Triplet<Identifier, OverlayId, Identifier> base = fromBin(buf, hint);
      Set<Integer> hashes = new TreeSet();
      int hashSize = buf.readInt();
      while (hashSize > 0) {
        hashSize--;
        hashes.add(buf.readInt());
      }
      return new R1TransferMsgs.HashReq(base.getValue0(), base.getValue1(), base.getValue2(), hashes);
    }
  }
  
  public static class HashResp extends Base {

    public HashResp(int id) {
      super(id);
    }

    @Override
    public void toBinary(Object o, ByteBuf buf) {
      R1TransferMsgs.HashResp obj = (R1TransferMsgs.HashResp)o;
      toBin(obj, buf);
      buf.writeInt(obj.hashValues.size());
      for (Map.Entry<Integer, byte[]> hv : obj.hashValues.entrySet()) {
        buf.writeInt(hv.getKey());
        buf.writeInt(hv.getValue().length);
        buf.writeBytes(hv.getValue());
      }
    }

    @Override
    public Object fromBinary(ByteBuf buf, Optional<Object> hint) {
      Triplet<Identifier, OverlayId, Identifier> base = fromBin(buf, hint);
      int hashSize = buf.readInt();
      Map<Integer, byte[]> hashValues = new TreeMap<>();
      while (hashSize > 0) {
        hashSize--;
        int hashNr = buf.readInt();
        int hashValueSize = buf.readInt();
        byte[] hashValue = new byte[hashValueSize];
        buf.readBytes(hashValue);
        hashValues.put(hashNr, hashValue);
      }
      return new R1TransferMsgs.HashResp(base.getValue0(), base.getValue1(), base.getValue2(), hashValues);
    }
    
  }
}
