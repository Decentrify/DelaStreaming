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
import org.javatuples.Pair;
import se.sics.kompics.util.Identifier;
import se.sics.ktoolbox.util.Either;
import se.sics.ktoolbox.util.identifiable.BasicIdentifiers;
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
  public static class CacheHintReq extends SilkEvent.E4 implements R1TransferMsg {
    public final KHint.Summary cacheHint;
    public CacheHintReq(OverlayId torrentId, Identifier fileId, KHint.Summary cacheHint) {
      super(BasicIdentifiers.msgId(), torrentId, fileId);
      this.cacheHint = cacheHint;
    }
    
    public CacheHintAcc accept() {
      return new CacheHintAcc(eventId, torrentId, fileId, cacheHint);
    }
  }
  
  public static class CacheHintAcc extends SilkEvent.E4 implements R1TransferMsg {
    public final KHint.Summary cacheHint;
    public CacheHintAcc(Identifier eventId, OverlayId torrentId, Identifier fileId, KHint.Summary cacheHint) {
      super(eventId, torrentId, fileId);
      this.cacheHint = cacheHint;
    }
  }
  
  public static class PieceReq extends SilkEvent.E4 implements R1TransferMsg {
    public final Pair<Integer, Integer> piece;
    public PieceReq(OverlayId torrentId, Identifier fileId, Pair<Integer, Integer> piece) {
      super(BasicIdentifiers.eventId(), torrentId, fileId);
      this.piece = piece;
    }
    
    public PieceResp accept(RangeKReference val) {
      return new PieceResp(torrentId, fileId, piece, Either.left(val));
    }
  }
  
  public static class PieceResp extends SilkEvent.E4 implements R1TransferMsg {
    public final Pair<Integer, Integer> piece;
    public final Either<KReference<byte[]>, byte[]> val;
    public PieceResp(OverlayId torrentId, Identifier fileId,
      Pair<Integer, Integer> piece, Either<KReference<byte[]>, byte[]> val) {
      super(BasicIdentifiers.msgId(), torrentId, fileId);
      this.piece = piece;
      this.val = val;
    }
  }
  
  public static class HashReq extends SilkEvent.E4 implements R1TransferMsg {
    public final Set<Integer> hashes;
    
    public HashReq(OverlayId torrentId, Identifier fileId, Set<Integer> hashes) {
      super(BasicIdentifiers.msgId(), torrentId, fileId);
      this.hashes = hashes;
    }
    
    public HashResp accept(Map<Integer, byte[]> values) {
      return new HashResp(eventId, torrentId, fileId, values);
    }
  }
  
  public static class HashResp extends SilkEvent.E4 implements R1TransferMsg {
    public final Map<Integer, byte[]> hashValues;

    public HashResp(Identifier eventId, OverlayId torrentId, Identifier fileId, Map<Integer, byte[]> hashValues) {
      super(eventId, torrentId, fileId);
      this.hashValues = hashValues;
    }
  }
}
