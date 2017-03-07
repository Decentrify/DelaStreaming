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
package se.sics.cobweb.transfer.handle.msg;

import java.util.Map;
import java.util.Set;
import org.javatuples.Pair;
import se.sics.cobweb.util.HandleEvent;
import se.sics.cobweb.util.HandleId;
import se.sics.ktoolbox.util.Either;
import se.sics.ktoolbox.util.identifiable.BasicIdentifiers;
import se.sics.ktoolbox.util.identifiable.Identifier;
import se.sics.ktoolbox.util.identifiable.overlay.OverlayId;
import se.sics.ktoolbox.util.reference.KReference;
import se.sics.nstream.storage.cache.KHint;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class HandleM {

  public static class CacheHintReq extends HandleEvent.Base {
    public final KHint.Summary requestCache;
    
    public CacheHintReq(OverlayId torrentId, HandleId handleId, KHint.Summary requestCache) {
      super(BasicIdentifiers.msgId(), torrentId, handleId);
      this.requestCache = requestCache;
    }
    
    public CacheHintSuccess success() {
      return new CacheHintSuccess(this);
    }

    @Override
    public HandleEvent.Base withHandleId(HandleId handleId) {
      throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }
  }
  
  public static class CacheHintSuccess extends HandleEvent.Base {
    
    private CacheHintSuccess(CacheHintReq req) {
      super(req.eventId, req.torrentId, req.handleId);
    }

    @Override
    public HandleEvent.Base withHandleId(HandleId handleId) {
      throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }
  }
  
  public static class CacheHintFault extends HandleEvent.Base {
    public CacheHintFault(CacheHintReq req) {
      super(req.eventId, req.torrentId, req.handleId);
    }

    @Override
    public HandleEvent.Base withHandleId(HandleId handleId) {
      throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }
  }
  
  public static class DwnlHashReq extends HandleEvent.Base {
    public final Set<Integer> hashes;
    
    public DwnlHashReq(OverlayId torrentId, HandleId handleId, Set<Integer> hashes) {
      super(BasicIdentifiers.msgId(), torrentId, handleId);
      this.hashes = hashes;
    }
    
    public DwnlHashSuccess success(Map<Integer, byte[]> hashValues) {
      return new DwnlHashSuccess(this, hashValues);
    }
    
    public DwnlHashFault fault() {
      return new DwnlHashFault(this);
    }

    @Override
    public HandleEvent.Base withHandleId(HandleId handleId) {
      throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }
  }
  
  public static class DwnlHashSuccess extends HandleEvent.Base {
    public final Map<Integer, byte[]> hashValues;
    
    private DwnlHashSuccess(DwnlHashReq req, Map<Integer, byte[]> hashValues) {
      super(req.eventId, req.torrentId, req.handleId);
      this.hashValues = hashValues;
    }

    @Override
    public HandleEvent.Base withHandleId(HandleId handleId) {
      throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }
  }
  
  public static class DwnlHashFault extends HandleEvent.Base {
    public final Set<Integer> hashes;
    
    private DwnlHashFault(DwnlHashReq req) {
      super(req.eventId, req.torrentId, req.handleId);
      this.hashes = req.hashes;
    }

    @Override
    public HandleEvent.Base withHandleId(HandleId handleId) {
      throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }
  }
  
  public static class DwnlPieceReq extends HandleEvent.Base {
    public final Pair<Integer, Integer> piece;
    
    public DwnlPieceReq(OverlayId torrentId, HandleId handleId, Pair<Integer, Integer> piece) {
      super(BasicIdentifiers.msgId(), torrentId, handleId);
      this.piece = piece;
    }
    
    public DwnlPieceSuccess success(KReference<byte[]> val) {
      return new DwnlPieceSuccess(this, val);
    }

    public DwnlPieceFault fault() {
      return new DwnlPieceFault(this);
    }

    @Override
    public HandleEvent.Base withHandleId(HandleId handleId) {
      throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }
  }
  
  public static class DwnlPieceSuccess extends HandleEvent.Base {
    public final Pair<Integer, Integer> piece;
    public final Either<KReference<byte[]>, byte[]> val;

    private DwnlPieceSuccess(Identifier msgId, OverlayId torrentId, HandleId handleId, Pair<Integer, Integer> piece, Either val) {
      super(msgId, torrentId, handleId);
      this.piece = piece;
      this.val = val;
    }

    private DwnlPieceSuccess(DwnlPieceReq req, KReference<byte[]> val) {
      this(req.eventId, req.torrentId, req.handleId, req.piece, Either.left(val));
    }

    @Override
    public HandleEvent.Base withHandleId(HandleId handleId) {
      throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }
  }
  
  public static class DwnlPieceFault extends HandleEvent.Base {
    public final Pair<Integer, Integer> piece;

    private DwnlPieceFault(DwnlPieceReq req) {
      super(req.eventId, req.torrentId, req.handleId);
      this.piece = req.piece;
    }

    @Override
    public HandleEvent.Base withHandleId(HandleId handleId) {
      throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }
  }
}
