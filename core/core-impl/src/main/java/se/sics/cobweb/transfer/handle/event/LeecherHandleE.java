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
package se.sics.cobweb.transfer.handle.event;

import java.util.Map;
import java.util.Set;
import se.sics.cobweb.transfer.instance.TransferFSMEvent;
import se.sics.cobweb.util.HandleEvent;
import se.sics.cobweb.util.HandleId;
import se.sics.ktoolbox.util.identifiable.BasicIdentifiers;
import se.sics.ktoolbox.util.identifiable.Identifier;
import se.sics.ktoolbox.util.identifiable.overlay.OverlayId;
import se.sics.nstream.util.BlockDetails;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class LeecherHandleE {
  
  public static class SetupReq extends HandleEvent.Request<SetupResp> {

    public SetupReq(OverlayId torrentId, HandleId handleId) {
      super(BasicIdentifiers.eventId(), torrentId, handleId);
    }
    
    public SetupResp answer(BlockDetails defaultBlockDetails, boolean withHashes) {
      return new SetupResp(this, defaultBlockDetails, withHashes);
    }
  }
  
  public static class SetupResp extends HandleEvent.Response {
    public final BlockDetails defaultBlockDetails;
    public final boolean withHashes;
    
    public SetupResp(HandleEvent.Request req, BlockDetails defaultBlockDetails, boolean withHashes) {
      super(req);
      this.defaultBlockDetails = defaultBlockDetails;
      this.withHashes = withHashes;
    }
  }

  public static class Download extends HandleEvent.Base implements TransferFSMEvent {

    public final Set<Integer> blocks;
    public final Map<Integer, BlockDetails> irregularBlocks;
    public final int requestedBlocks;

    public Download(OverlayId torrentId, HandleId handleId, Set<Integer> blocks,
      Map<Integer, BlockDetails> irregularBlocks, int requestedBlocks) {
      super(BasicIdentifiers.eventId(), torrentId, handleId);
      this.blocks = blocks;
      this.irregularBlocks = irregularBlocks;
      this.requestedBlocks = requestedBlocks;
    }

    @Override
    public Identifier getTransferFSMId() {
      return torrentId.baseId;
    }

    @Override
    public HandleEvent.Base withHandleId(HandleId handleId) {
      throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }
  }
  
  public static class Completed extends HandleEvent.Base implements TransferFSMEvent {

    public final Map<Integer, byte[]> hashes;
    public final Map<Integer, byte[]> blocks;
    public final int requestBlocks;

    public Completed(OverlayId torrentId, HandleId handleId, Map<Integer, byte[]> hashes, Map<Integer, byte[]> blocks,
      int requestBlocks) {
      super(BasicIdentifiers.eventId(), torrentId, handleId);
      this.hashes = hashes;
      this.blocks = blocks;
      this.requestBlocks = requestBlocks;
    }

    @Override
    public Identifier getTransferFSMId() {
      return torrentId.baseId;
    }

    @Override
    public HandleEvent.Base withHandleId(HandleId handleId) {
      throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }
  }
  
  public static class Shutdown extends HandleEvent.Request<ShutdownAck> {

    public final Set<Integer> incomplete;

    public Shutdown(OverlayId torrentId, HandleId handleId, Set<Integer> incomplete) {
      super(BasicIdentifiers.eventId(), torrentId, handleId);
      this.incomplete = incomplete;
    }
    
    public ShutdownAck ack() {
      return new ShutdownAck(this);
    }
  }

  public static class ShutdownAck extends HandleEvent.Response {

    public ShutdownAck(Shutdown req) {
      super(req);
    }
  }
}
