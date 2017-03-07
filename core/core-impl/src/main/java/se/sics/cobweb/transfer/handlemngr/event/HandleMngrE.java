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
package se.sics.cobweb.transfer.handlemngr.event;

import se.sics.cobweb.overlord.handle.LeecherHandleOverlordFSMEvent;
import se.sics.cobweb.overlord.handle.SeederHandleOverlordFSMEvent;
import se.sics.cobweb.util.TorrentEvent;
import se.sics.cobweb.util.HandleId;
import se.sics.ktoolbox.util.identifiable.BasicIdentifiers;
import se.sics.ktoolbox.util.identifiable.Identifier;
import se.sics.ktoolbox.util.identifiable.overlay.OverlayId;
import se.sics.ktoolbox.util.network.KAddress;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class HandleMngrE {

  public static class LeecherConnect extends TorrentEvent.Request<LeecherConnected> implements LeecherHandleOverlordFSMEvent {

    public final HandleId handleId;
    public final KAddress seederAdr;

    public LeecherConnect(OverlayId torrentId, HandleId handleId, KAddress seederAdr) {
      super(BasicIdentifiers.eventId(), torrentId);
      this.handleId = handleId;
      this.seederAdr = seederAdr;
    }

    public LeecherConnected success() {
      return new LeecherConnected(this);
    }

    @Override
    public Identifier getLHOId() {
      return handleId;
    }
  }

  public static class LeecherConnected extends TorrentEvent.Response implements LeecherHandleOverlordFSMEvent {

    public final HandleId handleId;

    private LeecherConnected(LeecherConnect req) {
      super(req);
      this.handleId = req.handleId;
    }

    @Override
    public Identifier getLHOId() {
      return handleId;
    }
  }

  public static class LeecherDisconnect extends TorrentEvent.Request<LeecherDisconnected> {

    public final HandleId handleId;

    public LeecherDisconnect(OverlayId torrentId, HandleId handleId) {
      super(BasicIdentifiers.eventId(), torrentId);
      this.handleId = handleId;
    }

    public LeecherDisconnected success() {
      return new LeecherDisconnected(this);
    }
  }

  public static class LeecherDisconnected extends TorrentEvent.Response {

    public final HandleId handleId;

    public LeecherDisconnected(LeecherDisconnect req) {
      super(req);
      this.handleId = req.handleId;
    }
  }

  public static class SeederConnect extends TorrentEvent.Request<SeederConnected> implements SeederHandleOverlordFSMEvent {

    public final HandleId handleId;
    public final KAddress leecherAdr;

    public SeederConnect(OverlayId torrentId, HandleId handleId, KAddress leecherAdr) {
      super(BasicIdentifiers.eventId(), torrentId);
      this.handleId = handleId;
      this.leecherAdr = leecherAdr;
    }

    public SeederConnected success() {
      return new SeederConnected(this);
    }
    
    public SeederDisconnect disconnect() {
      return new SeederDisconnect(torrentId, handleId);
    }

    @Override
    public Identifier getSHOId() {
      return handleId;
    }
  }

  public static class SeederConnected extends TorrentEvent.Response implements SeederHandleOverlordFSMEvent {

    public final HandleId handleId;

    public SeederConnected(SeederConnect req) {
      super(req);
      this.handleId = req.handleId;
    }

    @Override
    public Identifier getSHOId() {
      return handleId;
    }
  }

  public static class SeederDisconnect extends TorrentEvent.Request<SeederDisconnected> {

    public final HandleId handleId;

    public SeederDisconnect(OverlayId torrentId, HandleId handleId) {
      super(BasicIdentifiers.eventId(), torrentId);
      this.handleId = handleId;
    }

    public SeederDisconnected success() {
      return new SeederDisconnected(this);
    }
  }

  public static class SeederDisconnected extends TorrentEvent.Response {

    public final HandleId handleId;

    public SeederDisconnected(SeederDisconnect req) {
      super(req);
      this.handleId = req.handleId;
    }
  }
}
