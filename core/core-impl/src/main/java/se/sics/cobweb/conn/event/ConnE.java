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
package se.sics.cobweb.conn.event;

import se.sics.cobweb.conn.CLeecherHandleFSMEvent;
import se.sics.cobweb.conn.CSeederHandleFSMEvent;
import se.sics.cobweb.overlord.handle.LeecherHandleOverlordFSMEvent;
import se.sics.cobweb.overlord.handle.SeederHandleOverlordFSMEvent;
import se.sics.cobweb.util.HandleEvent;
import se.sics.cobweb.util.HandleId;
import se.sics.cobweb.util.TorrentEvent;
import se.sics.ktoolbox.util.identifiable.BasicIdentifiers;
import se.sics.ktoolbox.util.identifiable.Identifier;
import se.sics.ktoolbox.util.identifiable.overlay.OverlayId;
import se.sics.ktoolbox.util.network.KAddress;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class ConnE {
  public static class SeederSample extends TorrentEvent.Base {
    public final KAddress seederAdr;

    public SeederSample(OverlayId torrentId, KAddress seederAdr) {
      super(BasicIdentifiers.eventId(), torrentId);
      this.seederAdr = seederAdr;
    }
  }
  
  public static class Connect1Request extends HandleEvent.Request<Connect1Indication> implements CLeecherHandleFSMEvent {
    public final KAddress seederAdr;
    
    public Connect1Request(OverlayId torrentId, HandleId handleId, KAddress seederAdr) {
      super(BasicIdentifiers.eventId(), torrentId, handleId);
      this.seederAdr = seederAdr;
    }

    public Connect1Accept accept() {
      return new Connect1Accept(this);
    }

    @Override
    public Identifier clhFSMId() {
      return handleId;
    }
  }
  
  public static abstract class Connect1Indication extends HandleEvent.Response implements LeecherHandleOverlordFSMEvent {
    public final KAddress seederAdr;
    
    public Connect1Indication(Connect1Request req) {
      super(req);
      this.seederAdr = req.seederAdr;
    }
    
    @Override
    public Identifier getLHOId() {
      return handleId;
    }
  }
  
  public static class Connect1Accept extends Connect1Indication {
    public Connect1Accept(Connect1Request req) {
      super(req);
    }
  }
  
  public static class Connect2Request extends HandleEvent.Request<Connect2Indication> implements SeederHandleOverlordFSMEvent {
    public final KAddress leecherAdr;

    public Connect2Request(OverlayId torrentId, HandleId handleId, KAddress leecherAdr) {
      super(BasicIdentifiers.eventId(), torrentId, handleId);
      this.leecherAdr = leecherAdr;
    }
    
    public Connect2Accept accept() {
      return new Connect2Accept(this);
    }

    @Override
    public Identifier getSHOId() {
      return handleId;
    }
  }
  
  public static abstract class Connect2Indication extends HandleEvent.Response implements CSeederHandleFSMEvent {
    public final KAddress leecherAdr;
    public Connect2Indication(Connect2Request req) {
      super(req);
      this.leecherAdr = req.leecherAdr;
    }

    @Override
    public Identifier cshFSMId() {
      return handleId;
    }
  }
  
  public static class Connect2Accept extends Connect2Indication {
    public Connect2Accept(Connect2Request req) {
      super(req);
    }
  }
}
