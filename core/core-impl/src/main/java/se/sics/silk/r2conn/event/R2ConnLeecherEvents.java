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
package se.sics.silk.r2conn.event;

import se.sics.kompics.util.Identifier;
import se.sics.ktoolbox.util.identifiable.BasicIdentifiers;
import se.sics.ktoolbox.util.identifiable.overlay.OverlayId;
import se.sics.ktoolbox.util.overlays.OverlayEvent;
import se.sics.silk.r2conn.R2ConnLeecher;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class R2ConnLeecherEvents {
  public static abstract class Base implements R2ConnLeecher.Event, OverlayEvent {
    public final Identifier eventId;
    public final OverlayId torrentId;
    public final Identifier leecherId;

    Base(Identifier eventId, OverlayId torrentId, Identifier leecherId) {
      this.eventId = eventId;
      this.torrentId = torrentId;
      this.leecherId = leecherId;
    }
    
    @Override
    public Identifier getId() {
      return eventId;
    }

    @Override
    public OverlayId overlayId() {
      return torrentId;
    }
    
    @Override
    public Identifier getConnLeecherFSMId() {
      return leecherId;
    }
  }
  
  public static class ConnectReq extends Base {
    public ConnectReq(OverlayId torrentId, Identifier leecherId) {
      super(BasicIdentifiers.eventId(), torrentId, leecherId);
    }
    
    public ConnectSucc accept() {
      return new ConnectSucc(eventId, torrentId, leecherId);
    }
    
    public ConnectFail reject() {
      return new ConnectFail(eventId, torrentId, leecherId);
    }
    
    public Disconnect disconnect() {
      return new Disconnect(torrentId, leecherId);
    }
  }
  
  public static abstract class ConnectInd extends Base {
    ConnectInd(Identifier eventId, OverlayId torrentId, Identifier leecherId) {
      super(eventId, torrentId, leecherId);
    }
  }
  
  public static class ConnectSucc extends ConnectInd {
    ConnectSucc(Identifier eventId, OverlayId torrentId, Identifier leecherId) {
      super(eventId, torrentId, leecherId);
    }
  }
  
  public static class ConnectFail extends ConnectInd {
    ConnectFail(Identifier eventId, OverlayId torrentId, Identifier leecherId) {
      super(eventId, torrentId, leecherId);
    }
  }
  
  public static class Disconnect extends Base {
    public Disconnect(Identifier eventId, OverlayId torrentId, Identifier leecherId) {
      super(eventId, torrentId, leecherId);
    }
    
    public Disconnect(OverlayId torrentId, Identifier leecherId) {
      this(BasicIdentifiers.eventId(), torrentId, leecherId);
    }
  }
}
