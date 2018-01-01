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
package se.sics.silk.r2mngr.event;

import se.sics.kompics.util.Identifier;
import se.sics.ktoolbox.util.identifiable.BasicIdentifiers;
import se.sics.ktoolbox.util.identifiable.overlay.OverlayId;
import se.sics.ktoolbox.util.network.KAddress;
import se.sics.ktoolbox.util.overlays.OverlayEvent;
import se.sics.silk.r2mngr.ConnSeeder;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class ConnSeederEvents {
  public static abstract class Base implements ConnSeeder.Event, OverlayEvent {
    public final Identifier eventId;
    public final OverlayId torrentId;
    public final Identifier seederId;

    Base(Identifier eventId, OverlayId torrentId, Identifier seederId) {
      this.eventId = eventId;
      this.torrentId = torrentId;
      this.seederId = seederId;
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
    public Identifier getConnSeederFSMId() {
      return seederId;
    }
  }
  
  public static class Connect extends Base {
    public final KAddress seederAdr;
    
    public Connect(OverlayId torrentId, KAddress seederAdr) {
      super(BasicIdentifiers.eventId(), torrentId, seederAdr.getId());
      this.seederAdr = seederAdr;
    }

    public ConnectSuccess success() {
      return new ConnectSuccess(this);
    }

    public ConnectFail fail() {
      return new ConnectFail(this);
    }
  }

  public static class ConnectSuccess extends Base {
    public ConnectSuccess(Connect req) {
      super(req.eventId, req.torrentId, req.seederAdr.getId());
    }
  }
  
  public static class ConnectFail extends Base {
    public ConnectFail(Connect req) {
      super(req.eventId, req.torrentId, req.seederAdr.getId());
    }
  }
  
  public static class Disconnect extends Base {
    public Disconnect(OverlayId torrentId, Identifier seederId) {
      super(BasicIdentifiers.eventId(), torrentId, seederId);
    }
  }
}
