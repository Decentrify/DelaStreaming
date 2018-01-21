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
package se.sics.silk.r2torrent.transfer.events;

import se.sics.kompics.util.Identifier;
import se.sics.ktoolbox.util.identifiable.BasicIdentifiers;
import se.sics.ktoolbox.util.identifiable.overlay.OverlayId;
import se.sics.ktoolbox.util.network.KAddress;
import se.sics.silk.event.SilkEvent;
import se.sics.silk.r2torrent.torrent.R1FileUpload;
import se.sics.silk.r2torrent.transfer.R1TransferLeecher;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class R1TransferLeecherEvents {
   public static class ConnectReq extends SilkEvent.E2 implements R1FileUpload.ConnectEvent {
    public final KAddress leecherAdr;
    public ConnectReq(OverlayId torrentId, Identifier fileId, KAddress leecherAdr) {
      super(BasicIdentifiers.eventId(), torrentId, fileId, leecherAdr.getId());
      this.leecherAdr = leecherAdr;
    }
    
    public ConnectAcc accept() {
      return new ConnectAcc(torrentId, fileId, nodeId);
    }
    
    public ConnectRej reject() {
      return new ConnectRej(torrentId, fileId, nodeId);
    }
  }
  
  public static class ConnectAcc extends SilkEvent.E2 implements R1TransferLeecher.CtrlEvent {

    public ConnectAcc(OverlayId torrentId, Identifier fileId, Identifier leecherId) {
      super(BasicIdentifiers.eventId(), torrentId, fileId, leecherId);
    }
  }
  
  public static class ConnectRej extends SilkEvent.E2 implements R1TransferLeecher.CtrlEvent {

    public ConnectRej(OverlayId torrentId, Identifier fileId, Identifier leecherId) {
      super(BasicIdentifiers.eventId(), torrentId, fileId, leecherId);
    }
  }
  
  public static class Disconnect extends SilkEvent.E2 implements R1TransferLeecher.CtrlEvent {
    public Disconnect(OverlayId torrentId, Identifier fileId, Identifier leecherId) {
      super(BasicIdentifiers.eventId(), torrentId, fileId, leecherId);
    }
  }
  
  public static class Disconnected extends SilkEvent.E2 implements R1FileUpload.ConnectEvent {
    public Disconnected(OverlayId torrentId, Identifier fileId, Identifier leecherId) {
      super(BasicIdentifiers.eventId(), torrentId, fileId, leecherId);
    }
  }
}
