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

import se.sics.kompics.util.Identifier;
import se.sics.ktoolbox.util.identifiable.BasicIdentifiers;
import se.sics.ktoolbox.util.identifiable.overlay.OverlayId;
import se.sics.silk.event.SilkEvent;
import se.sics.silk.r2torrent.transfer.R1TransferLeecher;
import se.sics.silk.r2torrent.transfer.R1TransferSeeder;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class R1TransferConnMsgs {
  public static class Connect extends SilkEvent.E4 implements R1TransferLeecher.Msg {
    public Connect(OverlayId torrentId, Identifier fileId) {
      super(BasicIdentifiers.msgId(), torrentId, fileId);
    }
    
    public ConnectAcc accept() {
      return new ConnectAcc(torrentId, fileId);
    }
  }
  
  public static class ConnectAcc extends SilkEvent.E4 implements R1TransferSeeder.Msg {

    public ConnectAcc(OverlayId torrentId, Identifier fileId) {
      super(BasicIdentifiers.eventId(), torrentId, fileId);
    }
  }
  
  public static class Disconnect extends SilkEvent.E4 implements R1TransferLeecher.Msg, R1TransferSeeder.Msg{
    public Disconnect(OverlayId torrentId, Identifier fileId) {
      super(BasicIdentifiers.msgId(), torrentId, fileId);
    }
  }
  
  public static class Ping extends SilkEvent.E4 implements R1TransferLeecher.Msg {
    public Ping(OverlayId torrentId, Identifier fileId) {
      super(BasicIdentifiers.msgId(), torrentId, fileId);
    }
    
    public Pong pong() {
      return new Pong(eventId, torrentId, fileId);
    }
  }
  
  public static class Pong extends SilkEvent.E4 implements R1TransferSeeder.Msg {
    public Pong(Identifier eventId, OverlayId torrentId, Identifier fileId) {
      super(eventId, torrentId, fileId);
    }
  }
}
