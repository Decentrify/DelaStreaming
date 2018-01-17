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
package se.sics.silk.r2torrent.conn.event;

import se.sics.kompics.util.Identifier;
import se.sics.ktoolbox.util.identifiable.BasicIdentifiers;
import se.sics.ktoolbox.util.identifiable.overlay.OverlayId;
import se.sics.ktoolbox.util.network.KAddress;
import se.sics.silk.event.SilkEvent;
import se.sics.silk.r2torrent.torrent.R1MetadataGet;
import se.sics.silk.r2torrent.conn.R1TorrentSeeder;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class R1TorrentSeederEvents {

  public static abstract class Req extends SilkEvent.E2 implements R1TorrentSeeder.Event2 {

    public Req(Identifier eventId, OverlayId torrentId, Identifier fileId, Identifier nodeId) {
      super(eventId, torrentId, fileId, nodeId);
    }
  }

  public static abstract class Ind extends SilkEvent.E2 implements R1MetadataGet.ConnEvent {

    public Ind(Identifier eventId, OverlayId torrentId, Identifier fileId, Identifier nodeId) {
      super(eventId, torrentId, fileId, nodeId);
    }
  }
  
  public static class ConnectReq extends Req {
    public final KAddress node;
    public ConnectReq(OverlayId torrentId, Identifier fileId, KAddress node) {
      super(BasicIdentifiers.eventId(), torrentId, fileId, node.getId());
      this.node = node;
    }
    
    public ConnectSucc success() {
      return new ConnectSucc(eventId, torrentId, fileId, nodeId);
    }
    
    public ConnectFail fail() {
      return new ConnectFail(eventId, torrentId, fileId, nodeId);
    }
  }
  
  public static class ConnectSucc extends Ind {
    ConnectSucc(Identifier eventId, OverlayId torrentId, Identifier fileId, Identifier nodeId) {
      super(eventId, torrentId, fileId, nodeId);
    }
  }
  
  public static class ConnectFail extends Ind {
    ConnectFail(Identifier eventId, OverlayId torrentId, Identifier fileId, Identifier nodeId) {
      super(eventId, torrentId, fileId, nodeId);
    }
  }
  
  public static class Disconnect extends Req {
    public Disconnect(OverlayId torrentId, Identifier fileId, Identifier nodeId) {
      super(BasicIdentifiers.eventId(), torrentId, fileId, nodeId);
    }
  }
}
