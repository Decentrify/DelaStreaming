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
import se.sics.silk.event.SilkEvent;
import se.sics.silk.r2torrent.conn.R1TorrentLeecher;
import se.sics.silk.r2torrent.conn.R2NodeLeecher;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class R2NodeLeecherEvents {

  public static abstract class Req extends SilkEvent.E1 implements R2NodeLeecher.Event {

    public Req(Identifier eventId, OverlayId torrentId, Identifier nodeId) {
      super(eventId, torrentId, nodeId);
    }
  }
  
  public static abstract class Ind extends SilkEvent.E1 implements R1TorrentLeecher.Event1 {

    public Ind(Identifier eventId, OverlayId torrentId, Identifier nodeId) {
      super(eventId, torrentId, nodeId);
    }
  }

  public static class ConnectReq extends Req {

    public ConnectReq(OverlayId torrentId, Identifier leecherId) {
      super(BasicIdentifiers.eventId(), torrentId, leecherId);
    }

    public ConnectSucc accept() {
      return new ConnectSucc(eventId, torrentId, nodeId);
    }

    public ConnectFail reject() {
      return new ConnectFail(eventId, torrentId, nodeId);
    }
  }

  public static abstract class ConnectInd extends Ind {

    ConnectInd(Identifier eventId, OverlayId torrentId, Identifier nodeId) {
      super(eventId, torrentId, nodeId);
    }
  }

  public static class ConnectSucc extends ConnectInd {

    ConnectSucc(Identifier eventId, OverlayId torrentId, Identifier nodeId) {
      super(eventId, torrentId, nodeId);
    }
  }

  public static class ConnectFail extends ConnectInd {

    ConnectFail(Identifier eventId, OverlayId torrentId, Identifier nodeId) {
      super(eventId, torrentId, nodeId);
    }
  }

  public static class Disconnect extends Req {

    public Disconnect(OverlayId torrentId, Identifier nodeId) {
      super(BasicIdentifiers.eventId(), torrentId, nodeId);
    }
  }
}
