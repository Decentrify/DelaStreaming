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
package se.sics.silk.r2torrent.torrent.event;

import se.sics.kompics.util.Identifier;
import se.sics.ktoolbox.util.identifiable.BasicIdentifiers;
import se.sics.ktoolbox.util.identifiable.overlay.OverlayId;
import se.sics.silk.event.SilkEvent;
import se.sics.silk.r2torrent.torrent.R1Hash;
import se.sics.silk.r2torrent.torrent.R2Torrent;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class R1HashEvents {

  public static abstract class Req extends SilkEvent.E3 implements R1Hash.TorrentEvent {

    public Req(Identifier eventId, OverlayId torrentId) {
      super(eventId, torrentId);
    }
  }

  public static abstract class Ind extends SilkEvent.E3 implements R2Torrent.HashEvent {

    public Ind(Identifier eventId, OverlayId torrentId) {
      super(eventId, torrentId);
    }
  }

  public static class HashReq extends Req {

    public HashReq(OverlayId torrentId) {
      super(BasicIdentifiers.eventId(), torrentId);
    }

    public HashSucc success() {
      return new HashSucc(this);
    }

    public HashFail fail() {
      return new HashFail(this);
    }
  }

  public static class HashSucc extends Ind {

    HashSucc(HashReq req) {
      super(req.eventId, req.torrentId);
    }
  }

  public static class HashFail extends Ind {

    HashFail(HashReq req) {
      super(req.eventId, req.torrentId);
    }
  }

  public static class HashStop extends Req {

    public HashStop(OverlayId torrentId) {
      super(BasicIdentifiers.eventId(), torrentId);
    }

    public HashStopAck ack() {
      return new HashStopAck(this);
    }
  }

  public static class HashStopAck extends Ind {

    HashStopAck(HashStop req) {
      super(req.eventId, req.torrentId);
    }
  }
}
