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
import se.sics.silk.r2torrent.torrent.R1MetadataServe;
import se.sics.silk.r2torrent.torrent.R2Torrent;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class R1MetadataServeEvents {

  public static abstract class Req extends SilkEvent.E4 implements R1MetadataServe.TorrentEvent {

    public Req(Identifier eventId, OverlayId torrentId, Identifier fileId) {
      super(eventId, torrentId, fileId);
    }
  }

  public static abstract class Ind extends SilkEvent.E3 implements R2Torrent.MetadataEvent {

    public Ind(Identifier eventId, OverlayId torrentId) {
      super(eventId, torrentId);
    }
  }

  public static class ServeReq extends Req {


    public ServeReq(OverlayId torrentId, Identifier fileId) {
      super(BasicIdentifiers.eventId(), torrentId, fileId);
    }

    public ServeSucc success() {
      return new ServeSucc(this);
    }

    public ServeFail fail() {
      return new ServeFail(this);
    }
  }

  public static class ServeSucc extends Ind {

    ServeSucc(ServeReq req) {
      super(req.eventId, req.torrentId);
    }
  }

  public static class ServeFail extends Ind {

    ServeFail(ServeReq req) {
      super(req.eventId, req.torrentId);
    }
  }

  public static class Stop extends Req {

    public Stop(OverlayId torrentId, Identifier fileId) {
      super(BasicIdentifiers.eventId(), torrentId, fileId);
    }

    public StopAck ack() {
      return new StopAck(this);
    }
  }

  public static class StopAck extends Ind {

    StopAck(Stop req) {
      super(req.eventId, req.torrentId);
    }
  }
}
