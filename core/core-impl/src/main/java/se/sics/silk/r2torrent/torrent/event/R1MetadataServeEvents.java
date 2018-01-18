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

  public static class ServeReq extends SilkEvent.E4 implements R1MetadataServe.TorrentEvent {


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

  public static class ServeSucc extends SilkEvent.E3 implements R2Torrent.MetadataEvent {

    ServeSucc(ServeReq req) {
      super(req.eventId, req.torrentId);
    }
  }

  public static class ServeFail extends SilkEvent.E3 implements R2Torrent.MetadataEvent {

    ServeFail(ServeReq req) {
      super(req.eventId, req.torrentId);
    }
  }

  public static class Stop extends SilkEvent.E4 implements R1MetadataServe.TorrentEvent {

    public Stop(OverlayId torrentId, Identifier fileId) {
      super(BasicIdentifiers.eventId(), torrentId, fileId);
    }

    public StopAck ack() {
      return new StopAck(this);
    }
  }

  public static class StopAck extends SilkEvent.E3 implements R2Torrent.MetadataEvent {

    StopAck(Stop req) {
      super(req.eventId, req.torrentId);
    }
  }
}
