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
package se.sics.silk.r2torrent.event;

import se.sics.kompics.util.Identifier;
import se.sics.ktoolbox.util.identifiable.BasicIdentifiers;
import se.sics.ktoolbox.util.identifiable.overlay.OverlayId;
import se.sics.ktoolbox.util.network.KAddress;
import se.sics.silk.event.SilkEvent;
import se.sics.silk.r2torrent.R1MetadataGet;
import se.sics.silk.r2torrent.R2Torrent;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class R1MetadataGetEvents {

  public static abstract class Req extends SilkEvent.E4 implements R1MetadataGet.TorrentEvent {

    public Req(Identifier eventId, OverlayId torrentId, Identifier fileId) {
      super(eventId, torrentId, fileId);
    }
  }

  public static abstract class Ind extends SilkEvent.E3 implements R2Torrent.MetadataEvent {

    public Ind(Identifier eventId, OverlayId torrentId) {
      super(eventId, torrentId);
    }

    @Override
    public Identifier getR2TorrentFSMId() {
      return torrentId.baseId;
    }
  }

  public static class MetaGetReq extends Req {

    public final KAddress seeder;

    public MetaGetReq(OverlayId torrentId, Identifier fileId, KAddress seeder) {
      super(BasicIdentifiers.eventId(), torrentId, fileId);
      this.seeder = seeder;
    }

    public MetaGetSucc success() {
      return new MetaGetSucc(this);
    }

    public MetaGetFail fail() {
      return new MetaGetFail(this);
    }
  }

  public static class MetaGetSucc extends Ind {

    MetaGetSucc(MetaGetReq req) {
      super(req.eventId, req.torrentId);
    }
  }

  public static class MetaGetFail extends Ind {

    MetaGetFail(MetaGetReq req) {
      super(req.eventId, req.torrentId);
    }
  }

  public static class MetaServeReq extends Req {

    public MetaServeReq(OverlayId torrentId, Identifier fileId) {
      super(BasicIdentifiers.eventId(), torrentId, fileId);
    }

    public MetaServeSucc success() {
      return new MetaServeSucc(this);
    }
  }

  public static class MetaServeSucc extends Ind {

    public MetaServeSucc(MetaServeReq req) {
      super(req.eventId, req.torrentId);
    }
  }

  public static class MetaStop extends Req {

    public MetaStop(OverlayId torrentId, Identifier fileId) {
      super(BasicIdentifiers.eventId(), torrentId, fileId);
    }

    public MetaStopAck ack() {
      return new MetaStopAck(this);
    }
  }

  public static class MetaStopAck extends Ind {

    MetaStopAck(MetaStop req) {
      super(req.eventId, req.torrentId);
    }
  }
}
