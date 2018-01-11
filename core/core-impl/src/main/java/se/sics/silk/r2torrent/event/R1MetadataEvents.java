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

import se.sics.kompics.KompicsEvent;
import se.sics.kompics.util.Identifiable;
import se.sics.kompics.util.Identifier;
import se.sics.ktoolbox.util.identifiable.BasicIdentifiers;
import se.sics.ktoolbox.util.identifiable.overlay.OverlayId;
import se.sics.ktoolbox.util.network.KAddress;
import se.sics.silk.r2torrent.R1MetadataGet;
import se.sics.silk.r2torrent.R2Torrent;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class R1MetadataEvents {
   public static abstract class E1 implements KompicsEvent, Identifiable {

    public final Identifier eventId;
    public final OverlayId torrentId;

    E1(Identifier eventId, OverlayId torrentId) {
      this.eventId = eventId;
      this.torrentId = torrentId;
    }

    @Override
    public Identifier getId() {
      return eventId;
    }
  }

  public static class E2 extends E1 implements R1MetadataGet.TorrentEvent {

    public E2(Identifier eventId, OverlayId torrentId) {
      super(eventId, torrentId);
    }

    @Override
    public Identifier getR1MetadataFSMId() {
      return torrentId.baseId;
    }
  }

  public static class E3 extends E1 implements R2Torrent.MetadataEvent {

    public E3(Identifier eventId, OverlayId torrentId) {
      super(eventId, torrentId);
    }

    @Override
    public Identifier getR2TorrentFSMId() {
      return torrentId.baseId;
    }
  }
  
  public static class MetaGetReq extends E2 {
    public final KAddress seeder;
    public MetaGetReq(OverlayId torrentId, KAddress seeder) {
      super(BasicIdentifiers.eventId(), torrentId);
      this.seeder = seeder;
    }

    public MetaGetSucc success() {
      return new MetaGetSucc(this);
    }

    public MetaGetFail fail() {
      return new MetaGetFail(this);
    }
  }

  public static class MetaGetSucc extends E3 {

    MetaGetSucc(MetaGetReq req) {
      super(req.eventId, req.torrentId);
    }
  }

  public static class MetaGetFail extends E3 {

    MetaGetFail(MetaGetReq req) {
      super(req.eventId, req.torrentId);
    }
  }

  public static class MetaServeReq extends E2 {

    public MetaServeReq(OverlayId torrentId) {
      super(BasicIdentifiers.eventId(), torrentId);
    }

    public MetaServeSucc success() {
      return new MetaServeSucc(this);
    }
  }

  public static class MetaServeSucc extends E3 {

    public MetaServeSucc(MetaServeReq req) {
      super(req.eventId, req.torrentId);
    }
  }

  public static class MetaStop extends E2 {

    public MetaStop(OverlayId torrentId) {
      super(BasicIdentifiers.eventId(), torrentId);
    }

    public MetaStopAck ack() {
      return new MetaStopAck(this);
    }
  }

  public static class MetaStopAck extends E3 {

    MetaStopAck(MetaStop req) {
      super(req.eventId, req.torrentId);
    }
  }
}
