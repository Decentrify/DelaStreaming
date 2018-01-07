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

import se.sics.kompics.util.Identifiable;
import se.sics.kompics.util.Identifier;
import se.sics.ktoolbox.util.identifiable.BasicIdentifiers;
import se.sics.ktoolbox.util.identifiable.overlay.OverlayId;
import se.sics.silk.r2torrent.R2Torrent;
import se.sics.silk.r2torrent.util.R2TorrentStatus;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class R2TorrentCtrlEvents {

  public static abstract class Base implements R2Torrent.CtrlEvent, Identifiable {

    public final Identifier eventId;
    public final OverlayId torrentId;

    Base(Identifier eventId, OverlayId torrentId) {
      this.eventId = eventId;
      this.torrentId = torrentId;
    }

    @Override
    public Identifier getId() {
      return eventId;
    }

    @Override
    public Identifier getR2TorrentFSMId() {
      return torrentId.baseId;
    }
  }

  public static class MetaGetReq extends Base {

    public MetaGetReq(OverlayId torrentId) {
      super(BasicIdentifiers.eventId(), torrentId);
    }

    public MetaGetSucc success() {
      return new MetaGetSucc(this);
    }

    public MetaGetFail fail() {
      return new MetaGetFail(this);
    }
  }

  public static class MetaGetSucc extends Base {

    MetaGetSucc(MetaGetReq req) {
      super(req.eventId, req.torrentId);
    }
  }

  public static class MetaGetFail extends Base {

    MetaGetFail(MetaGetReq req) {
      super(req.eventId, req.torrentId);
    }
  }

  public static class Download extends Base {

    public Download(OverlayId torrentId) {
      super(BasicIdentifiers.eventId(), torrentId);
    }
  }
  
  public static class Upload extends Base {
    public Upload(OverlayId torrentId) {
      super(BasicIdentifiers.eventId(), torrentId);
    }
    
    public TorrentBaseInfo success(R2TorrentStatus status) {
      return new TorrentBaseInfo(this, status);
    }
  }

  public static class TorrentBaseInfoReq extends Base {

    public TorrentBaseInfoReq(OverlayId torrentId) {
      super(BasicIdentifiers.eventId(), torrentId);
    }

    public TorrentBaseInfo answer(R2TorrentStatus status) {
      return new TorrentBaseInfo(this, status);
    }
  }

  public static class TorrentBaseInfo extends Base {

    public final R2TorrentStatus status;

    public TorrentBaseInfo(OverlayId torrentId, R2TorrentStatus status) {
      super(BasicIdentifiers.eventId(), torrentId);
      this.status = status;
    }

    TorrentBaseInfo(TorrentBaseInfoReq req, R2TorrentStatus status) {
      super(req.eventId, req.torrentId);
      this.status = status;
    }
    
    TorrentBaseInfo(Upload req, R2TorrentStatus status) {
      super(req.eventId, req.torrentId);
      this.status = status;
    }
  }
  
  public static class Stop extends Base {
    public Stop(OverlayId torrentId) {
      super(BasicIdentifiers.eventId(), torrentId);
    }
    
    public StopAck ack() {
      return new StopAck(this);
    }
  }
  
  public static class StopAck extends Base {

    StopAck(Stop req) {
      super(req.eventId, req.torrentId);
    }
    
  }
}
