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

import java.util.List;
import se.sics.ktoolbox.util.identifiable.BasicIdentifiers;
import se.sics.ktoolbox.util.identifiable.overlay.OverlayId;
import se.sics.ktoolbox.util.network.KAddress;
import se.sics.silk.event.SilkEvent;
import se.sics.silk.r2torrent.torrent.R2Torrent;
import se.sics.silk.r2torrent.util.R2TorrentStatus;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class R2TorrentCtrlEvents {
  public static class MetaGetReq extends SilkEvent.E3 implements R2Torrent.CtrlEvent {
    public final List<KAddress> partners;
    public MetaGetReq(OverlayId torrentId, List<KAddress> partners) {
      super(BasicIdentifiers.eventId(), torrentId);
      this.partners = partners;
    }

    public MetaGetSucc success() {
      return new MetaGetSucc(this);
    }

    public MetaGetFail fail() {
      return new MetaGetFail(this);
    }
  }

  public static class MetaGetSucc extends SilkEvent.E3 implements R2Torrent.CtrlEvent {

    MetaGetSucc(MetaGetReq req) {
      super(req.eventId, req.torrentId);
    }
  }

  public static class MetaGetFail extends SilkEvent.E3 implements R2Torrent.CtrlEvent {

    MetaGetFail(MetaGetReq req) {
      super(req.eventId, req.torrentId);
    }
  }

  public static class Download extends SilkEvent.E3 implements R2Torrent.CtrlEvent {

    public Download(OverlayId torrentId) {
      super(BasicIdentifiers.eventId(), torrentId);
    }
  }
  
  public static class Upload extends SilkEvent.E3 implements R2Torrent.CtrlEvent {
    public Upload(OverlayId torrentId) {
      super(BasicIdentifiers.eventId(), torrentId);
    }
    
    public TorrentBaseInfo success(R2TorrentStatus status) {
      return new TorrentBaseInfo(this, status);
    }
  }

  public static class TorrentBaseInfoReq extends SilkEvent.E3 implements R2Torrent.CtrlEvent {

    public TorrentBaseInfoReq(OverlayId torrentId) {
      super(BasicIdentifiers.eventId(), torrentId);
    }

    public TorrentBaseInfo answer(R2TorrentStatus status) {
      return new TorrentBaseInfo(this, status);
    }
  }

  public static class TorrentBaseInfo extends SilkEvent.E3 implements R2Torrent.CtrlEvent {

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
  
  public static class Stop extends SilkEvent.E3 implements R2Torrent.CtrlEvent {
    public Stop(OverlayId torrentId) {
      super(BasicIdentifiers.eventId(), torrentId);
    }
    
    public StopAck ack() {
      return new StopAck(this);
    }
  }
  
  public static class StopAck extends SilkEvent.E3 implements R2Torrent.CtrlEvent {

    StopAck(Stop req) {
      super(req.eventId, req.torrentId);
    }
  }
}
