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

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class R2TorrentTransferEvents {

  public static abstract class Base implements R2Torrent.TransferEvent, Identifiable {

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

  public static class MetaServeReq extends Base {

    public MetaServeReq(OverlayId torrentId) {
      super(BasicIdentifiers.eventId(), torrentId);
    }
  }

  public static class MetaServeSucc extends Base {

    public MetaServeSucc(MetaServeReq req) {
      super(req.eventId, req.torrentId);
    }
  }

  public static class MetaStop extends Base {

    public MetaStop(OverlayId torrentId) {
      super(BasicIdentifiers.eventId(), torrentId);
    }

    public MetaStopAck ack() {
      return new MetaStopAck(this);
    }
  }

  public static class MetaStopAck extends Base {

    MetaStopAck(MetaStop req) {
      super(req.eventId, req.torrentId);
    }
  }

  public static class HashReq extends Base {

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

  public static class HashSucc extends Base {
    
    HashSucc(HashReq req) {
      super(req.eventId, req.torrentId);
    }
  }

  public static class HashFail extends Base {

    HashFail(HashReq req) {
      super(req.eventId, req.torrentId);
    }
  }

  public static class HashStop extends Base {

    public HashStop(OverlayId torrentId) {
      super(BasicIdentifiers.eventId(), torrentId);
    }
    
    public HashStopAck ack() {
      return new HashStopAck(this);
    }
  }
  
  public static class HashStopAck extends Base {
    HashStopAck(HashStop req) {
      super(req.eventId, req.torrentId);
    }
  }
}
