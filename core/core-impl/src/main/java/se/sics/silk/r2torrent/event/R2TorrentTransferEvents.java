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
import se.sics.silk.r2torrent.R2Torrent;
import se.sics.silk.r2transfer.R1Hash;
import se.sics.silk.r2transfer.R1Metadata;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class R2TorrentTransferEvents {

  public static abstract class Base1 implements KompicsEvent, Identifiable {

    public final Identifier eventId;
    public final OverlayId torrentId;

    Base1(Identifier eventId, OverlayId torrentId) {
      this.eventId = eventId;
      this.torrentId = torrentId;
    }

    @Override
    public Identifier getId() {
      return eventId;
    }
  }

  public static abstract class Base2 extends Base1 implements R2Torrent.TransferEvent, R1Metadata.TransferEvent {

    public Base2(Identifier eventId, OverlayId torrentId) {
      super(eventId, torrentId);
    }

    @Override
    public Identifier getR1MetadataFSMId() {
      return torrentId.baseId;
    }

    @Override
    public Identifier getR2TorrentFSMId() {
      return torrentId.baseId;
    }
  }

  public static class MetaGetReq extends Base2 {

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

  public static class MetaGetSucc extends Base2 {

    MetaGetSucc(MetaGetReq req) {
      super(req.eventId, req.torrentId);
    }
  }

  public static class MetaGetFail extends Base2 {

    MetaGetFail(MetaGetReq req) {
      super(req.eventId, req.torrentId);
    }
  }

  public static class MetaServeReq extends Base2 {

    public MetaServeReq(OverlayId torrentId) {
      super(BasicIdentifiers.eventId(), torrentId);
    }

    public MetaServeSucc success() {
      return new MetaServeSucc(this);
    }
  }

  public static class MetaServeSucc extends Base2 {

    public MetaServeSucc(MetaServeReq req) {
      super(req.eventId, req.torrentId);
    }
  }

  public static class MetaStop extends Base2 {

    public MetaStop(OverlayId torrentId) {
      super(BasicIdentifiers.eventId(), torrentId);
    }

    public MetaStopAck ack() {
      return new MetaStopAck(this);
    }
  }

  public static class MetaStopAck extends Base2 {

    MetaStopAck(MetaStop req) {
      super(req.eventId, req.torrentId);
    }
  }

  public static class Base3 extends Base1 implements R2Torrent.TransferEvent, R1Hash.TransferEvent {

    public Base3(Identifier eventId, OverlayId torrentId) {
      super(eventId, torrentId);
    }

    @Override
    public Identifier getR1HashFSMId() {
      return torrentId.baseId;
    }

    @Override
    public Identifier getR2TorrentFSMId() {
      return torrentId.baseId;
    }
  }

  public static class HashReq extends Base3 {

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

  public static class HashSucc extends Base3 {

    HashSucc(HashReq req) {
      super(req.eventId, req.torrentId);
    }
  }

  public static class HashFail extends Base3 {

    HashFail(HashReq req) {
      super(req.eventId, req.torrentId);
    }
  }

  public static class HashStop extends Base3 {

    public HashStop(OverlayId torrentId) {
      super(BasicIdentifiers.eventId(), torrentId);
    }

    public HashStopAck ack() {
      return new HashStopAck(this);
    }
  }

  public static class HashStopAck extends Base3 {

    HashStopAck(HashStop req) {
      super(req.eventId, req.torrentId);
    }
  }
}
