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
import se.sics.silk.r2torrent.R2NodeLeecher;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class R2NodeLeecherEvents {

  public static abstract class E1 implements KompicsEvent, Identifiable {

    public final Identifier eventId;
    public final OverlayId torrentId;
    public final Identifier leecherId;

    E1(Identifier eventId, OverlayId torrentId, Identifier leecherId) {
      this.eventId = eventId;
      this.torrentId = torrentId;
      this.leecherId = leecherId;
    }

    @Override
    public Identifier getId() {
      return eventId;
    }
  }

  public static abstract class E2 extends E1 implements R2NodeLeecher.Event {

    public E2(Identifier eventId, OverlayId torrentId, Identifier leecherId) {
      super(eventId, torrentId, leecherId);
    }

    @Override
    public Identifier getConnLeecherFSMId() {
      return leecherId;
    }
  }
  
  public static abstract class E3 extends E1 {
    public E3(Identifier eventId, OverlayId torrentId, Identifier leecherId) {
      super(eventId, torrentId, leecherId);
    }
  }

  public static class ConnectReq extends E2 {

    public ConnectReq(OverlayId torrentId, Identifier leecherId) {
      super(BasicIdentifiers.eventId(), torrentId, leecherId);
    }

    public ConnectSucc accept() {
      return new ConnectSucc(eventId, torrentId, leecherId);
    }

    public ConnectFail reject() {
      return new ConnectFail(eventId, torrentId, leecherId);
    }

    public Disconnect2 disconnect() {
      return new Disconnect2(torrentId, leecherId);
    }
  }

  public static abstract class ConnectInd extends E3 {

    ConnectInd(Identifier eventId, OverlayId torrentId, Identifier leecherId) {
      super(eventId, torrentId, leecherId);
    }
  }

  public static class ConnectSucc extends ConnectInd {

    ConnectSucc(Identifier eventId, OverlayId torrentId, Identifier leecherId) {
      super(eventId, torrentId, leecherId);
    }
  }

  public static class ConnectFail extends ConnectInd {

    ConnectFail(Identifier eventId, OverlayId torrentId, Identifier leecherId) {
      super(eventId, torrentId, leecherId);
    }
  }

  public static class Disconnect1 extends E2 {

    public Disconnect1(OverlayId torrentId, Identifier leecherId) {
      super(BasicIdentifiers.eventId(), torrentId, leecherId);
    }
  }
  
  public static class Disconnect2 extends E3 {

    public Disconnect2(OverlayId torrentId, Identifier leecherId) {
      super(BasicIdentifiers.eventId(), torrentId, leecherId);
    }
  }
}
