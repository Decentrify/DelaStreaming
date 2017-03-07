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
package se.sics.cobweb.util;

import se.sics.cobweb.BaseEvent;
import se.sics.kompics.timer.SchedulePeriodicTimeout;
import se.sics.kompics.timer.ScheduleTimeout;
import se.sics.ktoolbox.util.identifiable.BasicIdentifiers;
import se.sics.ktoolbox.util.identifiable.Identifier;
import se.sics.ktoolbox.util.identifiable.overlay.OverlayId;
import se.sics.ktoolbox.util.overlays.OverlayEvent;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class TorrentEvent {

  public static abstract class Base extends BaseEvent.Base implements OverlayEvent {

    public final OverlayId torrentId;

    protected Base(Identifier eventId, OverlayId torrentId) {
      super(eventId);
      this.torrentId = torrentId;
    }

    @Override
    public OverlayId overlayId() {
      return torrentId;
    }
  }

  public static abstract class Request<R extends Response> extends BaseEvent.Request<R> implements OverlayEvent {
    public final OverlayId torrentId;

    public Request(Identifier eventId, OverlayId torrentId) {
      super(eventId);
      this.torrentId = torrentId;
    }
    
    public Request(OverlayId torrentId) {
      this(BasicIdentifiers.eventId(), torrentId);
    }

    @Override
    public OverlayId overlayId() {
      return torrentId;
    }
  }
  
  public static abstract class Response extends BaseEvent.Response implements OverlayEvent {

    public final OverlayId torrentId;

    protected Response(Request req) {
      super(req);
      this.torrentId = req.torrentId;
    }

    @Override
    public OverlayId overlayId() {
      return torrentId;
    }
  }
  
  public static abstract class Timeout extends BaseEvent.Timeout implements OverlayEvent {
    public final OverlayId torrentId;
    
    protected Timeout(ScheduleTimeout request, Identifier eventId, OverlayId torrentId) {
      super(request, eventId);
      this.torrentId = torrentId;
    }

    protected Timeout(SchedulePeriodicTimeout request, Identifier eventId, OverlayId torrentId) {
      super(request, eventId);
      this.torrentId = torrentId;
    }
    
    @Override
    public OverlayId overlayId() {
      return torrentId;
    }
  }
}
