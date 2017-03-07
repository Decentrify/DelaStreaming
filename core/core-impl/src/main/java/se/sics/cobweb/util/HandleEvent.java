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

import se.sics.kompics.KompicsEvent;
import se.sics.kompics.timer.SchedulePeriodicTimeout;
import se.sics.kompics.timer.ScheduleTimeout;
import se.sics.ktoolbox.util.identifiable.Identifier;
import se.sics.ktoolbox.util.identifiable.overlay.OverlayId;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class HandleEvent {

  public static interface Identifiable extends KompicsEvent {

    public HandleId handleId();
  }

  public static abstract class Base extends TorrentEvent.Base implements Identifiable {

    public final HandleId handleId;

    public Base(Identifier eventId, OverlayId torrentId, HandleId handleId) {
      super(eventId, torrentId);
      this.handleId = handleId;
    }

    @Override
    public HandleId handleId() {
      return handleId;
    }
    
    public abstract Base withHandleId(HandleId handleId);
  }

  public static abstract class Request<R extends Response> extends TorrentEvent.Request<R> implements Identifiable {

    public final HandleId handleId;

    public Request(Identifier eventId, OverlayId torrentId, HandleId handleId) {
      super(eventId, torrentId);
      this.handleId = handleId;
    }

    @Override
    public HandleId handleId() {
      return handleId;
    }
  }

  public static abstract class Response extends TorrentEvent.Response implements Identifiable {

    public final HandleId handleId;

    public Response(Request req) {
      super(req);
      this.handleId = req.handleId;
    }

    @Override
    public HandleId handleId() {
      return handleId;
    }
  }

  public static abstract class Timeout extends TorrentEvent.Timeout implements Identifiable {

    public final HandleId handleId;

    protected Timeout(ScheduleTimeout request, Identifier eventId, OverlayId torrentId, HandleId handleId) {
      super(request, eventId, torrentId);
      this.handleId = handleId;
    }

    protected Timeout(SchedulePeriodicTimeout request, Identifier eventId, OverlayId torrentId, HandleId handleId) {
      super(request, eventId, torrentId);
      this.handleId = handleId;
    }

    @Override
    public HandleId handleId() {
      return handleId;
    }
  }
}
