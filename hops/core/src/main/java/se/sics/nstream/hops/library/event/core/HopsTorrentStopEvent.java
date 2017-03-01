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
package se.sics.nstream.hops.library.event.core;

import se.sics.gvod.stream.mngr.event.VoDMngrEvent;
import se.sics.kompics.Direct;
import se.sics.ktoolbox.nutil.fsm.api.FSMEvent;
import se.sics.ktoolbox.util.identifiable.BasicIdentifiers;
import se.sics.ktoolbox.util.identifiable.Identifier;
import se.sics.ktoolbox.util.identifiable.overlay.OverlayId;
import se.sics.ktoolbox.util.result.Result;

/**
 *
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class HopsTorrentStopEvent {

  public static class Request extends Direct.Request<Response> implements VoDMngrEvent, FSMEvent {

    public final Identifier eventId;
    public final OverlayId torrentId;

    public Request(Identifier eventId, OverlayId torrentId) {
      this.eventId = eventId;
      this.torrentId = torrentId;
    }

    public Request(OverlayId torrentId) {
      this(BasicIdentifiers.eventId(), torrentId);
    }

    @Override
    public Identifier getId() {
      return eventId;
    }

    @Override
    public Identifier getBaseId() {
      return torrentId.baseId;
    }

    public Response success() {
      return new Response(this, Result.success(true));
    }

    public Response badRequest(Exception ex) {
      return new Response(this, Result.badRequest(ex));
    }

    public Response internalFailure(Exception ex) {
      return new Response(this, Result.internalFailure(ex));
    }
  }

  public static class Response implements Direct.Response, VoDMngrEvent {

    public final Request req;
    public final Result<Boolean> result;

    public Response(Request req, Result<Boolean> result) {
      this.req = req;
      this.result = result;
    }

    @Override
    public Identifier getId() {
      return req.eventId;
    }
  }
}
