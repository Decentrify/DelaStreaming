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
package se.sics.gvod.core.downloadMngr;

import se.sics.gvod.common.event.GVoDEvent;
import se.sics.gvod.common.event.ReqStatus;
import se.sics.kompics.Direct;
import se.sics.kompics.util.Identifier;

/**
 * @author Alex Ormenisan <aaor@sics.se>
 */
public class Data {

  public static class Request extends Direct.Request<Response> implements GVoDEvent {

    public final Identifier eventId;
    public final Identifier overlayId;
    public final long readPos;
    public final int readBlockSize;

    public Request(Identifier eventId, Identifier overlayId, long readPos, int readBlockSize) {
      this.eventId = eventId;
      this.overlayId = overlayId;
      this.readPos = readPos;
      this.readBlockSize = readBlockSize;
    }

    @Override
    public Identifier getId() {
      return eventId;
    }

    public Response fail(ReqStatus status) {
      return new Response(this, status, null);
    }

    public Response success(byte[] block) {
      return new Response(this, ReqStatus.SUCCESS, block);
    }
  }

  public static class Response implements Direct.Response, GVoDEvent {

    public final Request req;
    public final ReqStatus status;
    public final byte[] block;

    private Response(Request req, ReqStatus status, byte[] block) {
      this.req = req;
      this.status = status;
      this.block = block;
    }

    @Override
    public Identifier getId() {
      return req.getId();
    }
  }
}
