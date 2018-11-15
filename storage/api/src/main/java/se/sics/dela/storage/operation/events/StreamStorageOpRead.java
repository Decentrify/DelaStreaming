/*
 * Copyright (C) 2009 Swedish Institute of Computer Science (SICS) Copyright (C)
 * 2009 Royal Institute of Technology (KTH)
 *
 * KompicsToolbox is free software; you can redistribute it and/or
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
package se.sics.dela.storage.operation.events;

import se.sics.kompics.Direct;
import se.sics.kompics.util.Identifier;
import se.sics.ktoolbox.util.result.Result;
import se.sics.ktoolbox.util.trysf.Try;
import se.sics.nstream.StreamId;
import se.sics.nstream.util.range.KBlock;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class StreamStorageOpRead {

  public static class Request extends Direct.Request<Response> implements StreamStorageOpEvent {

    public final Identifier eventId;
    public final StreamId streamId;
    public final KBlock readRange;

    public Request(Identifier eventId, StreamId streamId, KBlock readRange) {
      this.eventId = eventId;
      this.streamId = streamId;
      this.readRange = readRange;
    }

    @Override
    public Identifier getId() {
      return eventId;
    }

    @Override
    public StreamId getStreamId() {
      return streamId;
    }

    @Override
    public Identifier getEndpointId() {
      return streamId.endpointId;
    }

    public Response respond(Result<byte[]> result) {
      return new Response(this, result);
    }

    public Response respond(Try<byte[]> result) {
      try {
        result.checkedGet();
        return new Response(this, Result.success(result.get()));
      } catch (Throwable t) {
        return new Response(this, Result.internalFailure((Exception) t));
      }
    }
  }

  public static class Response implements Direct.Response, StreamStorageOpEvent {

    public final Request req;
    public final Result<byte[]> result;

    public Response(Request req, Result<byte[]> result) {
      this.req = req;
      this.result = result;
    }

    @Override
    public Identifier getId() {
      return req.getId();
    }

    @Override
    public StreamId getStreamId() {
      return req.getStreamId();
    }

    @Override
    public Identifier getEndpointId() {
      return req.getEndpointId();
    }
  }

  public static class Complete implements StreamStorageOpEvent {

    public final Identifier eventId;
    public final StreamId streamId;

    public Complete(Identifier eventId, StreamId streamId) {
      this.eventId = eventId;
      this.streamId = streamId;
    }

    @Override
    public StreamId getStreamId() {
      return streamId;
    }

    @Override
    public Identifier getEndpointId() {
      return streamId.endpointId;
    }

    @Override
    public Identifier getId() {
      return eventId;
    }
  }
}
