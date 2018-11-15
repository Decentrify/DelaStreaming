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
package se.sics.dela.storage.mngr.stream.events;

import se.sics.dela.storage.StreamStorage;
import se.sics.kompics.Direct;
import se.sics.kompics.util.Identifier;
import se.sics.nstream.StreamId;

/**
 * @author Alex Ormenisan <aaor@kth.se>2
 */
public class StreamMngrConnect {

  public static class Request extends Direct.Request<Success> implements StreamMngrEvent {

    public final Identifier eventId;
    public final Identifier clientId;
    public final StreamId streamId;
    public final StreamStorage stream;

    public Request(Identifier eventId, Identifier clientId, StreamId streamId, StreamStorage stream) {
      this.eventId = eventId;
      this.clientId = clientId;
      this.streamId = streamId;
      this.stream = stream;
    }

    @Override
    public Identifier getId() {
      return eventId;
    }

    public Success success(long filePos) {
      return new Success(this, filePos);
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
    public String toString() {
      return "Request{" + "streamId=<" + streamId.toString() + ">}";
    }

    @Override
    public Identifier getClientId() {
      return clientId;
    }
  }

  public static class Success implements Direct.Response, StreamMngrEvent {

    public final Request req;
    public final long streamPos;

    private Success(Request req, long streamPos) {
      this.req = req;
      this.streamPos = streamPos;
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

    @Override
    public Identifier getClientId() {
      return req.getClientId();
    }
  }
}
