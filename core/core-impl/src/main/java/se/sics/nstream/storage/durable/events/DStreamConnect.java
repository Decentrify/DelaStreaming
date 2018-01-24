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
package se.sics.nstream.storage.durable.events;

import org.javatuples.Pair;
import se.sics.kompics.Direct;
import se.sics.kompics.fsm.FSMEvent;
import se.sics.kompics.util.Identifiable;
import se.sics.kompics.util.Identifier;
import se.sics.ktoolbox.util.identifiable.BasicIdentifiers;
import se.sics.nstream.StreamId;
import se.sics.nstream.storage.durable.util.MyStream;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class DStreamConnect {

  public static class Request extends Direct.Request<Success> implements DStreamEvent {

    public final Identifier eventId;
    public final Pair<StreamId, MyStream> stream;

    public Request(Pair<StreamId, MyStream> stream) {
      this.eventId = BasicIdentifiers.eventId();
      this.stream = stream;
    }
    
    public Request(StreamId streamId, MyStream stream) {
      this(Pair.with(streamId, stream));
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
      return stream.getValue0();
    }

    @Override
    public Identifier getEndpointId() {
      return stream.getValue0().endpointId;
    }

    @Override
    public String toString() {
      return "Request{" + "streamId=<" + stream.getValue0().toString() + ">}";
    }
  }

  public static class Success implements Direct.Response, Identifiable, FSMEvent {

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
  }
}
