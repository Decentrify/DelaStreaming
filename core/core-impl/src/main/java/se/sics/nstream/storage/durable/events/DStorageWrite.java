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
package se.sics.nstream.storage.durable.events;

import se.sics.kompics.Direct;
import se.sics.kompics.util.Identifier;
import se.sics.ktoolbox.util.identifiable.BasicIdentifiers;
import se.sics.ktoolbox.util.result.Result;
import se.sics.nstream.StreamId;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class DStorageWrite {
    public static class Request extends Direct.Request<Response> implements DStreamEvent {
        public final Identifier eventId;
        public final StreamId streamId;
        public final long pos;
        public final byte[] value;
        
        public Request(Identifier eventId, StreamId streamId, long pos, byte[] value) {
            this.eventId = eventId;
            this.streamId = streamId;
            this.pos = pos;
            this.value = value;
        }
        
        public Request(StreamId streamId, long pos, byte[] value) {
            this(BasicIdentifiers.eventId(), streamId, pos, value);
        }
        
        @Override
        public Identifier getId() {
            return eventId;
        }
        
        public Response respond(Result<Boolean> result) {
            return new Response(this, result);
        }

        @Override
        public StreamId getStreamId() {
            return streamId;
        }

        @Override
        public Identifier getEndpointId() {
            return streamId.endpointId;
        }
    }
    
    public static class Response implements Direct.Response, DStreamEvent {
        public final Request req;
        public final Result<Boolean> result;
        
        public Response(Request req, Result<Boolean> result) {
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
}
