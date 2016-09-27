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
package se.sics.nstream.storage;

import se.sics.kompics.Direct;
import se.sics.ktoolbox.util.identifiable.Identifier;
import se.sics.ktoolbox.util.identifiable.overlay.OverlayId;
import se.sics.ktoolbox.util.identifiable.basic.UUIDId;
import se.sics.ktoolbox.util.overlays.OverlayEvent;
import se.sics.ktoolbox.util.result.Result;
import se.sics.nstream.util.StreamResource;
import se.sics.nstream.util.range.KBlock;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class StorageRead {
    public static class Request extends Direct.Request<Response> implements OverlayEvent {
        public final Identifier eventId;
        public final StreamResource resource;
        public final KBlock readRange;
        
        protected Request(Identifier eventId, StreamResource resource, KBlock readRange) {
            this.eventId = eventId;
            this.resource = resource;
            this.readRange = readRange;
        }
        
        public Request(StreamResource resource, KBlock readRange) {
            this(UUIDId.randomId(), resource, readRange);
        }
        
        @Override
        public Identifier getId() {
            return eventId;
        }

        @Override
        public OverlayId overlayId() {
            return resource.getResourceId();
        }
        
        public Response respond(Result<byte[]> result) {
            return new Response(this, result);
        }
    }
    
    public static class Response implements Direct.Response, OverlayEvent {
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
        public OverlayId overlayId() {
            return req.overlayId();
        }
    }
}
