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
package se.sics.nstream.torrent.conn.msg;

import se.sics.ktoolbox.util.identifiable.Identifier;
import se.sics.ktoolbox.util.identifiable.basic.UUIDIdentifier;
import se.sics.ktoolbox.util.overlays.OverlayEvent;
import se.sics.nstream.storage.cache.KHint;
import se.sics.nstream.torrent.FileIdentifier;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class CacheHint {

    public static class Request implements OverlayEvent {

        public final Identifier eventId;
        public final FileIdentifier fileId;
        public final KHint.Summary requestCache;

        protected Request(Identifier eventId, FileIdentifier fileId, KHint.Summary requestCache) {
            this.eventId = eventId;
            this.fileId = fileId;
            this.requestCache = requestCache;
        }
        public Request(FileIdentifier fileId, KHint.Summary requestCache) {
            this(UUIDIdentifier.randomId(), fileId, requestCache);
        }

        @Override
        public Identifier overlayId() {
            return fileId;
        }

        @Override
        public Identifier getId() {
            return eventId;
        }
        
        public Response success() {
            return new Response(this);
        }
    }
    
    public static class Response implements OverlayEvent {
        public final Identifier eventId;
        public final FileIdentifier fileId;
        
        protected Response(Identifier eventId, FileIdentifier fileId) {
            this.eventId = eventId;
            this.fileId = fileId;
        }
        
        private Response(Request req) {
            this(req.eventId, req.fileId);
        }

        @Override
        public Identifier overlayId() {
            return fileId;
        }

        @Override
        public Identifier getId() {
            return eventId;
        }
    }
}
