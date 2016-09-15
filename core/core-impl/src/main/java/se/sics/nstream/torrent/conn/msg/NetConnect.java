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

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class NetConnect {
    public static class Request implements OverlayEvent {
        public final Identifier eventId;
        public final Identifier overlayId;
        
        protected Request(Identifier eventId, Identifier overlayId) {
            this.eventId = eventId;
            this.overlayId = overlayId;
        }
        
        public Request(Identifier overlayId) {
            this(UUIDIdentifier.randomId(), overlayId);
        }
        
        @Override
        public Identifier overlayId() {
            return overlayId;
        }

        @Override
        public Identifier getId() {
            return eventId;
        }
        
        public Response answer(boolean result) {
            return new Response(this, result);
        }
    }
    
    public static class Response implements OverlayEvent {
        public final Identifier eventId;
        public final Identifier overlayId;
        public final boolean result;
        
        protected Response(Identifier eventId, Identifier overlayId, boolean result) {
            this.eventId = eventId;
            this.overlayId = overlayId;
            this.result = result;
        }
        
        private Response(Request req, boolean result) {
            this(req.eventId, req.overlayId, result);
        }

        @Override
        public Identifier overlayId() {
            return overlayId;
        }

        @Override
        public Identifier getId() {
            return eventId;
        }
    }
}
