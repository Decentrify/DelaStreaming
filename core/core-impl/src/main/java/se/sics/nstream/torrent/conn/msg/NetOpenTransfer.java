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
import se.sics.nstream.torrent.FileIdentifier;

/**
 *
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class NetOpenTransfer {

    public static class Request implements OverlayEvent {

        public final Identifier eventId;
        public final FileIdentifier fileId;

        protected Request(Identifier eventId, FileIdentifier fileId) {
            this.eventId = eventId;
            this.fileId = fileId;
        }

        public Request(FileIdentifier fileId) {
            this(UUIDIdentifier.randomId(), fileId);
        }
        
        @Override
        public Identifier getId() {
            return eventId;
        }

        @Override
        public Identifier overlayId() {
            return fileId.overlayId;
        }
        
        public Response answer(boolean result) {
            return new Response(this, result);
        }
    }
    
    public static class Response implements OverlayEvent {

        public final Identifier eventId;
        public final FileIdentifier fileId;
        public final boolean result;

        protected Response(Identifier eventId, FileIdentifier fileId, boolean result) {
            this.eventId = eventId;
            this.fileId = fileId;
            this.result = result;
        }

        private Response(Request req, boolean result) {
            this(req.eventId, req.fileId, result);
        }
        
        @Override
        public Identifier getId() {
            return eventId;
        }

        @Override
        public Identifier overlayId() {
            return fileId.overlayId;
        }

    }
}
