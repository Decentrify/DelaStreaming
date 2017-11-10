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

import se.sics.kompics.util.Identifier;
import se.sics.ktoolbox.util.identifiable.BasicIdentifiers;
import se.sics.ktoolbox.util.identifiable.overlay.OverlayId;
import se.sics.ktoolbox.util.overlays.OverlayEvent;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class NetConnect {
    public static class Request implements OverlayEvent {
        public final Identifier msgId;
        public final OverlayId torrentId;
        
        protected Request(Identifier msgId, OverlayId torrentId) {
            this.msgId = msgId;
            this.torrentId = torrentId;
        }
        
        public Request(OverlayId torrentId) {
            this(BasicIdentifiers.msgId(), torrentId);
        }
        
        @Override
        public OverlayId overlayId() {
            return torrentId;
        }

        @Override
        public Identifier getId() {
            return msgId;
        }
        
        public Response answer(boolean result) {
            return new Response(this, result);
        }
    }
    
    public static class Response implements OverlayEvent {
        public final Identifier msgId;
        public final OverlayId torrentId;
        public final boolean result;
        
        protected Response(Identifier msgId, OverlayId torrentId, boolean result) {
            this.msgId = msgId;
            this.torrentId = torrentId;
            this.result = result;
        }
        
        private Response(Request req, boolean result) {
            this(req.msgId, req.torrentId, result);
        }

        @Override
        public OverlayId overlayId() {
            return torrentId;
        }

        @Override
        public Identifier getId() {
            return msgId;
        }
    }
}
