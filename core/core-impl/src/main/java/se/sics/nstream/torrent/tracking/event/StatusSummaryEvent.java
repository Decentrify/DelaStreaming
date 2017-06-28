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
package se.sics.nstream.torrent.tracking.event;

import se.sics.nstream.util.TorrentExtendedStatus;
import se.sics.kompics.Direct;
import se.sics.ktoolbox.util.identifiable.BasicIdentifiers;
import se.sics.ktoolbox.util.identifiable.Identifier;
import se.sics.ktoolbox.util.identifiable.overlay.OverlayId;
import se.sics.ktoolbox.util.overlays.OverlayEvent;
import se.sics.nstream.StreamEvent;
import se.sics.nstream.library.restart.LibTFSMEvent;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class StatusSummaryEvent {
    public static class Request extends Direct.Request<Response> implements StreamEvent, OverlayEvent {
        public final Identifier eventId;
        public final OverlayId torrentId;
        
        public Request(Identifier eventId, OverlayId torrentId) {
            this.eventId = eventId;
            this.torrentId = torrentId;
        }
        
        public Request(OverlayId torrentId) {
            this(BasicIdentifiers.eventId(), torrentId);
        }
        
        @Override
        public Identifier getId() {
            return eventId;
        }
        
        @Override
        public OverlayId overlayId() {
            return torrentId;
        }
        
        public Response success(TorrentExtendedStatus value) {
            return new Response(this, value);
        }
    }
    
    public static class Response implements Direct.Response, StreamEvent, OverlayEvent, LibTFSMEvent {
        public final Request req;
        public final TorrentExtendedStatus result;

        public Response(Request req, TorrentExtendedStatus result) {
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

    @Override
    public Identifier getLibTFSMId() {
      return req.torrentId.baseId;
    }
    }
}
