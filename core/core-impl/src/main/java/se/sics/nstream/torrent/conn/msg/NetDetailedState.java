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
import se.sics.nstream.transfer.MyTorrent.ManifestDef;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class NetDetailedState {

    public static class Request implements OverlayEvent {

        public final Identifier eventId;
        public final Identifier torrentId;

        protected Request(Identifier eventId, Identifier torrentId) {
            this.eventId = eventId;
            this.torrentId = torrentId;
        }

        public Request(Identifier torrentId) {
            this(UUIDIdentifier.randomId(), torrentId);
        }

        @Override
        public Identifier overlayId() {
            return torrentId;
        }

        @Override
        public Identifier getId() {
            return eventId;
        }

        public Response success(ManifestDef manifestDef) {
            return new Response(this, manifestDef
            );
        }
    }

    public static class Response implements OverlayEvent {

        public final Identifier eventId;
        public final Identifier torrentId;
        public final ManifestDef manifestDef;

        protected Response(Identifier eventId, Identifier torrentId, ManifestDef manifestDef) {
            this.eventId = eventId;
            this.torrentId = torrentId;
            this.manifestDef = manifestDef;
        }

        private Response(Request req, ManifestDef manifestDef) {
            this(req.eventId, req.torrentId, manifestDef);
        }

        @Override
        public Identifier overlayId() {
            return torrentId;
        }

        @Override
        public Identifier getId() {
            return eventId;
        }
    }
}
