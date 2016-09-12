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
package se.sics.nstream.torrent.conn.upld.event;

import java.util.Map;
import java.util.Set;
import se.sics.kompics.KompicsEvent;
import se.sics.ktoolbox.util.identifiable.Identifier;
import se.sics.ktoolbox.util.identifiable.basic.UUIDIdentifier;
import se.sics.ktoolbox.util.network.KAddress;
import se.sics.ktoolbox.util.overlays.OverlayEvent;
import se.sics.ktoolbox.util.reference.KReference;
import se.sics.nstream.torrent.FileIdentifier;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class GetBlocks {
    public static class Request implements KompicsEvent {
        public final Identifier eventId;
        public final FileIdentifier fileId;
        public final KAddress target;
        public final Set<Integer> blocks;
        
        public Request(FileIdentifier fileId, KAddress target, Set<Integer> blocks) {
            this.eventId = UUIDIdentifier.randomId();
            this.fileId = fileId;
            this.target = target;
            this.blocks = blocks;
        }

        public Response success(Map<Integer, KReference<byte[]>> blocks) {
            return new Response(this, blocks);
        }
    }
    
    public static class Response implements OverlayEvent {
        public final Identifier eventId;
        public final FileIdentifier fileId;
        public final KAddress target;
        public final Map<Integer, KReference<byte[]>> blocks;
        
        private Response(Request req, Map<Integer, KReference<byte[]>> blocks) {
            this.eventId = req.eventId;
            this.fileId = req.fileId;
            this.target = req.target;
            this.blocks = blocks;
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
