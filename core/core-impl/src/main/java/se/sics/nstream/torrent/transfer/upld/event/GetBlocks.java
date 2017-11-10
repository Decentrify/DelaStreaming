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
package se.sics.nstream.torrent.transfer.upld.event;

import java.util.Map;
import java.util.Set;
import se.sics.kompics.Direct;
import se.sics.kompics.util.Identifier;
import se.sics.ktoolbox.util.identifiable.BasicIdentifiers;
import se.sics.ktoolbox.util.identifiable.overlay.OverlayId;
import se.sics.ktoolbox.util.reference.KReference;
import se.sics.nstream.ConnId;
import se.sics.nstream.storage.cache.KHint;
import se.sics.nstream.torrent.transfer.TorrentConnEvent;
import se.sics.nstream.util.BlockDetails;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class GetBlocks {

    public static class Request extends Direct.Request<Response> implements TorrentConnEvent {

        public final Identifier eventId;
        public final ConnId connId;
        public final Set<Integer> blocks;
        public final boolean withHashes;
        
        public final KHint.Summary cacheHint;

        public Request(ConnId connId, Set<Integer> blocks, boolean withHashes, KHint.Summary cacheHint) {
            this.eventId = BasicIdentifiers.eventId();
            this.connId = connId;
            this.blocks = blocks;
            this.withHashes = withHashes;
            this.cacheHint = cacheHint;
        }

        @Override
        public Identifier getId() {
            return eventId;
        }

        @Override
        public OverlayId overlayId() {
            return connId.fileId.torrentId;
        }

        @Override
        public ConnId connId() {
            return connId;
        }

        public Response success(Map<Integer, byte[]> hashes, Map<Integer, KReference<byte[]>> blocks, Map<Integer, BlockDetails> irregularBlocks) {
            return new Response(this, hashes, blocks, irregularBlocks);
        }
    }

    public static class Response implements Direct.Response, TorrentConnEvent {

        public final Identifier eventId;
        public final ConnId connId;
        public final Map<Integer, byte[]> hashes;
        public final Map<Integer, BlockDetails> irregularBlocks;
        public final Map<Integer, KReference<byte[]>> blocks;

        private Response(Request req, Map<Integer, byte[]> hashes, Map<Integer, KReference<byte[]>> blocks, Map<Integer, BlockDetails> irregularBlocks) {
            this.eventId = req.eventId;
            this.connId = req.connId;
            this.hashes = hashes;
            this.blocks = blocks;
            this.irregularBlocks = irregularBlocks;
        }

        @Override
        public Identifier getId() {
            return eventId;
        }

        @Override
        public OverlayId overlayId() {
            return connId.fileId.torrentId;
        }

        @Override
        public ConnId connId() {
            return connId;
        }
    }
}
