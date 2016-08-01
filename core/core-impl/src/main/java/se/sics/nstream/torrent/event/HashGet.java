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
package se.sics.nstream.torrent.event;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Set;
import se.sics.ktoolbox.util.identifiable.Identifier;
import se.sics.ktoolbox.util.identifiable.basic.UUIDIdentifier;
import se.sics.ktoolbox.util.result.Result;
import se.sics.nstream.storage.cache.KHint;
import se.sics.nstream.util.event.StreamMsg;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class HashGet {

    public static class Request implements StreamMsg.Request {

        public final Identifier eventId;
        public final Identifier overlayId;
        public final Map<String, KHint.Summary> cacheHints;
        public final String fileName;
        public final int targetPos;
        public final Set<Integer> hashes;

        protected Request(Identifier eventId, Identifier overlayId, Map<String, KHint.Summary> cacheHints, String fileName, int targetPos, Set<Integer> hashes) {
            this.eventId = eventId;
            this.overlayId = overlayId;
            this.cacheHints = cacheHints;
            this.fileName = fileName;
            this.targetPos = targetPos;
            this.hashes = hashes;
        }

        public Request(Identifier overlayId, Map<String, KHint.Summary> hints, String fileName, int targetPos, Set<Integer> hashes) {
            this(UUIDIdentifier.randomId(), overlayId, hints, fileName, targetPos, hashes);
        }

        @Override
        public Identifier getId() {
            return eventId;
        }

        @Override
        public Identifier overlayId() {
            return overlayId;
        }
        
        public Response success(Map<Integer, ByteBuffer> hashes, Set<Integer> missingHashes) {
            return new Response(this, Result.Status.SUCCESS, hashes, missingHashes);
        }
    }

    public static class Response implements StreamMsg.Response {

        public final Identifier eventId;
        public final Identifier overlayId;
        public final Result.Status status;
        public final String fileName;
        public final int targetPos;
        public final Map<Integer, ByteBuffer> hashes;
        public final Set<Integer> missingHashes;

        protected Response(Identifier eventId, Identifier overlayId, Result.Status status, String fileName, 
                int targetPos, Map<Integer, ByteBuffer> hashes, Set<Integer> missingHashes) {
            this.eventId = eventId;
            this.overlayId = overlayId;
            this.status = status;
            this.fileName = fileName;
            this.targetPos = targetPos;
            this.hashes = hashes;
            this.missingHashes = missingHashes;
        }

        public Response(HashGet.Request req, Result.Status status, Map<Integer, ByteBuffer> hashes, Set<Integer> missingHashes) {
            this(req.eventId, req.overlayId, status, req.fileName, req.targetPos, hashes, missingHashes);
        }

        @Override
        public Identifier getId() {
            return eventId;
        }

        @Override
        public Identifier overlayId() {
            return overlayId;
        }

        @Override
        public Result.Status getStatus() {
            return status;
        }
    }
}
