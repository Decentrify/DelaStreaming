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
import se.sics.ktoolbox.util.identifiable.Identifier;
import se.sics.ktoolbox.util.identifiable.basic.UUIDIdentifier;
import se.sics.ktoolbox.util.result.Result;
import se.sics.nstream.storage.cache.KHint;
import se.sics.nstream.util.event.StreamMsg;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class PieceGet {
    public static class Request implements StreamMsg.Request {

        public final Identifier eventId;
        public final Identifier overlayId;
        public final Map<String, KHint.Summary> cacheHints;
        public final String fileName;
        public final int pieceNr;
        
        protected Request(Identifier eventId, Identifier overlayId, Map<String, KHint.Summary> cacheHints, String fileName, int pieceNr) {
            this.eventId = eventId;
            this.overlayId = overlayId;
            this.cacheHints = cacheHints;
            this.fileName = fileName;
            this.pieceNr = pieceNr;
        }

        public Request(Identifier overlayId, Map<String, KHint.Summary> cacheHints, String fileName, int pieceNr) {
            this(UUIDIdentifier.randomId(), overlayId, cacheHints, fileName, pieceNr);
        }

        @Override
        public Identifier getId() {
            return eventId;
        }

        @Override
        public Identifier overlayId() {
            return overlayId;
        }
    }

    public static class Response implements StreamMsg.Response {
        public final Identifier eventId;
        public final Identifier overlayId;
        public final Result.Status status;
        public final String fileName;
        public final int pieceNr;
        public final ByteBuffer piece;
        
        protected Response(Identifier eventId, Identifier overlayId, Result.Status status, String fileName, int pieceNr, ByteBuffer piece) {
            this.eventId = eventId;
            this.overlayId = overlayId;
            this.status = status;
            this.fileName = fileName;
            this.pieceNr = pieceNr;
            this.piece = piece;
        }
        
        public Response(HashGet.Request req, Result.Status status, String fileName, int pieceNr, ByteBuffer piece) {
           this(req.eventId, req.overlayId, status, fileName, pieceNr, piece);
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
