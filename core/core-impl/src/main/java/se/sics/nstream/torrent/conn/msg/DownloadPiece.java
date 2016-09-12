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

import org.javatuples.Pair;
import se.sics.ktoolbox.util.Either;
import se.sics.ktoolbox.util.identifiable.Identifier;
import se.sics.ktoolbox.util.identifiable.basic.UUIDIdentifier;
import se.sics.ktoolbox.util.overlays.OverlayEvent;
import se.sics.ktoolbox.util.reference.KReference;
import se.sics.nstream.torrent.FileIdentifier;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class DownloadPiece {
    public static class Request implements OverlayEvent {
        public final Identifier eventId;
        public final FileIdentifier fileId;
        public final Pair<Integer, Integer> piece;
        
        protected Request(Identifier eventId, FileIdentifier fileId, Pair<Integer, Integer> piece) {
            this.eventId = eventId;
            this.fileId = fileId;
            this.piece = piece;
        }

        public Request(FileIdentifier fileId, Pair<Integer, Integer> piece) {
            this(UUIDIdentifier.randomId(), fileId, piece);
        }
        
        @Override
        public Identifier getId() {
            return eventId;
        }

        @Override
        public Identifier overlayId() {
            return fileId;
        }
        
        public Response success(KReference<byte[]> val) {
            return new Response(this, val);
        }
    }
    
    public static class Response implements OverlayEvent {
        public final Identifier eventId;
        public final FileIdentifier fileId;
        public final Pair<Integer, Integer> piece;
        public final Either<KReference<byte[]>, byte[]> val;
        
        private Response(Identifier eventId, FileIdentifier fileId, Pair<Integer, Integer> piece, Either val) {
            this.eventId = eventId;
            this.fileId = fileId;
            this.piece = piece;
            this.val = val;
        }
        
        private Response(Request req, KReference<byte[]> val) {
            this(req.eventId, req.fileId, req.piece, Either.left(val));
        }
        
        protected Response(Identifier eventId, FileIdentifier fileId, Pair<Integer, Integer> piece, byte[] val) {
            this(eventId, fileId, piece, Either.right(val));
        }
        
        @Override
        public Identifier getId() {
            return eventId;
        }
        
        @Override
        public Identifier overlayId() {
            return fileId;
        }
    }
}
