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
package se.sics.nstream.old.torrent.event;

import java.nio.ByteBuffer;
import java.util.Map;
import org.javatuples.Pair;
import se.sics.ktoolbox.util.identifiable.BasicIdentifiers;
import se.sics.ktoolbox.util.identifiable.Identifier;
import se.sics.ktoolbox.util.identifiable.overlay.OverlayId;
import se.sics.ktoolbox.util.reference.KReference;
import se.sics.ktoolbox.util.result.Result;
import se.sics.nstream.storage.cache.KHint;
import se.sics.nstream.util.event.StreamMsg;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class PieceGet {
    public static class Request implements StreamMsg.Request {

        public final Identifier msgId;
        public final OverlayId overlayId;
        public final Map<String, KHint.Summary> cacheHints;
        public final String fileName;
        public final Pair<Integer, Integer> pieceNr;
        
        protected Request(Identifier msgId, OverlayId overlayId, Map<String, KHint.Summary> cacheHints, String fileName, Pair<Integer, Integer> pieceNr) {
            this.msgId = msgId;
            this.overlayId = overlayId;
            this.cacheHints = cacheHints;
            this.fileName = fileName;
            this.pieceNr = pieceNr;
        }

        public Request(OverlayId overlayId, Map<String, KHint.Summary> cacheHints, String fileName, Pair<Integer, Integer> pieceNr) {
            this(BasicIdentifiers.msgId(), overlayId, cacheHints, fileName, pieceNr);
        }

        @Override
        public Identifier getId() {
            return msgId;
        }

        @Override
        public OverlayId overlayId() {
            return overlayId;
        }
        
        public Response success(KReference<byte[]> piece) {
            return new Response(msgId, msgId, overlayId, Result.Status.SUCCESS, fileName, pieceNr, ByteBuffer.wrap(piece.getValue().get()));
        }
        
        public Response missingBlock() {
            return new Response(msgId, msgId, overlayId, Result.Status.BAD_REQUEST, fileName, pieceNr, null);
        }
    }
    
    public static class RangeRequest implements StreamMsg.Request {

        public final Identifier msgId;
        public final OverlayId overlayId;
        public final Map<String, KHint.Summary> cacheHints;
        public final String fileName;
        public final int blockNr;
        public final int from;
        public final int to;
        
        protected RangeRequest(Identifier msgId, OverlayId overlayId, Map<String, KHint.Summary> cacheHints, String fileName, int blockNr, int from, int to) {
            this.msgId = msgId;
            this.overlayId = overlayId;
            this.cacheHints = cacheHints;
            this.fileName = fileName;
            this.blockNr = blockNr;
            this.from = from;
            this.to = to;
        }

        public RangeRequest(OverlayId overlayId, Map<String, KHint.Summary> cacheHints, String fileName, int blockNr, int from, int to) {
            this(BasicIdentifiers.msgId(), overlayId, cacheHints, fileName, blockNr, from, to);
        }

        @Override
        public Identifier getId() {
            return msgId;
        }

        @Override
        public OverlayId overlayId() {
            return overlayId;
        }
    }
    
    public static class Response implements StreamMsg.Response {
        public final Identifier msgId;
        public final Identifier reqId;
        public final OverlayId overlayId;
        public final Result.Status status;
        public final String fileName;
        public final Pair<Integer, Integer> pieceNr;
        public final ByteBuffer piece;
        
        protected Response(Identifier msgId, Identifier reqId, OverlayId overlayId, Result.Status status, String fileName, 
                Pair<Integer, Integer> pieceNr, ByteBuffer piece) {
            this.msgId = msgId;
            this.reqId = reqId;
            this.overlayId = overlayId;
            this.status = status;
            this.fileName = fileName;
            this.pieceNr = pieceNr;
            this.piece = piece;
        }
        
        @Override
        public Identifier getId() {
            return msgId;
        }
        
        @Override
        public OverlayId overlayId() {
            return overlayId;
        }
        
        @Override
        public Result.Status getStatus() {
            return status;
        }
    }
}
