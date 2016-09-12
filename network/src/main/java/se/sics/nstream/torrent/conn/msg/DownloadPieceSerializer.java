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

import com.google.common.base.Optional;
import io.netty.buffer.ByteBuf;
import org.javatuples.Pair;
import se.sics.kompics.network.netty.serialization.Serializer;
import se.sics.kompics.network.netty.serialization.Serializers;
import se.sics.ktoolbox.util.identifiable.Identifier;
import se.sics.ktoolbox.util.reference.KReference;
import se.sics.ktoolbox.util.reference.KReferenceException;
import se.sics.nstream.torrent.FileIdentifier;

/**
 *
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class DownloadPieceSerializer {
    public static class Request implements Serializer {
        private final int id;
        
        public Request(int id) {
            this.id = id;
        }

        @Override
        public int identifier() {
            return id;
        }

        @Override
        public void toBinary(Object o, ByteBuf buf) {
            DownloadPiece.Request obj = (DownloadPiece.Request)o;
            Serializers.toBinary(obj.eventId, buf);
            Serializers.lookupSerializer(FileIdentifier.class).toBinary(obj.fileId, buf);
            buf.writeInt(obj.piece.getValue0());
            buf.writeInt(obj.piece.getValue1());
        }

        @Override
        public Object fromBinary(ByteBuf buf, Optional<Object> hint) {
            Identifier eventId = (Identifier)Serializers.fromBinary(buf, hint);
            FileIdentifier fileId = (FileIdentifier)Serializers.lookupSerializer(FileIdentifier.class).fromBinary(buf, hint);
            int blockNr = buf.readInt();
            int pieceNr = buf.readInt();
            return new DownloadPiece.Request(eventId, fileId, Pair.with(blockNr, pieceNr));
        }
    }
    
    public static class Response implements Serializer {
        private final int id;
        
        public Response(int id) {
            this.id = id;
        }

        @Override
        public int identifier() {
            return id;
        }

        @Override
        public void toBinary(Object o, ByteBuf buf) {
            DownloadPiece.Response obj = (DownloadPiece.Response)o;
            Serializers.toBinary(obj.eventId, buf);
            Serializers.lookupSerializer(FileIdentifier.class).toBinary(obj.fileId, buf);
            buf.writeInt(obj.piece.getValue0());
            buf.writeInt(obj.piece.getValue1());
           
            byte[] piece = getPiece(obj.val.getLeft());
            buf.writeInt(piece.length);
            buf.writeBytes(piece);
        }
        
        private byte[] getPiece(KReference<byte[]> pieceRef) {
            byte[] piece = pieceRef.getValue().get();
            try {
                pieceRef.release();
            } catch (KReferenceException ex) {
                throw new RuntimeException(ex);
            }
            return piece;
        }

        @Override
        public Object fromBinary(ByteBuf buf, Optional<Object> hint) {
            Identifier eventId = (Identifier)Serializers.fromBinary(buf, hint);
            FileIdentifier fileId = (FileIdentifier)Serializers.lookupSerializer(FileIdentifier.class).fromBinary(buf, hint);
            int blockNr = buf.readInt();
            int pieceNr = buf.readInt();
            
            int pieceSize = buf.readInt();
            byte[] piece = new byte[pieceSize];
            buf.readBytes(piece);
            return new DownloadPiece.Response(eventId, fileId, Pair.with(blockNr, pieceNr), piece);
        }
    }
}
