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
import se.sics.nstream.util.BlockDetails;

/**
 *
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class NetDetailedStateSerializer {

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
            NetDetailedState.Request obj = (NetDetailedState.Request) o;
            Serializers.toBinary(obj.eventId, buf);
            Serializers.toBinary(obj.torrentId, buf);
        }

        @Override
        public Object fromBinary(ByteBuf buf, Optional<Object> hint) {
            Identifier eventId = (Identifier) Serializers.fromBinary(buf, hint);
            Identifier torrentId = (Identifier) Serializers.fromBinary(buf, hint);
            return new NetDetailedState.Request(eventId, torrentId);
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
            NetDetailedState.Response obj = (NetDetailedState.Response) o;
            Serializers.toBinary(obj.eventId, buf);
            Serializers.toBinary(obj.torrentId, buf);
            buf.writeInt(obj.lastBlockDetails.getValue0());
            buf.writeInt(obj.lastBlockDetails.getValue1().blockSize);
            buf.writeInt(obj.lastBlockDetails.getValue1().defaultPieceSize);
            buf.writeInt(obj.lastBlockDetails.getValue1().lastPieceSize);
            buf.writeInt(obj.lastBlockDetails.getValue1().nrPieces);
        }

        @Override
        public Object fromBinary(ByteBuf buf, Optional<Object> hint) {
            Identifier eventId = (Identifier) Serializers.fromBinary(buf, hint);
            Identifier torrentId = (Identifier) Serializers.fromBinary(buf, hint);
            int lastBlockNr = buf.readInt();
            int blockSize = buf.readInt();
            int defaultPieceSize = buf.readInt();
            int lastPieceSize = buf.readInt();
            int nrPieces = buf.readInt();
            BlockDetails lastBlock = new BlockDetails(blockSize, nrPieces, defaultPieceSize, lastPieceSize);
            return new NetDetailedState.Response(eventId, torrentId, Pair.with(lastBlockNr, lastBlock));
        }
    }
}
