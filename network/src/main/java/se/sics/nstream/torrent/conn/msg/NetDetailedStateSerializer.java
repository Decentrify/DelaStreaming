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
import se.sics.kompics.id.Identifier;
import se.sics.kompics.network.netty.serialization.Serializer;
import se.sics.kompics.network.netty.serialization.Serializers;
import se.sics.ktoolbox.util.identifiable.BasicIdentifiers;
import se.sics.ktoolbox.util.identifiable.IdentifierRegistry;
import se.sics.ktoolbox.util.identifiable.overlay.OverlayId;
import se.sics.nstream.transfer.MyTorrent;
import se.sics.nstream.util.BlockDetails;

/**
 *
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class NetDetailedStateSerializer {

    public static class Request implements Serializer {

        private final int id;
        private final Class msgIdType;

        public Request(int id) {
            this.id = id;
            this.msgIdType = IdentifierRegistry.lookup(BasicIdentifiers.Values.MSG.toString()).idType();
        }

        @Override
        public int identifier() {
            return id;
        }

        @Override
        public void toBinary(Object o, ByteBuf buf) {
            NetDetailedState.Request obj = (NetDetailedState.Request) o;
            Serializers.lookupSerializer(msgIdType).toBinary(obj.msgId, buf);
            Serializers.lookupSerializer(OverlayId.class).toBinary(obj.torrentId, buf);
        }

        @Override
        public Object fromBinary(ByteBuf buf, Optional<Object> hint) {
            Identifier msgId = (Identifier) Serializers.lookupSerializer(msgIdType).fromBinary(buf, hint);
            OverlayId torrentId = (OverlayId) Serializers.lookupSerializer(OverlayId.class).fromBinary(buf, hint);
            return new NetDetailedState.Request(msgId, torrentId);
        }
    }

    public static class Response implements Serializer {

        private final int id;
        private final Class msgIdType;

        public Response(int id) {
            this.id = id;
            this.msgIdType = IdentifierRegistry.lookup(BasicIdentifiers.Values.MSG.toString()).idType();
        }

        @Override
        public int identifier() {
            return id;
        }

        @Override
        public void toBinary(Object o, ByteBuf buf) {
            NetDetailedState.Response obj = (NetDetailedState.Response) o;
            Serializers.lookupSerializer(msgIdType).toBinary(obj.msgId, buf);
            Serializers.lookupSerializer(OverlayId.class).toBinary(obj.torrentId, buf);
            buf.writeInt(obj.manifestDef.nrBlocks);
            Serializers.lookupSerializer(BlockDetails.class).toBinary(obj.manifestDef.lastBlock, buf);
        }

        @Override
        public Object fromBinary(ByteBuf buf, Optional<Object> hint) {
            Identifier msgId = (Identifier) Serializers.lookupSerializer(msgIdType).fromBinary(buf, hint);
            OverlayId torrentId = (OverlayId) Serializers.lookupSerializer(OverlayId.class).fromBinary(buf, hint);
            int nrBlocks = buf.readInt();
            int blockSize = buf.readInt();
            int defaultPieceSize = buf.readInt();
            int lastPieceSize = buf.readInt();
            int nrPieces = buf.readInt();
            BlockDetails lastBlock = new BlockDetails(blockSize, nrPieces, defaultPieceSize, lastPieceSize);
            return new NetDetailedState.Response(msgId, torrentId, new MyTorrent.ManifestDef(nrBlocks, lastBlock));
        }
    }
}
