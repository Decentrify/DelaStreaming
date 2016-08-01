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

import com.google.common.base.Optional;
import io.netty.buffer.ByteBuf;
import java.util.Map;
import se.sics.kompics.network.netty.serialization.Serializer;
import se.sics.kompics.network.netty.serialization.Serializers;
import se.sics.ktoolbox.util.SerializerHelper;
import se.sics.ktoolbox.util.identifiable.Identifier;
import se.sics.nstream.storage.cache.KHint;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class BlockGetSerializer {
    public static final class Request implements Serializer {

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
            BlockGet.Request obj = (BlockGet.Request) o;
            Serializers.toBinary(obj.eventId, buf);
            Serializers.toBinary(obj.overlayId, buf);
            SerializerHelper.stringToBinary(obj.fileName, buf);

            buf.writeInt(obj.blockNr);
            SerializerHelper.sKMtoBinary(obj.cacheHints, KHint.Summary.class, buf);
        }

        @Override
        public Object fromBinary(ByteBuf buf, Optional<Object> hint) {
            Identifier eventId = (Identifier) Serializers.fromBinary(buf, hint);
            Identifier overlayId = (Identifier) Serializers.fromBinary(buf, hint);
            String fileName = SerializerHelper.stringFromBinary(buf);
            int blockNr = buf.readInt();
            Map<String, KHint.Summary> cacheHints = SerializerHelper.sKMFromBinary(KHint.Summary.class, buf);
            return new BlockGet.Request(eventId, overlayId, cacheHints, fileName, blockNr);
        }
    }
}
