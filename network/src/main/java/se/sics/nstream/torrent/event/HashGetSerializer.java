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
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import se.sics.kompics.network.netty.serialization.Serializer;
import se.sics.kompics.network.netty.serialization.Serializers;
import se.sics.ktoolbox.util.SerializerHelper;
import se.sics.ktoolbox.util.identifiable.Identifier;
import se.sics.ktoolbox.util.result.Result;
import se.sics.nstream.storage.cache.KHint;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class HashGetSerializer {

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
            HashGet.Request obj = (HashGet.Request) o;
            Serializers.toBinary(obj.eventId, buf);
            Serializers.toBinary(obj.overlayId, buf);
            SerializerHelper.stringToBinary(obj.fileName, buf);

            buf.writeInt(obj.targetPos);
            buf.writeInt(obj.hashes.size());
            for (Integer hash : obj.hashes) {
                buf.writeInt(hash);
            }
            SerializerHelper.sKMtoBinary(obj.cacheHints, KHint.Summary.class, buf);
        }

        @Override
        public Object fromBinary(ByteBuf buf, Optional<Object> hint) {
            Identifier eventId = (Identifier) Serializers.fromBinary(buf, hint);
            Identifier overlayId = (Identifier) Serializers.fromBinary(buf, hint);
            String fileName = SerializerHelper.stringFromBinary(buf);

            int targetPos = buf.readInt();
            int nrHashes = buf.readInt();
            Set<Integer> hashes = new HashSet<>();
            for (int i = 0; i < nrHashes; i++) {
                hashes.add(buf.readInt());
            }
            Map<String, KHint.Summary> cacheHints = SerializerHelper.sKMFromBinary(KHint.Summary.class, buf);
            return new HashGet.Request(eventId, overlayId, cacheHints, fileName, targetPos, hashes);
        }
    }

    public static final class Response implements Serializer {

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
            HashGet.Response obj = (HashGet.Response) o;
            Serializers.toBinary(obj.eventId, buf);
            Serializers.toBinary(obj.overlayId, buf);
            Serializers.lookupSerializer(Result.Status.class).toBinary(obj.status, buf);
            SerializerHelper.stringToBinary(obj.fileName, buf);

            buf.writeInt(obj.targetPos);
            buf.writeInt(obj.hashes.size());
            if (obj.hashes.size() > 0) {
                int hashSize = obj.hashes.values().iterator().next().array().length;
                buf.writeInt(hashSize);

                for (Map.Entry<Integer, ByteBuffer> e : obj.hashes.entrySet()) {
                    buf.writeInt(e.getKey());
                    buf.writeBytes(e.getValue().array());
                }
            }

            buf.writeInt(obj.missingHashes.size());
            for (Integer missingHash : obj.missingHashes) {
                buf.writeInt(missingHash);
            }
        }

        @Override
        public Object fromBinary(ByteBuf buf, Optional<Object> hint) {
            Identifier eventId = (Identifier) Serializers.fromBinary(buf, hint);
            Identifier overlayId = (Identifier) Serializers.fromBinary(buf, hint);
            Result.Status status = (Result.Status) Serializers.lookupSerializer(Result.Status.class).fromBinary(buf, hint);
            String fileName = SerializerHelper.stringFromBinary(buf);

            int targetPos = buf.readInt();
            int nrHashes = buf.readInt();
            Map<Integer, ByteBuffer> hashes = new HashMap<>();
            if (nrHashes > 0) {
                int hashSize = buf.readInt();
                byte[] hash;
                for (int i = 0; i < nrHashes; i++) {
                    int hashId = buf.readInt();
                    hash = new byte[hashSize];
                    buf.readBytes(hash);
                    hashes.put(hashId, ByteBuffer.wrap(hash));
                }
            }

            Set<Integer> missingHashes = new HashSet<>();
            int nrMissingHashes = buf.readInt();
            for (int i = 0; i < nrMissingHashes; i++) {
                missingHashes.add(buf.readInt());
            }

            return new HashGet.Response(eventId, overlayId, status, fileName, targetPos, hashes, missingHashes);
        }
    }
}
