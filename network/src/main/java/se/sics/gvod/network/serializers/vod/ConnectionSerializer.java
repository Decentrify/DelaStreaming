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
package se.sics.gvod.network.serializers.vod;

import com.google.common.base.Optional;
import io.netty.buffer.ByteBuf;
import java.util.UUID;
import se.sics.gvod.common.msg.ReqStatus;
import se.sics.gvod.common.msg.vod.Connection;
import se.sics.gvod.common.util.VodDescriptor;
import se.sics.kompics.network.netty.serialization.Serializer;
import se.sics.kompics.network.netty.serialization.Serializers;

/**
 * @author Alex Ormenisan <aaor@sics.se>
 */
public class ConnectionSerializer {

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
            Connection.Request obj = (Connection.Request) o;
            Serializers.lookupSerializer(UUID.class).toBinary(obj.id, buf);
            Serializers.lookupSerializer(VodDescriptor.class).toBinary(obj.desc, buf);
        }

        @Override
        public Object fromBinary(ByteBuf buf, Optional<Object> hint) {
            UUID mId = (UUID) Serializers.lookupSerializer(UUID.class).fromBinary(buf, hint);
            VodDescriptor desc = (VodDescriptor) Serializers.lookupSerializer(VodDescriptor.class).fromBinary(buf, hint);
            return new Connection.Request(mId, desc);
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
            Connection.Response obj = (Connection.Response) o;
            Serializers.lookupSerializer(UUID.class).toBinary(obj.id, buf);
            Serializers.lookupSerializer(ReqStatus.class).toBinary(obj.status, buf);
            Serializers.lookupSerializer(VodDescriptor.class).toBinary(obj.desc, buf);
        }

        @Override
        public Object fromBinary(ByteBuf buf, Optional<Object> hint) {
            UUID mId = (UUID) Serializers.lookupSerializer(UUID.class).fromBinary(buf, hint);
            ReqStatus status = (ReqStatus) Serializers.lookupSerializer(ReqStatus.class).fromBinary(buf, hint);
            VodDescriptor desc = (VodDescriptor) Serializers.lookupSerializer(VodDescriptor.class).fromBinary(buf, hint);
            return new Connection.Response(mId, status, desc);
        }
    }

    public static final class Update implements Serializer {
        private final int id;
        
        public Update(int id) {
            this.id = id;
        }

        @Override
        public int identifier() {
            return id;
        }

        @Override
        public void toBinary(Object o, ByteBuf buf) {
            Connection.Update obj = (Connection.Update) o;
            Serializers.lookupSerializer(UUID.class).toBinary(obj.id, buf);
            Serializers.lookupSerializer(VodDescriptor.class).toBinary(obj.desc, buf);
            buf.writeBoolean(obj.downloadConnection);
        }

        @Override
        public Object fromBinary(ByteBuf buf, Optional<Object> hint) {
            UUID mId = (UUID) Serializers.lookupSerializer(UUID.class).fromBinary(buf, hint);
            VodDescriptor desc = (VodDescriptor) Serializers.lookupSerializer(VodDescriptor.class).fromBinary(buf, hint);
            boolean connectionType = buf.readBoolean();
            return new Connection.Update(mId, desc, connectionType);
        }
    }

    public static final class Close implements Serializer {
        private final int id;
        
        public Close(int id) {
            this.id = id;
        }
        
        @Override
        public int identifier() {
            return id;
        }

        @Override
        public void toBinary(Object o, ByteBuf buf) {
            Connection.Close obj = (Connection.Close) o;
            Serializers.lookupSerializer(UUID.class).toBinary(obj.id, buf);
            buf.writeBoolean(obj.downloadConnection);
        }

        @Override
        public Object fromBinary(ByteBuf buf, Optional<Object> hint) {
             UUID mId = (UUID) Serializers.lookupSerializer(UUID.class).fromBinary(buf, hint);
             boolean downloadConnection = buf.readBoolean();
             return new Connection.Close(mId, downloadConnection);
        }
    }
}