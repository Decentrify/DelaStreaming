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
package se.sics.gvod.network.serializers.base;

import io.netty.buffer.ByteBuf;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;
import se.sics.gvod.common.msg.builder.GVoDMsgBuilder;
import se.sics.gvod.common.msg.GvodMsg;
import se.sics.gvod.common.msg.ReqStatus;
import se.sics.gvod.network.serializers.HierarhicSerializer;
import se.sics.gvod.network.serializers.SerializationContext;
import se.sics.gvod.network.serializers.Serializer;

/**
 * @author Alex Ormenisan <aaor@sics.se>
 */
public class GvodMsgSerializer {

    public static abstract class AbsBase<E extends GvodMsg.Base, F extends GVoDMsgBuilder.Base> implements HierarhicSerializer<E, F> {

        @Override
        public F decode(SerializationContext context, ByteBuf buf, F shellObj) throws SerializerException, SerializationContext.MissingException {
            UUID id = context.getSerializer(UUID.class).decode(context, buf);
            shellObj.setId(id);
            return shellObj;
        }

        @Override
        public ByteBuf encode(SerializationContext context, ByteBuf buf, E obj) throws SerializerException, SerializationContext.MissingException {
            context.getSerializer(UUID.class).encode(context, buf, obj.id);
            return buf;
        }

        @Override
        public int getSize(SerializationContext context, E obj) throws SerializerException, SerializationContext.MissingException {
            return context.getSerializer(UUID.class).getSize(context, obj.id);
        }
    }

    public static abstract class AbsRequest<E extends GvodMsg.Request, F extends GVoDMsgBuilder.Request> extends AbsBase<E, F> {
    }

    public static abstract class AbsResponse<E extends GvodMsg.Response, F extends GVoDMsgBuilder.Response> extends AbsBase<E, F> {

        @Override
        public F decode(SerializationContext context, ByteBuf buf, F shellObj) throws SerializerException, SerializationContext.MissingException {
            super.decode(context, buf, shellObj);
            ReqStatus status = context.getSerializer(ReqStatus.class).decode(context, buf);
            shellObj.setStatus(status);
            return shellObj;
        }

        @Override
        public ByteBuf encode(SerializationContext context, ByteBuf buf, E obj) throws SerializerException, SerializationContext.MissingException {
            super.encode(context, buf, obj);
            context.getSerializer(ReqStatus.class).encode(context, buf, obj.status);
            return buf;
        }

        @Override
        public int getSize(SerializationContext context, E obj) throws SerializerException, SerializationContext.MissingException {
            int size = super.getSize(context, obj);
            size += context.getSerializer(ReqStatus.class).getSize(context, obj.status);
            return size;
        }
    }

    public static abstract class AbsOneWay<E extends GvodMsg.OneWay, F extends GVoDMsgBuilder.OneWay> extends AbsBase<E, F> {
    }
}
