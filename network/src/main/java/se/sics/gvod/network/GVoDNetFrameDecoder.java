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
package se.sics.gvod.network;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import se.sics.gvod.common.msgs.MessageDecodingException;
import se.sics.gvod.net.BaseMsgFrameDecoder;
import se.sics.gvod.net.msgs.RewriteableMsg;
import se.sics.gvod.network.serializers.SerializationContext;
import se.sics.gvod.network.serializers.SerializationContextImpl;
import se.sics.gvod.network.serializers.net.base.SerializerAdapter;

/**
 * @author Alex Ormenisan <aaor@sics.se>
 */
public class GVoDNetFrameDecoder extends BaseMsgFrameDecoder {

    public static final byte GVOD_ONEWAY = (byte)0x90;
    public static final byte GVOD_REQUEST = (byte)0x91;
    public static final byte GVOD_RESPONSE = (byte)0x92;

    private static SerializationContext context = new SerializationContextImpl();

    public static void reset() {
        context = new SerializationContextImpl();
    }
    
    public static void register() {
        try {
            context.registerAlias(GVoDNetworkSettings.MsgAliases.GVOD_NET_ONEWAY.aliasedClass, GVoDNetworkSettings.MsgAliases.GVOD_NET_ONEWAY.toString(), GVOD_ONEWAY);
            context.registerAlias(GVoDNetworkSettings.MsgAliases.GVOD_NET_REQUEST.aliasedClass, GVoDNetworkSettings.MsgAliases.GVOD_NET_REQUEST.toString(), GVOD_REQUEST);
            context.registerAlias(GVoDNetworkSettings.MsgAliases.GVOD_NET_RESPONSE.aliasedClass, GVoDNetworkSettings.MsgAliases.GVOD_NET_RESPONSE.toString(), GVOD_RESPONSE);
        } catch (SerializationContext.DuplicateException ex) {
            throw new RuntimeException(ex);
        }

        GVoDNetworkSettings.setContext(context);
    }

    public GVoDNetFrameDecoder() {
        super();
    }

    @Override
    protected RewriteableMsg decodeMsg(ChannelHandlerContext ctx, ByteBuf buffer) throws MessageDecodingException {
        // See if msg is part of parent project, if yes then return it.
        // Otherwise decode the msg here.
        RewriteableMsg msg = super.decodeMsg(ctx, buffer);
        if (msg != null) {
            return msg;
        }

        switch (opKod) {
            case GVOD_ONEWAY:
                SerializerAdapter.OneWay oneWayS = new SerializerAdapter.OneWay();
                return oneWayS.decodeMsg(buffer);
            case GVOD_REQUEST:
                SerializerAdapter.Request requestS = new SerializerAdapter.Request();
                return requestS.decodeMsg(buffer);
            case GVOD_RESPONSE:
                SerializerAdapter.Response responseS = new SerializerAdapter.Response();
                return responseS.decodeMsg(buffer);
            default:
                return null;
        }
    }
}
