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
import io.netty.buffer.Unpooled;
import org.javatuples.Pair;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import se.sics.gvod.network.GVoDSerializerSetup;
import se.sics.kompics.network.netty.serialization.Serializer;
import se.sics.kompics.network.netty.serialization.Serializers;
import se.sics.ktoolbox.util.identifiable.basic.IntIdentifier;
import se.sics.ktoolbox.util.setup.BasicSerializerSetup;
import se.sics.nstream.test.NetDetailedStateRequestEC;
import se.sics.nstream.test.NetDetailedStateResponseEC;
import se.sics.nstream.util.BlockDetails;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class NetDetailedStateSerializerTest {

    @BeforeClass
    public static void setup() {
        int serializerId = 128;
        serializerId = BasicSerializerSetup.registerBasicSerializers(serializerId);
        serializerId = GVoDSerializerSetup.registerSerializers(serializerId);
    }

    @Test
    public void simpleReq() {
        Serializer serializer = Serializers.lookupSerializer(NetDetailedState.Request.class);
        NetDetailedStateRequestEC ec = new NetDetailedStateRequestEC();
        NetDetailedState.Request original, copy;
        ByteBuf serializedOriginal, serializedCopy;

        original = new NetDetailedState.Request(new IntIdentifier(1));
        serializedOriginal = Unpooled.buffer();
        serializer.toBinary(original, serializedOriginal);

        serializedCopy = Unpooled.buffer();
        serializedOriginal.getBytes(0, serializedCopy, serializedOriginal.readableBytes());
        copy = (NetDetailedState.Request) serializer.fromBinary(serializedCopy, Optional.absent());

        Assert.assertTrue(ec.isEqual(original, copy));
        Assert.assertEquals(0, serializedCopy.readableBytes());
    }

    @Test
    public void simpleResp() {
        Serializer serializer = Serializers.lookupSerializer(NetDetailedState.Response.class);
        NetDetailedStateResponseEC ec = new NetDetailedStateResponseEC();
        NetDetailedState.Response original, copy;
        ByteBuf serializedOriginal, serializedCopy;

        NetDetailedState.Request request = new NetDetailedState.Request(new IntIdentifier(1));
        original = request.success(Pair.with(2, new BlockDetails(5, 3, 2, 1)));
        serializedOriginal = Unpooled.buffer();
        serializer.toBinary(original, serializedOriginal);

        serializedCopy = Unpooled.buffer();
        serializedOriginal.getBytes(0, serializedCopy, serializedOriginal.readableBytes());
        copy = (NetDetailedState.Response) serializer.fromBinary(serializedCopy, Optional.absent());

        Assert.assertTrue(ec.isEqual(original, copy));
        Assert.assertEquals(0, serializedCopy.readableBytes());
    }
}
