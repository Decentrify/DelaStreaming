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
package se.sics.gvod.network.vod;

import com.google.common.base.Optional;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import se.sics.gvod.common.event.ReqStatus;
import se.sics.gvod.common.event.vod.Connection;
import se.sics.gvod.common.util.VodDescriptor;
import se.sics.gvod.network.GVoDSerializerSetup;
import se.sics.kompics.network.netty.serialization.Serializer;
import se.sics.kompics.network.netty.serialization.Serializers;
import se.sics.ktoolbox.util.identifiable.basic.UUIDIdentifier;
import se.sics.ktoolbox.util.setup.BasicSerializerSetup;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class ConnectionSerializerTest {

    @BeforeClass
    public static void setup() {
        int serializerId = 128;
        serializerId = BasicSerializerSetup.registerBasicSerializers(serializerId);
        serializerId = GVoDSerializerSetup.registerSerializers(serializerId);
    }

    @Test
    public void testRequest() {
        Serializer serializer = Serializers.lookupSerializer(Connection.Request.class);
        Connection.Request original, copy;
        ByteBuf buf, copyBuf;

        original = new Connection.Request(UUIDIdentifier.randomId(), new VodDescriptor(100));
        //serializer
        buf = Unpooled.buffer();
        serializer.toBinary(original, buf);

        copyBuf = Unpooled.buffer();
        buf.getBytes(0, copyBuf, buf.readableBytes());
        copy = (Connection.Request) serializer.fromBinary(copyBuf, Optional.absent());

        Assert.assertEquals(original.eventId, copy.eventId);
        Assert.assertEquals(original.overlayId, copy.overlayId);
        Assert.assertEquals(original.desc, copy.desc);
        Assert.assertEquals(0, copyBuf.readableBytes());

        //generic
        buf = Unpooled.buffer();
        Serializers.toBinary(original, buf);

        copyBuf = Unpooled.buffer();
        buf.getBytes(0, copyBuf, buf.readableBytes());
        copy = (Connection.Request) Serializers.fromBinary(copyBuf, Optional.absent());

        Assert.assertEquals(original.eventId, copy.eventId);
        Assert.assertEquals(original.overlayId, copy.overlayId);
        Assert.assertEquals(original.desc, copy.desc);
        Assert.assertEquals(0, copyBuf.readableBytes());
    }

    @Test
    public void testResponse() {
        Serializer serializer = Serializers.lookupSerializer(Connection.Response.class);
        Connection.Response original, copy;
        ByteBuf buf, copyBuf;

        original = new Connection.Response(UUIDIdentifier.randomId(), UUIDIdentifier.randomId(),
                ReqStatus.SUCCESS, new VodDescriptor(100));
        //serializer
        buf = Unpooled.buffer();
        serializer.toBinary(original, buf);

        copyBuf = Unpooled.buffer();
        buf.getBytes(0, copyBuf, buf.readableBytes());
        copy = (Connection.Response) serializer.fromBinary(copyBuf, Optional.absent());

        Assert.assertEquals(original.eventId, copy.eventId);
        Assert.assertEquals(original.overlayId, copy.overlayId);
        Assert.assertEquals(original.status, copy.status);
        Assert.assertEquals(original.desc, copy.desc);
        Assert.assertEquals(0, copyBuf.readableBytes());

        //generic
        buf = Unpooled.buffer();
        Serializers.toBinary(original, buf);

        copyBuf = Unpooled.buffer();
        buf.getBytes(0, copyBuf, buf.readableBytes());
        copy = (Connection.Response) Serializers.fromBinary(copyBuf, Optional.absent());

        Assert.assertEquals(original.eventId, copy.eventId);
        Assert.assertEquals(original.overlayId, copy.overlayId);
        Assert.assertEquals(original.status, copy.status);
        Assert.assertEquals(original.desc, copy.desc);
        Assert.assertEquals(0, copyBuf.readableBytes());
    }

    @Test
    public void testUpdate() {
        Serializer serializer = Serializers.lookupSerializer(Connection.Update.class);
        Connection.Update original, copy;
        ByteBuf buf, copyBuf;

        original = new Connection.Update(UUIDIdentifier.randomId(), new VodDescriptor(100), true);
        //serializer
        buf = Unpooled.buffer();
        serializer.toBinary(original, buf);

        copyBuf = Unpooled.buffer();
        buf.getBytes(0, copyBuf, buf.readableBytes());
        copy = (Connection.Update) serializer.fromBinary(copyBuf, Optional.absent());

        Assert.assertEquals(original.eventId, copy.eventId);
        Assert.assertEquals(original.overlayId, copy.overlayId);
        Assert.assertEquals(original.desc, copy.desc);
        Assert.assertEquals(original.downloadConnection, copy.downloadConnection);
        Assert.assertEquals(0, copyBuf.readableBytes());

        //generic
        buf = Unpooled.buffer();
        Serializers.toBinary(original, buf);

        copyBuf = Unpooled.buffer();
        buf.getBytes(0, copyBuf, buf.readableBytes());
        copy = (Connection.Update) Serializers.fromBinary(copyBuf, Optional.absent());

        Assert.assertEquals(original.eventId, copy.eventId);
        Assert.assertEquals(original.overlayId, copy.overlayId);
        Assert.assertEquals(original.desc, copy.desc);
        Assert.assertEquals(original.downloadConnection, copy.downloadConnection);
        Assert.assertEquals(0, copyBuf.readableBytes());
    }

    @Test
    public void testClose() {
        Serializer serializer = Serializers.lookupSerializer(Connection.Close.class);
        Connection.Close original, copy;
        ByteBuf buf, copyBuf;

        original = new Connection.Close(UUIDIdentifier.randomId(), true);
        //serializer
        buf = Unpooled.buffer();
        serializer.toBinary(original, buf);

        copyBuf = Unpooled.buffer();
        buf.getBytes(0, copyBuf, buf.readableBytes());
        copy = (Connection.Close) serializer.fromBinary(copyBuf, Optional.absent());

        Assert.assertEquals(original.eventId, copy.eventId);
        Assert.assertEquals(original.overlayId, copy.overlayId);
        Assert.assertEquals(original.downloadConnection, copy.downloadConnection);
        Assert.assertEquals(0, copyBuf.readableBytes());

        //generic
        buf = Unpooled.buffer();
        Serializers.toBinary(original, buf);

        copyBuf = Unpooled.buffer();
        buf.getBytes(0, copyBuf, buf.readableBytes());
        copy = (Connection.Close) Serializers.fromBinary(copyBuf, Optional.absent());

        Assert.assertEquals(original.eventId, copy.eventId);
        Assert.assertEquals(original.overlayId, copy.overlayId);
        Assert.assertEquals(original.downloadConnection, copy.downloadConnection);
        Assert.assertEquals(0, copyBuf.readableBytes());
    }
}
