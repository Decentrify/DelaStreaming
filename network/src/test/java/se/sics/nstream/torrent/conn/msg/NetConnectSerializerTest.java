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
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import se.sics.gvod.network.GVoDSerializerSetup;
import se.sics.kompics.network.netty.serialization.Serializer;
import se.sics.kompics.network.netty.serialization.Serializers;
import se.sics.ktoolbox.util.identifiable.BasicIdentifiers;
import se.sics.ktoolbox.util.identifiable.IdentifierFactory;
import se.sics.ktoolbox.util.identifiable.IdentifierRegistryV2;
import se.sics.ktoolbox.util.identifiable.overlay.OverlayId;
import se.sics.ktoolbox.util.identifiable.overlay.OverlayIdFactory;
import se.sics.ktoolbox.util.identifiable.overlay.OverlayRegistry;
import se.sics.ktoolbox.util.setup.BasicSerializerSetup;
import se.sics.nstream.test.NetConnectRequestEC;
import se.sics.nstream.test.NetConnectResponseEC;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class NetConnectSerializerTest {
    private static OverlayIdFactory overlayIdFactory;
    @BeforeClass
    public static void setup() {
        IdentifierRegistryV2.registerBaseDefaults1(64);
        OverlayRegistry.initiate(new OverlayId.BasicTypeFactory((byte)0), new OverlayId.BasicTypeComparator());
        int serializerId = 128;
        serializerId = BasicSerializerSetup.registerBasicSerializers(serializerId);
        serializerId = GVoDSerializerSetup.registerSerializers(serializerId);
        
        byte ownerId = 1;
        IdentifierFactory baseIdFactory = IdentifierRegistryV2.instance(BasicIdentifiers.Values.OVERLAY, java.util.Optional.of(1234l));
        overlayIdFactory = new OverlayIdFactory(baseIdFactory, OverlayId.BasicTypes.OTHER, ownerId);
    }

    @Test
    public void simpleReq() {
        Serializer serializer = Serializers.lookupSerializer(NetConnect.Request.class);
        NetConnectRequestEC ec = new NetConnectRequestEC();
        NetConnect.Request original, copy;
        ByteBuf serializedOriginal, serializedCopy;

        
        original = new NetConnect.Request(overlayIdFactory.randomId());
        serializedOriginal = Unpooled.buffer();
        serializer.toBinary(original, serializedOriginal);

        serializedCopy = Unpooled.buffer();
        serializedOriginal.getBytes(0, serializedCopy, serializedOriginal.readableBytes());
        copy = (NetConnect.Request) serializer.fromBinary(serializedCopy, Optional.absent());

        Assert.assertTrue(ec.isEqual(original, copy));
        Assert.assertEquals(0, serializedCopy.readableBytes());
    }

    @Test
    public void simpleResp() {
        Serializer serializer = Serializers.lookupSerializer(NetConnect.Response.class);
        NetConnectResponseEC ec = new NetConnectResponseEC();
        NetConnect.Response original, copy;
        ByteBuf serializedOriginal, serializedCopy;

        NetConnect.Request request = new NetConnect.Request(overlayIdFactory.randomId());
        original = request.answer(true);
        serializedOriginal = Unpooled.buffer();
        serializer.toBinary(original, serializedOriginal);

        serializedCopy = Unpooled.buffer();
        serializedOriginal.getBytes(0, serializedCopy, serializedOriginal.readableBytes());
        copy = (NetConnect.Response) serializer.fromBinary(serializedCopy, Optional.absent());

        Assert.assertTrue(ec.isEqual(original, copy));
        Assert.assertEquals(0, serializedCopy.readableBytes());
    }
}
