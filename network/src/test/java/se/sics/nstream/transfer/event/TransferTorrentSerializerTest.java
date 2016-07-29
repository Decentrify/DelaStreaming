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
package se.sics.nstream.transfer.event;

import com.google.common.base.Optional;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import se.sics.gvod.network.GVoDSerializerSetup;
import se.sics.kompics.network.netty.serialization.Serializer;
import se.sics.kompics.network.netty.serialization.Serializers;
import se.sics.ktoolbox.util.identifiable.basic.IntIdentifier;
import se.sics.ktoolbox.util.identifiable.basic.UUIDIdentifier;
import se.sics.ktoolbox.util.result.Result;
import se.sics.ktoolbox.util.setup.BasicSerializerSetup;
import se.sics.nstream.test.TransferTorrentEqc;

/**
 *
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class TransferTorrentSerializerTest {

    @BeforeClass
    public static void setup() {
        int serializerId = 128;
        serializerId = BasicSerializerSetup.registerBasicSerializers(serializerId);
        serializerId = GVoDSerializerSetup.registerSerializers(serializerId);
    }
    
    @Test
    public void simpleReqTest() {
        Serializer serializer = Serializers.lookupSerializer(TransferTorrent.Request.class);
        TransferTorrentEqc.Request eqc = new TransferTorrentEqc.Request();
        TransferTorrent.Request original, copy;
        ByteBuf serializedOriginal, serializedCopy;
        
        original = new TransferTorrent.Request(new IntIdentifier(10));
        serializedOriginal = Unpooled.buffer();
        serializer.toBinary(original, serializedOriginal);

        serializedCopy = Unpooled.buffer();
        serializedOriginal.getBytes(0, serializedCopy, serializedOriginal.readableBytes());
        copy = (TransferTorrent.Request) serializer.fromBinary(serializedCopy, Optional.absent());
        
        Assert.assertTrue(eqc.isEqual(original, copy));
        Assert.assertEquals(0, serializedCopy.readableBytes());
    }
    
    @Test
    public void simpleRespTest() {
        Serializer serializer = Serializers.lookupSerializer(TransferTorrent.Response.class);
        TransferTorrentEqc.Response eqc = new TransferTorrentEqc.Response();
        TransferTorrent.Response original, copy;
        ByteBuf serializedOriginal, serializedCopy;
        
        original = new TransferTorrent.Response(UUIDIdentifier.randomId(), new IntIdentifier(10), Result.Status.SUCCESS, new byte[]{1,2,3});
        serializedOriginal = Unpooled.buffer();
        serializer.toBinary(original, serializedOriginal);

        serializedCopy = Unpooled.buffer();
        serializedOriginal.getBytes(0, serializedCopy.ensureWritable(serializedOriginal.readableBytes()), serializedOriginal.readableBytes());
        copy = (TransferTorrent.Response) serializer.fromBinary(serializedCopy, Optional.absent());
        
        Assert.assertTrue(eqc.isEqual(original, copy));
        Assert.assertEquals(0, serializedCopy.readableBytes());
    }
}
