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
package se.sics.gvod.stream.torrent.event;

import com.google.common.base.Optional;
import com.google.common.collect.Sets;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import se.sics.gvod.network.GVoDSerializerSetup;
import se.sics.kompics.network.netty.serialization.Serializer;
import se.sics.kompics.network.netty.serialization.Serializers;
import se.sics.ktoolbox.util.identifiable.basic.IntIdentifier;
import se.sics.ktoolbox.util.setup.BasicSerializerSetup;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class DownloadSerializerTest {

    @BeforeClass
    public static void setup() {
        int serializerId = 128;
        serializerId = BasicSerializerSetup.registerBasicSerializers(serializerId);
        serializerId = GVoDSerializerSetup.registerSerializers(serializerId);
    }

    @Test
    public void testDataRequest() {
        Serializer serializer = Serializers.lookupSerializer(Download.DataRequest.class);
        Download.DataRequest original, copy;
        ByteBuf buf, copyBuf;

        original = new Download.DataRequest(new IntIdentifier(10), 10);
        //serializer
        buf = Unpooled.buffer();
        serializer.toBinary(original, buf);

        copyBuf = Unpooled.buffer();
        buf.getBytes(0, copyBuf, buf.readableBytes());
        copy = (Download.DataRequest) serializer.fromBinary(copyBuf, Optional.absent());

        Assert.assertEquals(original.eventId, copy.eventId);
        Assert.assertEquals(original.overlayId, copy.overlayId);
        Assert.assertEquals(original.pieceId, copy.pieceId);
        Assert.assertEquals(0, copyBuf.readableBytes());

        //generic
        buf = Unpooled.buffer();
        Serializers.toBinary(original, buf);

        copyBuf = Unpooled.buffer();
        buf.getBytes(0, copyBuf, buf.readableBytes());
        copy = (Download.DataRequest) Serializers.fromBinary(copyBuf, Optional.absent());

        Assert.assertEquals(original.eventId, copy.eventId);
        Assert.assertEquals(original.overlayId, copy.overlayId);
        Assert.assertEquals(original.pieceId, copy.pieceId);
        Assert.assertEquals(0, copyBuf.readableBytes());
    }

    @Test
    public void testResponse() {
        Serializer serializer = Serializers.lookupSerializer(Download.DataResponse.class);
        Download.DataResponse original, copy;
        ByteBuf buf, copyBuf;

        Download.DataRequest aux = new Download.DataRequest(new IntIdentifier(10), 10);
        original = aux.success(ByteBuffer.wrap(new byte[]{1, 2, 3, 4, 5}));
        original.setSendingTime(1024*1024);
        //serializer
        buf = Unpooled.buffer();
        serializer.toBinary(original, buf);

        copyBuf = Unpooled.buffer();
        buf.getBytes(0, copyBuf, buf.readableBytes());
        copy = (Download.DataResponse) serializer.fromBinary(copyBuf, Optional.absent());

        Assert.assertEquals(original.eventId, copy.eventId);
        Assert.assertEquals(original.overlayId, copy.overlayId);
        Assert.assertEquals(original.status, copy.status);
        Assert.assertEquals(original.pieceId, copy.pieceId);
        Assert.assertEquals(original.piece, copy.piece);
        Assert.assertEquals(original.sendingTime, copy.sendingTime);
        Assert.assertEquals(0, copyBuf.readableBytes());

        //generic
        buf = Unpooled.buffer();
        Serializers.toBinary(original, buf);

        copyBuf = Unpooled.buffer();
        buf.getBytes(0, copyBuf, buf.readableBytes());
        copy = (Download.DataResponse) Serializers.fromBinary(copyBuf, Optional.absent());

        Assert.assertEquals(original.eventId, copy.eventId);
        Assert.assertEquals(original.overlayId, copy.overlayId);
        Assert.assertEquals(original.status, copy.status);
        Assert.assertEquals(original.pieceId, copy.pieceId);
        Assert.assertEquals(original.piece, copy.piece);
        Assert.assertEquals(original.sendingTime, copy.sendingTime);
        Assert.assertEquals(0, copyBuf.readableBytes());
    }

    @Test
    public void testHashRequest() {
        Serializer serializer = Serializers.lookupSerializer(Download.HashRequest.class);
        Download.HashRequest original, copy;
        ByteBuf buf, copyBuf;

        Set<Integer> hashes = new HashSet<>();
        hashes.add(10);
        hashes.add(11);
        original = new Download.HashRequest(new IntIdentifier(10), 10, hashes);
        //serializer
        buf = Unpooled.buffer();
        serializer.toBinary(original, buf);

        copyBuf = Unpooled.buffer();
        buf.getBytes(0, copyBuf, buf.readableBytes());
        copy = (Download.HashRequest) serializer.fromBinary(copyBuf, Optional.absent());

        Assert.assertEquals(original.eventId, copy.eventId);
        Assert.assertEquals(original.overlayId, copy.overlayId);
        Assert.assertEquals(original.targetPos, copy.targetPos);
        Assert.assertTrue(Sets.symmetricDifference(original.hashes, copy.hashes).isEmpty());
        Assert.assertEquals(0, copyBuf.readableBytes());

        //generic
        buf = Unpooled.buffer();
        Serializers.toBinary(original, buf);

        copyBuf = Unpooled.buffer();
        buf.getBytes(0, copyBuf, buf.readableBytes());
        copy = (Download.HashRequest) Serializers.fromBinary(copyBuf, Optional.absent());

        Assert.assertEquals(original.eventId, copy.eventId);
        Assert.assertEquals(original.overlayId, copy.overlayId);
        Assert.assertEquals(original.targetPos, copy.targetPos);
        Assert.assertTrue(Sets.symmetricDifference(original.hashes, copy.hashes).isEmpty());
        Assert.assertEquals(0, copyBuf.readableBytes());
    }

    @Test
    public void testHashResponse() {
        Serializer serializer = Serializers.lookupSerializer(Download.HashResponse.class);
        Download.HashResponse original, copy;
        ByteBuf buf, copyBuf;

        Set<Integer> reqHashes = new HashSet<>();
        reqHashes.add(10);
        reqHashes.add(11);
        reqHashes.add(12);
        reqHashes.add(13);
        Download.HashRequest aux = new Download.HashRequest(new IntIdentifier(10), 10, reqHashes);
        //serializer

        Set<Integer> missingHashes = new HashSet<>();
        missingHashes.add(10);
        missingHashes.add(12);
        Map<Integer, ByteBuffer> hashes = new HashMap<>();
        hashes.put(11, ByteBuffer.wrap(new byte[]{1, 2, 3, 4}));
        hashes.put(13, ByteBuffer.wrap(new byte[]{1, 1, 1, 1}));
        original = aux.success(hashes, missingHashes);
        original.setSendingTime(1024*1024);
        //serializer
        buf = Unpooled.buffer();
        serializer.toBinary(original, buf);

        copyBuf = Unpooled.buffer();
        buf.getBytes(0, copyBuf, buf.readableBytes());
        copy = (Download.HashResponse) serializer.fromBinary(copyBuf, Optional.absent());

        Assert.assertEquals(original.eventId, copy.eventId);
        Assert.assertEquals(original.overlayId, copy.overlayId);
        Assert.assertEquals(original.targetPos, copy.targetPos);
        Assert.assertEquals(original.status, copy.status);
        Assert.assertTrue(Sets.symmetricDifference(original.hashes.entrySet(), copy.hashes.entrySet()).isEmpty());
        Assert.assertTrue(Sets.symmetricDifference(original.missingHashes, copy.missingHashes).isEmpty());
        Assert.assertEquals(original.sendingTime, copy.sendingTime);
        Assert.assertEquals(0, copyBuf.readableBytes());

        //generic
        buf = Unpooled.buffer();
        Serializers.toBinary(original, buf);

        copyBuf = Unpooled.buffer();
        buf.getBytes(0, copyBuf, buf.readableBytes());
        copy = (Download.HashResponse) Serializers.fromBinary(copyBuf, Optional.absent());

        Assert.assertEquals(original.eventId, copy.eventId);
        Assert.assertEquals(original.overlayId, copy.overlayId);
        Assert.assertEquals(original.targetPos, copy.targetPos);
        Assert.assertEquals(original.status, copy.status);
        Assert.assertTrue(Sets.symmetricDifference(original.hashes.entrySet(), copy.hashes.entrySet()).isEmpty());
        Assert.assertTrue(Sets.symmetricDifference(original.missingHashes, copy.missingHashes).isEmpty());
        Assert.assertEquals(original.sendingTime, copy.sendingTime);
        Assert.assertEquals(0, copyBuf.readableBytes());
    }
}
