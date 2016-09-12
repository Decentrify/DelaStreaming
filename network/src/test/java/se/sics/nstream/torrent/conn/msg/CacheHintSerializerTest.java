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
import java.util.Set;
import java.util.TreeSet;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import se.sics.gvod.network.GVoDSerializerSetup;
import se.sics.kompics.network.netty.serialization.Serializer;
import se.sics.kompics.network.netty.serialization.Serializers;
import se.sics.ktoolbox.util.identifiable.basic.IntIdentifier;
import se.sics.ktoolbox.util.setup.BasicSerializerSetup;
import se.sics.nstream.storage.cache.KHint;
import se.sics.nstream.test.CacheHintRequestEC;
import se.sics.nstream.test.CacheHintResponseEC;
import se.sics.nstream.torrent.FileIdentifier;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class CacheHintSerializerTest {
    @BeforeClass
    public static void setup() {
        int serializerId = 128;
        serializerId = BasicSerializerSetup.registerBasicSerializers(serializerId);
        serializerId = GVoDSerializerSetup.registerSerializers(serializerId);
    }
    
    @Test
    public void simpleReq() {
        Serializer serializer = Serializers.lookupSerializer(CacheHint.Request.class);
        CacheHintRequestEC ec = new CacheHintRequestEC();
        CacheHint.Request original, copy;
        ByteBuf serializedOriginal, serializedCopy;
        
        Set<Integer> blocks = new TreeSet<>();
        blocks.add(0);
        blocks.add(1);
        KHint.Summary cacheHint = new KHint.Summary(1l, blocks);
        original = new CacheHint.Request(new FileIdentifier(new IntIdentifier(1), 2), cacheHint);
        serializedOriginal = Unpooled.buffer();
        serializer.toBinary(original, serializedOriginal);

        serializedCopy = Unpooled.buffer();
        serializedOriginal.getBytes(0, serializedCopy, serializedOriginal.readableBytes());
        copy = (CacheHint.Request) serializer.fromBinary(serializedCopy, Optional.absent());
        
        Assert.assertTrue(ec.isEqual(original, copy));
        Assert.assertEquals(0, serializedCopy.readableBytes());
    }
    
    @Test
    public void simpleResp() {
        Serializer serializer = Serializers.lookupSerializer(CacheHint.Response.class);
        CacheHintResponseEC ec = new CacheHintResponseEC();
        CacheHint.Response original, copy;
        ByteBuf serializedOriginal, serializedCopy;
        
        Set<Integer> blocks = new TreeSet<>();
        blocks.add(0);
        blocks.add(1);
        KHint.Summary cacheHint = new KHint.Summary(1l, blocks);
        CacheHint.Request request = new CacheHint.Request(new FileIdentifier(new IntIdentifier(1), 2), cacheHint);
        original = request.success();
        serializedOriginal = Unpooled.buffer();
        serializer.toBinary(original, serializedOriginal);

        serializedCopy = Unpooled.buffer();
        serializedOriginal.getBytes(0, serializedCopy, serializedOriginal.readableBytes());
        copy = (CacheHint.Response) serializer.fromBinary(serializedCopy, Optional.absent());
        
        Assert.assertTrue(ec.isEqual(original, copy));
        Assert.assertEquals(0, serializedCopy.readableBytes());
    }
}
