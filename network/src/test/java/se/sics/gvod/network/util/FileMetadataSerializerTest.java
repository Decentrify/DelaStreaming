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
package se.sics.gvod.network.util;

import com.google.common.base.Optional;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import se.sics.gvod.common.util.FileMetadata;
import se.sics.gvod.network.GVoDSerializerSetup;
import se.sics.kompics.network.netty.serialization.Serializer;
import se.sics.kompics.network.netty.serialization.Serializers;
import se.sics.ktoolbox.util.managedStore.core.util.HashUtil;
import se.sics.ktoolbox.util.setup.BasicSerializerSetup;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class FileMetadataSerializerTest {

    @BeforeClass
    public static void setup() {
        int serializerId = 128;
        if (!BasicSerializerSetup.checkSetup()) {
            serializerId = BasicSerializerSetup.registerBasicSerializers(serializerId);
        }
        if (!GVoDSerializerSetup.checkSetup()) {
            serializerId = GVoDSerializerSetup.registerSerializers(serializerId);
        }
    }

    @Test
    public void testUpdate() {
        Serializer serializer = Serializers.lookupSerializer(FileMetadata.class);
        FileMetadata original, copy;
        ByteBuf buf, copyBuf;

        original = new FileMetadata("test.mp4", 10000, 1024, HashUtil.getAlgName(HashUtil.SHA), 200);
        //serializer
        buf = Unpooled.buffer();
        serializer.toBinary(original, buf);

        copyBuf = Unpooled.buffer();
        buf.getBytes(0, copyBuf, buf.readableBytes());
        copy = (FileMetadata) serializer.fromBinary(copyBuf, Optional.absent());

        Assert.assertEquals(original, copy);
        Assert.assertEquals(0, copyBuf.readableBytes());

        //generic
        buf = Unpooled.buffer();
        Serializers.toBinary(original, buf);

        copyBuf = Unpooled.buffer();
        buf.getBytes(0, copyBuf, buf.readableBytes());
        copy = (FileMetadata) Serializers.fromBinary(copyBuf, Optional.absent());

        Assert.assertEquals(original, copy);
        Assert.assertEquals(0, copyBuf.readableBytes());
    }
}
