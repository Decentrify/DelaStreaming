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
package se.sics.nstream.util;

import se.sics.nstream.test.FileBaseDetailsEqc;
import com.google.common.base.Optional;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import se.sics.gvod.network.GVoDSerializerSetup;
import se.sics.kompics.network.netty.serialization.Serializer;
import se.sics.kompics.network.netty.serialization.Serializers;
import se.sics.ktoolbox.util.managedStore.core.util.HashUtil;
import se.sics.ktoolbox.util.setup.BasicSerializerSetup;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class FileBaseDetailsSerializerTest {

    @BeforeClass
    public static void setup() {
        int serializerId = 128;
        serializerId = BasicSerializerSetup.registerBasicSerializers(serializerId);
        serializerId = GVoDSerializerSetup.registerSerializers(serializerId);
    }
    
    @Test
    public void simpleTest() {
        Serializer serializer = Serializers.lookupSerializer(FileBaseDetails.class);
        FileBaseDetailsEqc eqc = new FileBaseDetailsEqc();
        FileBaseDetails original, copy;
        ByteBuf serializedOriginal, serializedCopy;
        
        BlockDetails defaultBlock = new BlockDetails(1,1,1,1);
        BlockDetails lastBlock = new BlockDetails(2,2,2,2);
        String hashAlg = HashUtil.getAlgName(HashUtil.SHA);
        original = new FileBaseDetails(1l, 1, defaultBlock, lastBlock, hashAlg);
        serializedOriginal = Unpooled.buffer();
        serializer.toBinary(original, serializedOriginal);

        serializedCopy = Unpooled.buffer();
        serializedOriginal.getBytes(0, serializedCopy, serializedOriginal.readableBytes());
        copy = (FileBaseDetails) serializer.fromBinary(serializedCopy, Optional.absent());
        
        Assert.assertTrue(eqc.isEqual(original, copy));
        Assert.assertEquals(0, serializedCopy.readableBytes());
    }
}
