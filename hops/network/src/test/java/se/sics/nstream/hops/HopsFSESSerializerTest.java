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
package se.sics.nstream.hops;

import com.google.common.base.Optional;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import se.sics.gvod.network.GVoDSerializerSetup;
import se.sics.kompics.network.netty.serialization.Serializer;
import se.sics.kompics.network.netty.serialization.Serializers;
import se.sics.ktoolbox.util.setup.BasicSerializerSetup;
import se.sics.nstream.hops.test.HopsFESEqc;

/**
 *
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class HopsFSESSerializerTest {

    @BeforeClass
    public static void setup() {
        int serializerId = 128;
        serializerId = BasicSerializerSetup.registerBasicSerializers(serializerId);
        serializerId = GVoDSerializerSetup.registerSerializers(serializerId);
        serializerId = HopsNStreamSerializerSetup.registerSerializers(serializerId);
    }

    @Test
    public void testSimple() {
        Serializer serializer = Serializers.lookupSerializer(HopsFES.class);
        HopsFESEqc eqc = new HopsFESEqc();
        HopsFES original, copy;
        ByteBuf serializedOriginal, serializedCopy;
        
        original = new HopsFES("testing schema");
        
        serializedOriginal = Unpooled.buffer();
        serializer.toBinary(original, serializedOriginal);

        serializedCopy = Unpooled.buffer();
        serializedOriginal.getBytes(0, serializedCopy, serializedOriginal.readableBytes());
        copy = (HopsFES) serializer.fromBinary(serializedCopy, Optional.absent());
        
        Assert.assertTrue(eqc.isEqual(original, copy));
        Assert.assertEquals(0, serializedCopy.readableBytes());
    }
}
