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
package se.sics.nstream.storage.cache;

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
import se.sics.ktoolbox.util.identifiable.IdentifierRegistryV2;
import se.sics.ktoolbox.util.identifiable.overlay.OverlayId;
import se.sics.ktoolbox.util.identifiable.overlay.OverlayRegistryV2;
import se.sics.ktoolbox.util.setup.BasicSerializerSetup;
import se.sics.nstream.test.KHintSummaryEC;

/**
 *
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class KHintSummarySerializerTest {
    @BeforeClass
    public static void setup() {
        OverlayRegistryV2.initiate(new OverlayId.BasicTypeFactory((byte)0), new OverlayId.BasicTypeComparator());
        IdentifierRegistryV2.registerBaseDefaults1(64);
        
        int serializerId = 128;
        serializerId = BasicSerializerSetup.registerBasicSerializers(serializerId);
        serializerId = GVoDSerializerSetup.registerSerializers(serializerId);
    }
    
    @Test
    public void simpleTest() {
        Serializer serializer = Serializers.lookupSerializer(KHint.Summary.class);
        KHintSummaryEC eqc = new KHintSummaryEC();
        KHint.Summary original, copy;
        ByteBuf serializedOriginal, serializedCopy;
        
        Set<Integer> blocks = new TreeSet<>();
        blocks.add(0);
        blocks.add(1);
        original = new KHint.Summary(1l, blocks);
        serializedOriginal = Unpooled.buffer();
        serializer.toBinary(original, serializedOriginal);

        serializedCopy = Unpooled.buffer();
        serializedOriginal.getBytes(0, serializedCopy, serializedOriginal.readableBytes());
        copy = (KHint.Summary) serializer.fromBinary(serializedCopy, Optional.absent());
        
        Assert.assertTrue(eqc.isEqual(original, copy));
        Assert.assertEquals(0, serializedCopy.readableBytes());
    }
}
