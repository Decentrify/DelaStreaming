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

package se.sics.gvod.network.nettyadapter;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.UUID;
import org.junit.Assert;
import org.junit.Test;
import se.sics.gvod.address.Address;
import se.sics.gvod.common.msg.impl.AddOverlayMsg;
import se.sics.gvod.common.msg.impl.BootstrapGlobalMsg;
import se.sics.gvod.common.msgs.MessageEncodingException;
import se.sics.gvod.common.util.FileMetadata;
import se.sics.gvod.net.VodAddress;
import se.sics.gvod.network.GVoDAdapterFactory;
import se.sics.gvod.network.GVoDNetFrameDecoder;
import se.sics.gvod.network.gvodadapter.GVoDAdapter;
import se.sics.gvod.network.nettymsg.GvodNetMsg;

/**
 *
 * @author Alex Ormenisan <aaor@sics.se>
 */
public class NettyAdapterTest {
    @Test
    public void testRequest() throws UnknownHostException, MessageEncodingException, Exception {
        AddOverlayMsg.Request req = new AddOverlayMsg.Request(UUID.randomUUID(), 1, new FileMetadata(10000, 1024));
        VodAddress src = new VodAddress(new Address(InetAddress.getLocalHost(), 1234, 1), -1);
        VodAddress dest = new VodAddress(new Address(InetAddress.getLocalHost(), 1234, 2), -1);
        GvodNetMsg.Request<AddOverlayMsg.Request> expected = new GvodNetMsg.Request<AddOverlayMsg.Request>(src, dest, req);
        expected.setTimeoutId(se.sics.gvod.timer.UUID.nextUUID());
        int expectedSize = expected.getSize();
        ByteBuf buf = expected.toByteArray();
        ByteBuf newBuf = Unpooled.wrappedBuffer(buf.array());
        GVoDNetFrameDecoder decoder = new GVoDNetFrameDecoder();
        GvodNetMsg.Request<AddOverlayMsg.Request> decoded = (GvodNetMsg.Request<AddOverlayMsg.Request>)decoder.parse(newBuf);
        Assert.assertEquals(expected, decoded);
        GVoDAdapter<AddOverlayMsg.Request> adapter = GVoDAdapterFactory.getAdapter(GVoDAdapterFactory.ADD_OVERLAY_REQUEST);
        Assert.assertEquals(adapter.getEncodedSize(expected.payload), adapter.getEncodedSize(decoded.payload));
//        Assert.assertEquals(expectedSize, buf.readableBytes());
    }
    
    @Test
    public void testRequest2() throws UnknownHostException, MessageEncodingException, Exception {
        BootstrapGlobalMsg.Request req = new BootstrapGlobalMsg.Request(UUID.randomUUID());
        VodAddress src = new VodAddress(new Address(InetAddress.getLocalHost(), 1234, 1), -1);
        VodAddress dest = new VodAddress(new Address(InetAddress.getLocalHost(), 1234, 2), -1);
        GvodNetMsg.Request<BootstrapGlobalMsg.Request> expected = new GvodNetMsg.Request<BootstrapGlobalMsg.Request>(src, dest, req);
        expected.setTimeoutId(se.sics.gvod.timer.UUID.nextUUID());
        int expectedSize = expected.getSize();
        ByteBuf buf = expected.toByteArray();
        ByteBuf newBuf = Unpooled.wrappedBuffer(buf.array());
        GVoDNetFrameDecoder decoder = new GVoDNetFrameDecoder();
        GvodNetMsg.Request<BootstrapGlobalMsg.Request> decoded = (GvodNetMsg.Request<BootstrapGlobalMsg.Request>)decoder.parse(newBuf);
        Assert.assertEquals(expected, decoded);
        GVoDAdapter<BootstrapGlobalMsg.Request> adapter = GVoDAdapterFactory.getAdapter(GVoDAdapterFactory.BOOTSTRAP_GLOBAL_REQUEST);
        Assert.assertEquals(adapter.getEncodedSize(expected.payload), adapter.getEncodedSize(decoded.payload));
//        Assert.assertEquals(expectedSize, buf.readableBytes());
    }
}
