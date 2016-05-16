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
import se.sics.ktoolbox.util.identifiable.Identifier;
import se.sics.ktoolbox.util.identifiable.basic.IntIdentifier;
import se.sics.ktoolbox.util.managedStore.core.util.FileInfo;
import se.sics.ktoolbox.util.managedStore.core.util.HashUtil;
import se.sics.ktoolbox.util.managedStore.core.util.Torrent;
import se.sics.ktoolbox.util.managedStore.core.util.TorrentInfo;
import se.sics.ktoolbox.util.setup.BasicSerializerSetup;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class TorrentGetSerializerTest {

    @BeforeClass
    public static void setup() {
        int serializerId = 128;
        serializerId = BasicSerializerSetup.registerBasicSerializers(serializerId);
        serializerId = GVoDSerializerSetup.registerSerializers(serializerId);
    }

    @Test
    public void testTorrentGetRequest() {
        Serializer serializer = Serializers.lookupSerializer(TorrentGet.Request.class);
        TorrentGet.Request original, copy;
        ByteBuf buf, copyBuf;

        original = new TorrentGet.Request();
        //serializer
        buf = Unpooled.buffer();
        serializer.toBinary(original, buf);

        copyBuf = Unpooled.buffer();
        buf.getBytes(0, copyBuf, buf.readableBytes());
        copy = (TorrentGet.Request) serializer.fromBinary(copyBuf, Optional.absent());

        Assert.assertEquals(original.eventId, copy.eventId);
        Assert.assertEquals(0, copyBuf.readableBytes());

        //generic
        buf = Unpooled.buffer();
        Serializers.toBinary(original, buf);

        copyBuf = Unpooled.buffer();
        buf.getBytes(0, copyBuf, buf.readableBytes());
        copy = (TorrentGet.Request) Serializers.fromBinary(copyBuf, Optional.absent());

        Assert.assertEquals(original.eventId, copy.eventId);
        Assert.assertEquals(0, copyBuf.readableBytes());
    }

    @Test
    public void testTorrentGetResponse() {
        Serializer serializer = Serializers.lookupSerializer(TorrentGet.Response.class);
        TorrentGet.Response original, copy;
        ByteBuf buf, copyBuf;

        Identifier overlayId = new IntIdentifier(10);
        FileInfo fileInfo = FileInfo.newFile("file1", 1024 * 1024);
        String shaHash = HashUtil.getAlgName(HashUtil.SHA);
        int hashFileSize = HashUtil.getHashSize(shaHash) * 10;
        TorrentInfo torrentInfo = new TorrentInfo(1024, 1000, shaHash, hashFileSize);
        Torrent torrent = new Torrent(overlayId, fileInfo, torrentInfo);
        TorrentGet.Request aux = new TorrentGet.Request();
        original = aux.success(torrent);
        //serializer
        buf = Unpooled.buffer();
        serializer.toBinary(original, buf);

        copyBuf = Unpooled.buffer();
        buf.getBytes(0, copyBuf, buf.readableBytes());
        copy = (TorrentGet.Response) serializer.fromBinary(copyBuf, Optional.absent());

        Assert.assertEquals(original.eventId, copy.eventId);
        Assert.assertEquals(original.torrent.overlayId, copy.torrent.overlayId);
        Assert.assertEquals(original.torrent.fileInfo.name, copy.torrent.fileInfo.name);
        Assert.assertEquals(original.torrent.fileInfo.size, copy.torrent.fileInfo.size);
        Assert.assertEquals(original.torrent.torrentInfo.hashAlg, copy.torrent.torrentInfo.hashAlg);
        Assert.assertEquals(original.torrent.torrentInfo.hashFileSize, copy.torrent.torrentInfo.hashFileSize);
        Assert.assertEquals(original.torrent.torrentInfo.pieceSize, copy.torrent.torrentInfo.pieceSize);
        Assert.assertEquals(original.torrent.torrentInfo.piecesPerBlock, copy.torrent.torrentInfo.piecesPerBlock);
        Assert.assertEquals(0, copyBuf.readableBytes());

        //generic
        buf = Unpooled.buffer();
        Serializers.toBinary(original, buf);

        copyBuf = Unpooled.buffer();
        buf.getBytes(0, copyBuf, buf.readableBytes());
        copy = (TorrentGet.Response) Serializers.fromBinary(copyBuf, Optional.absent());

        Assert.assertEquals(original.eventId, copy.eventId);
        Assert.assertEquals(original.torrent.overlayId, copy.torrent.overlayId);
        Assert.assertEquals(original.torrent.fileInfo.name, copy.torrent.fileInfo.name);
        Assert.assertEquals(original.torrent.fileInfo.size, copy.torrent.fileInfo.size);
        Assert.assertEquals(original.torrent.torrentInfo.hashAlg, copy.torrent.torrentInfo.hashAlg);
        Assert.assertEquals(original.torrent.torrentInfo.hashFileSize, copy.torrent.torrentInfo.hashFileSize);
        Assert.assertEquals(original.torrent.torrentInfo.pieceSize, copy.torrent.torrentInfo.pieceSize);
        Assert.assertEquals(original.torrent.torrentInfo.piecesPerBlock, copy.torrent.torrentInfo.piecesPerBlock);
        Assert.assertEquals(0, copyBuf.readableBytes());
    }
}