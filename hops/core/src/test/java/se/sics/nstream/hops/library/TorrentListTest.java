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
package se.sics.nstream.hops.library;

import java.io.File;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.BeforeClass;
import org.junit.Test;
import se.sics.ktoolbox.util.identifiable.BasicIdentifiers;
import se.sics.ktoolbox.util.identifiable.overlay.OverlayId;
import se.sics.ktoolbox.util.identifiable.overlay.OverlayRegistry;

/**
 *
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class TorrentListTest {

    private static final String dirPath = "src/test/resources/torrentList";
    private static final String torrentListPath = dirPath + File.separator + "torrent.list";

    @BeforeClass
    public static void setup() throws IOException {
        systemSetup();
        experimentSetup();
    }

    private static void systemSetup() {
        OverlayRegistry.initiate(new OverlayId.BasicTypeFactory((byte) 0), new OverlayId.BasicTypeComparator());
        BasicIdentifiers.registerDefaults(1234l);
    }

    private static void experimentSetup() throws IOException {
        Path testDir = FileSystems.getDefault().getPath(dirPath);
        if (!Files.exists(testDir)) {
            Files.createDirectory(testDir);
        }
        if (!Files.isDirectory(testDir)) {
            Files.delete(testDir);
            Files.createDirectory(testDir);
        }
        clean(testDir);
    }

    private static void clean(Path dirPath) throws IOException {
        try (DirectoryStream<Path> dirContents = Files.newDirectoryStream(dirPath)) {
            for (Path file : dirContents) {
                Files.delete(file);
            }
        }
    }

    @Test
    public void simpleTest() {
//        TorrentList tl = TorrentList.readTorrentList(torrentListPath);
//
//        HDFSEndpoint hdfsEndpoint = HDFSEndpoint.getBasic("user1", "bbc1.sics.se", 20000);
//        HDFSResource m1 = new HDFSResource("/experiment/upload1", "manifest");
//        Library.Torrent t1 = new Library.Torrent(hdfsEndpoint, m1, null);
//        HDFSResource m2 = new HDFSResource("/experiment/upload2", "manifest");
//        Library.Torrent t2 = new Library.Torrent(hdfsEndpoint, m2, null);
//
//        //TODO Alex - new kind of overlay
//        IdentifierFactory baseIdFactory2 = IdentifierRegistry.lookup(BasicIdentifiers.Values.OVERLAY.toString());
//        byte owner2 = 2;
//        OverlayIdFactory torrentIdFactory = new OverlayIdFactory(baseIdFactory2, OverlayId.BasicTypes.OTHER, owner2);

//        tl.write(TorrentSummary.getSummary(torrentIdFactory.randomId(), "torrent1", t1), hdfsEndpoint.hopsURL, hdfsEndpoint.user);
//        tl.write(TorrentSummary.getSummary(torrentIdFactory.randomId(), "torrent2", t2), hdfsEndpoint.hopsURL, hdfsEndpoint.user);
    }
}
