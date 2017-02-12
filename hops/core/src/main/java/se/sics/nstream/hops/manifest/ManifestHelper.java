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
package se.sics.nstream.hops.manifest;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.Map;
import org.javatuples.Pair;
import se.sics.ktoolbox.util.identifiable.overlay.OverlayId;
import se.sics.ktoolbox.util.managedStore.core.util.HashUtil;
import se.sics.nstream.FileId;
import se.sics.nstream.TorrentIds;
import se.sics.nstream.transfer.MyTorrent;
import se.sics.nstream.util.BlockDetails;
import se.sics.nstream.util.BlockHelper;
import se.sics.nstream.util.FileBaseDetails;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class ManifestHelper {

    public static ManifestJSON getManifestJSON(byte[] jsonByte) {
        String jsonString;
        try {
            jsonString = new String(jsonByte, "UTF-8");
        } catch (UnsupportedEncodingException ex) {
            throw new RuntimeException(ex);
        }
        Gson gson = new GsonBuilder().create();
        ManifestJSON manifest = gson.fromJson(jsonString, ManifestJSON.class);
        return manifest;
    }
    
    public static MyTorrent.Manifest getManifest(ManifestJSON manifestJSON) {
        return MyTorrent.buildDefinition(getManifestByte(manifestJSON));
    }

    public static byte[] getManifestByte(ManifestJSON manifest) {
        Gson gson = new GsonBuilder().create();
        String jsonString = gson.toJson(manifest);
        byte[] jsonByte;
        try {
            jsonByte = jsonString.getBytes("UTF-8");
        } catch (UnsupportedEncodingException ex) {
            throw new RuntimeException(ex);
        }
        return jsonByte;
    }

    public static Pair<Map<String, FileId>, Map<FileId, FileBaseDetails>> getBaseDetails(OverlayId torrentId, ManifestJSON manifest, BlockDetails defaultBlock) {
        Map<String, FileId> nameToId = new HashMap<>();
        Map<FileId, FileBaseDetails> baseDetails = new HashMap<>();
        int fileNr = 1; //start from 1 - 0 is for the definition of the torrent
        for (FileInfoJSON fileInfo : manifest.getFileInfos()) {
            FileId fileId = TorrentIds.fileId(torrentId, fileNr++);
            nameToId.put(fileInfo.getFileName(), fileId);
            String hashAlg = HashUtil.getAlgName(HashUtil.SHA);
            Pair<Integer, BlockDetails> fileDetails = BlockHelper.getFileDetails(fileInfo.getLength(), defaultBlock);
            FileBaseDetails fbd = new FileBaseDetails(fileInfo.getLength(), fileDetails.getValue0(), defaultBlock, fileDetails.getValue1(), hashAlg);
            baseDetails.put(fileId, fbd);
        }
        return Pair.with(nameToId, baseDetails);
    }
}
