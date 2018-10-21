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
package se.sics.nstream.hops.storage.hops;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonSyntaxException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import org.javatuples.Pair;
import se.sics.ktoolbox.util.identifiable.overlay.OverlayId;
import se.sics.ktoolbox.util.managedStore.core.util.HashUtil;
import se.sics.ktoolbox.util.trysf.Try;
import se.sics.ktoolbox.util.trysf.TryHelper;
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

  public static ManifestJSON dummyManifest() {
    ManifestJSON manifest = new ManifestJSON();
    manifest.setCreatorDate("Mon Aug 27 17:35:13 CEST 2018");
    manifest.setCreatorEmail("dummy@somemail.com");
    manifest.setDatasetDescription("this is not a description");
    manifest.setDatasetName("dataset");
    manifest.setKafkaSupport(false);
    List<FileInfoJSON> fileInfos = new ArrayList<>();
    FileInfoJSON file1Info = new FileInfoJSON();
    file1Info.setFileName("file1");
    file1Info.setLength(1024);
    file1Info.setSchema("");
    fileInfos.add(file1Info);
    manifest.setFileInfos(fileInfos);
    List<String> metadata = new ArrayList();
    metadata.add("metadata");
    manifest.setMetaDataJsons(metadata);
    return manifest;
  }

  public static BiFunction<byte[], Throwable, Try<ManifestJSON>> tryGetManifestJSON() {
    return TryHelper.tryFSucc1((byte[] jsonBytes) -> {
      return tryGetManifestJSON(jsonBytes);
    });
  }

  public static Try<ManifestJSON> tryGetManifestJSON(byte[] jsonBytes) {
    Gson gson = new GsonBuilder().create();
    try {
      String jsonString = new String(jsonBytes, "UTF-8");
      ManifestJSON manifest = gson.fromJson(jsonString, ManifestJSON.class);
      return new Try.Success(manifest);
    } catch (UnsupportedEncodingException | JsonSyntaxException ex) {
      return new Try.Failure(ex);
    }
  }

  public static ManifestJSON getManifestJSON(byte[] jsonByte) {
    try {
      ManifestJSON manifest = tryGetManifestJSON(jsonByte).checkedGet();
      return manifest;
    } catch (Throwable ex) {
      throw new RuntimeException(ex);
    }
  }

  public static MyTorrent.Manifest getManifest(ManifestJSON manifestJSON) {
    return MyTorrent.buildDefinition(getManifestByte(manifestJSON));
  }

  public static Try<byte[]> tryGetManifestBytes(ManifestJSON manifest) {
    Gson gson = new GsonBuilder().create();
    String jsonString = gson.toJson(manifest);
    byte[] jsonBytes;
    try {
      jsonBytes = jsonString.getBytes("UTF-8");
      return new Try.Success(jsonBytes);
    } catch (UnsupportedEncodingException ex) {
      return new Try.Failure(ex);
    }
  }
  public static byte[] getManifestByte(ManifestJSON manifest) {
    try {
      byte[] jsonBytes = tryGetManifestBytes(manifest).checkedGet();
      return jsonBytes;
    } catch (Throwable ex) {
      throw new RuntimeException(ex);
    }
  }

  public static Pair<Map<String, FileId>, Map<FileId, FileBaseDetails>> getBaseDetails(OverlayId torrentId,
    ManifestJSON manifest, BlockDetails defaultBlock) {
    Map<String, FileId> nameToId = new HashMap<>();
    Map<FileId, FileBaseDetails> baseDetails = new HashMap<>();
    int fileNr = 1; //start from 1 - 0 is for the definition of the torrent
    for (FileInfoJSON fileInfo : manifest.getFileInfos()) {
      FileId fileId = TorrentIds.fileId(torrentId, fileNr++);
      nameToId.put(fileInfo.getFileName(), fileId);
      String hashAlg = HashUtil.getAlgName(HashUtil.SHA);
      Pair<Integer, BlockDetails> fileDetails = BlockHelper.getFileDetails(fileInfo.getLength(), defaultBlock);
      FileBaseDetails fbd = new FileBaseDetails(fileInfo.getLength(), fileDetails.getValue0(), defaultBlock,
        fileDetails.getValue1(), hashAlg);
      baseDetails.put(fileId, fbd);
    }
    return Pair.with(nameToId, baseDetails);
  }
}
