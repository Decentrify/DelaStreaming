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
package se.sics.nstream.hops.library.util;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import org.javatuples.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.kompics.util.Identifier;
import se.sics.ktoolbox.util.identifiable.overlay.OverlayId;
import se.sics.ktoolbox.util.identifiable.overlay.OverlayIdFactory;
import se.sics.ktoolbox.util.network.KAddress;
import se.sics.ktoolbox.util.result.Result;
import se.sics.nstream.hops.library.Torrent;
import se.sics.nstream.hops.storage.disk.DiskResource;
import se.sics.nstream.hops.storage.hdfs.HDFSEndpoint;
import se.sics.nstream.hops.storage.hdfs.HDFSResource;
import se.sics.nstream.library.util.TorrentState;
import se.sics.nstream.storage.durable.util.MyStream;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class LibrarySummaryHelper {

  private static final Logger LOG = LoggerFactory.getLogger(LibrarySummaryHelper.class);

  public static LibrarySummaryJSON toSummary(Map<OverlayId, Torrent> torrents) {
    LibrarySummaryJSON library = new LibrarySummaryJSON();
    DiskLibrarySummaryJSON disk = new DiskLibrarySummaryJSON();
    library.setDiskLibrary(disk);
    HDFSLibrarySummaryJSON hdfs = new HDFSLibrarySummaryJSON();
    library.setHdfsLibrary(hdfs);

    for (Map.Entry<OverlayId, Torrent> torrent : torrents.entrySet()) {
      if (!(torrent.getValue().getTorrentStatus().equals(TorrentState.DOWNLOADING)
        || torrent.getValue().getTorrentStatus().equals(TorrentState.UPLOADING))) {
        continue;
      }
      Identifier torrentBaseId = torrent.getKey().baseId;
      MyStream manifestStream = torrent.getValue().getManifestStream();
      if (manifestStream.resource instanceof DiskResource) {
        disk.addTorrent(DiskLibrarySummaryJSON.TorrentJSON.toJSON(torrent.getKey(), torrent.getValue()));
      } else if (manifestStream.resource instanceof HDFSResource) {
        hdfs.addTorrent(HDFSLibrarySummaryJSON.TorrentJSON.toJSON(torrent.getKey(), torrent.getValue()));
      }
    }
    return library;
  }

  public static Map<OverlayId, Torrent> fromSummary(LibrarySummaryJSON summary,
    OverlayIdFactory torrentIdFactory) {
    Map<OverlayId, Torrent> library = new HashMap<>();
    for (DiskLibrarySummaryJSON.TorrentJSON t : summary.getDiskLibrary().getTorrents()) {
      Pair<OverlayId, Torrent> torrent = t.fromJSON(torrentIdFactory);
      library.put(torrent.getValue0(), torrent.getValue1());
    }
    for (HDFSLibrarySummaryJSON.TorrentJSON t : summary.getHdfsLibrary().getTorrents()) {
      Pair<OverlayId, Torrent> torrent = t.fromJSON(torrentIdFactory);
      library.put(torrent.getValue0(), torrent.getValue1());
    }
    return library;
  }

  public static Result<Boolean> writeTorrentList(String torrentListPath, LibrarySummaryJSON summary) {
    Gson gson = new Gson();
    String jsonContent = gson.toJson(summary);

    try (FileWriter fileWriter = new FileWriter(torrentListPath)) {
      fileWriter.write(jsonContent);
      return Result.success(true);
    } catch (IOException ex) {
      return Result.externalSafeFailure(ex);
    }
  }

  public static Result<LibrarySummaryJSON> readTorrentList(String torrentListPath) {
    File torrentListFile = new File(torrentListPath);
    if (!torrentListFile.isFile()) {
      LOG.info("no torrent list file detected");
      try {
        torrentListFile.createNewFile();
        return createEmptyTorrentList(torrentListFile);
      } catch (IOException ex) {
        return Result.externalSafeFailure(ex);
      }
    }
    try {
      Scanner fileScanner = new Scanner(torrentListFile);
      String jsonContent = fileScanner.useDelimiter("\\Z").next();
      Gson gson = new Gson();
      LibrarySummaryJSON c = gson.fromJson(jsonContent, LibrarySummaryJSON.class);
      return Result.success(c);
    } catch (FileNotFoundException ex) {
      return Result.internalFailure(ex);
    }
  }

  private static Result<LibrarySummaryJSON> createEmptyTorrentList(File torrentListFile) throws IOException {
    LibrarySummaryJSON emptyContent = new LibrarySummaryJSON();
    emptyContent.setDiskLibrary(new DiskLibrarySummaryJSON());
    emptyContent.setHdfsLibrary(new HDFSLibrarySummaryJSON());

    Gson gson = new Gson();
    String jsonContent = gson.toJson(emptyContent);

    try (FileWriter fileWriter = new FileWriter(torrentListFile)) {
      fileWriter.write(jsonContent);
    }
    return Result.success(emptyContent);
  }

  public static MyStream streamFromJSON(String val) {
    Gson gson = new Gson();
    MyStreamJSON.Read readVal = gson.fromJson(val, MyStreamJSON.Read.class);
    if (readVal.getType().equals("hdfs")) {
      HDFSEndpointJSON endpoint = gson.fromJson(readVal.getEndpoint(), HDFSEndpointJSON.class);
      HDFSResourceJSON resource = gson.fromJson(readVal.getResource(), HDFSResourceJSON.class);
      return new MyStream(endpoint.fromJSON(), resource.fromJSON());
    } else {
      throw new RuntimeException("incomplete");
    }
  }

  public static String streamToJSON(MyStream stream) {
    Gson gson = new Gson();
    if (stream.endpoint instanceof HDFSEndpoint) {
      MyStreamJSON.Write writeVal = new MyStreamJSON.Write("hdfs", HDFSEndpointJSON.toJSON(
        (HDFSEndpoint) stream.endpoint), HDFSResourceJSON.toJSON((HDFSResource) stream.resource));
      return gson.toJson(writeVal);
    } else {
      throw new RuntimeException("incomplete");
    }
  }

  public static String partnersToJSON(List<KAddress> partners) {
    Gson gson = new Gson();
    List<AddressJSON> ps = new LinkedList<>();
    for (KAddress p : partners) {
      ps.add(AddressJSON.toJSON(p));
    }
    return gson.toJson(ps);
  }

  public static List<KAddress> partnersFromJSON(String stringP) {
    Gson gson = new Gson();
    Type listType = new TypeToken<ArrayList<AddressJSON>>() {
    }.getType();
    List<AddressJSON> partners = new Gson().fromJson(stringP, listType);
    List<KAddress> ps = new LinkedList<>();
    if (partners != null) {
      for (AddressJSON p : partners) {
        ps.add(p.fromJSON());
      }
    }
    return ps;
  }
}
