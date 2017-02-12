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
package se.sics.nstream.library.disk;

import com.google.gson.Gson;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.ktoolbox.util.identifiable.BasicBuilders;
import se.sics.ktoolbox.util.identifiable.Identifier;
import se.sics.ktoolbox.util.identifiable.overlay.OverlayId;
import se.sics.ktoolbox.util.identifiable.overlay.OverlayIdFactory;
import se.sics.ktoolbox.util.result.Result;
import se.sics.nstream.hops.hdfs.HDFSEndpoint;
import se.sics.nstream.hops.hdfs.HDFSResource;
import se.sics.nstream.library.Library;
import se.sics.nstream.library.util.TorrentState;
import se.sics.nstream.storage.durable.disk.DiskEndpoint;
import se.sics.nstream.storage.durable.disk.DiskResource;
import se.sics.nstream.storage.durable.util.MyStream;
import se.sics.nstream.transfer.MyTorrent;

/**
 *
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class LibrarySummaryHelper {
    
    private static final Logger LOG = LoggerFactory.getLogger(LibrarySummaryHelper.class);
    
    public static LibrarySummaryJSON toSummary(Map<OverlayId, Library.Torrent> torrents) {
        LibrarySummaryJSON library = new LibrarySummaryJSON();
        DiskLibrarySummaryJSON disk = new DiskLibrarySummaryJSON();
        library.setDiskLibrary(disk);
        HDFSLibrarySummaryJSON hdfs = new HDFSLibrarySummaryJSON();
        library.setHdfsLibrary(hdfs);
        
        for(Map.Entry<OverlayId, Library.Torrent> torrent : torrents.entrySet()) {
            Identifier torrentBaseId = torrent.getKey().baseId;
            MyStream manifestStream = torrent.getValue().getManifestStream();
            if(manifestStream.resource instanceof DiskResource) {
                DiskLibrarySummaryJSON.TorrentJSON torrentJSON = new DiskLibrarySummaryJSON.TorrentJSON();
                torrentJSON.setProjectId(torrent.getValue().projectId);
                torrentJSON.setTorrentName(torrent.getValue().torrentName);
                torrentJSON.setTorrentStatus(torrent.getValue().getTorrentStatus().toString());
                torrentJSON.setBaseId(torrentBaseId.toString());
                torrentJSON.setDirPath(((DiskResource)manifestStream.resource).dirPath);
                disk.addTorrent(torrentJSON);
            } else if(manifestStream.resource instanceof HDFSResource) {
                HDFSLibrarySummaryJSON.TorrentJSON torrentJSON = new HDFSLibrarySummaryJSON.TorrentJSON();
                torrentJSON.setProjectId(torrent.getValue().projectId);
                torrentJSON.setTorrentName(torrent.getValue().torrentName);
                torrentJSON.setTorrentStatus(torrent.getValue().getTorrentStatus().toString());
                torrentJSON.setBaseId(torrentBaseId.toString());
                //endpoint
                HDFSEndpoint hdfsEndpoint = (HDFSEndpoint)manifestStream.endpoint;
                HDFSLibrarySummaryJSON.HDFSEndpointJSON endpoint = new HDFSLibrarySummaryJSON.HDFSEndpointJSON();
                endpoint.setUrl(hdfsEndpoint.hopsURL);
                endpoint.setUser(hdfsEndpoint.user);
                torrentJSON.setEndpoint(endpoint);
                //resource
                HDFSResource hdfsResource = (HDFSResource)manifestStream.resource;
                torrentJSON.setDirPath(hdfsResource.dirPath);
                hdfs.addTorrent(torrentJSON);
            }
        }
        return library;
    }
    
    public static Map<OverlayId, Library.Torrent> fromSummary(LibrarySummaryJSON summary, OverlayIdFactory torrentIdFactory) {
        Map<OverlayId, Library.Torrent> library = new HashMap<>();
        for(DiskLibrarySummaryJSON.TorrentJSON torrent : summary.getDiskLibrary().getTorrents()) {
            OverlayId torrentId = torrentIdFactory.id(new BasicBuilders.StringBuilder(torrent.getBaseId()));
            DiskResource manifestResource = new DiskResource(torrent.getDirPath(), MyTorrent.MANIFEST_NAME);
            MyStream manifestStream = new MyStream(new DiskEndpoint(), manifestResource);
            TorrentState status = TorrentState.valueOf(torrent.getTorrentStatus());
            library.put(torrentId, new Library.Torrent(torrent.getProjectId(), torrent.getTorrentName(), status, manifestStream));
        }
        for(HDFSLibrarySummaryJSON.TorrentJSON torrent : summary.getHdfsLibrary().getTorrents()) {
            OverlayId torrentId = torrentIdFactory.id(new BasicBuilders.StringBuilder(torrent.getBaseId()));
            HDFSEndpoint endpoint = HDFSEndpoint.getBasic(torrent.getEndpoint().getUrl(), torrent.getEndpoint().getUser());
            HDFSResource manifestResource = new HDFSResource(torrent.getDirPath(), MyTorrent.MANIFEST_NAME);
            MyStream manifestStream = new MyStream(endpoint, manifestResource);
            TorrentState status = TorrentState.valueOf(torrent.getTorrentStatus());
            library.put(torrentId, new Library.Torrent(torrent.getProjectId(), torrent.getTorrentName(), status, manifestStream));
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
}
