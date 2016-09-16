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
 * GNU General Public License for more defLastBlock.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
 */
package se.sics.nstream.hops.library;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.javatuples.Pair;
import se.sics.gvod.mngr.util.ElementSummary;
import se.sics.ktoolbox.util.identifiable.Identifier;
import se.sics.nstream.hops.hdfs.HDFSEndpoint;
import se.sics.nstream.hops.hdfs.HDFSResource;
import se.sics.nstream.library.util.TorrentStatus;
import se.sics.nstream.transfer.MyTorrent;
import se.sics.nstream.transfer.MyTorrent.Manifest;
import se.sics.nstream.util.FileExtendedDetails;

/**
 *
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class Library {

    private final Map<Identifier, Pair<String, TorrentStatus>> torrentStatus = new HashMap<>();
    private final Map<Identifier, Pair<String, Torrent>> manifests = new HashMap<>();
    private final Map<Identifier, Pair<String, TorrentBuilder>> pendingManifests = new HashMap<>();

    public boolean containsTorrent(Identifier torrentId) {
        return torrentStatus.containsKey(torrentId);
    }

    public TorrentStatus getStatus(Identifier torrentId) {
        Pair<String, TorrentStatus> status = torrentStatus.get(torrentId);
        if (status == null) {
            status = Pair.with("", TorrentStatus.NONE);
        }
        return status.getValue1();
    }

    public void destroyed(Identifier torrentId) {
        remove(torrentId);
        Pair<String, TorrentStatus> status = torrentStatus.remove(torrentId);
        status = Pair.with(status.getValue0(), TorrentStatus.DESTROYED);
        torrentStatus.put(torrentId, status);
    }

    public void upload(Identifier torrentId, String torrentName, Torrent torrent) {
        torrentStatus.put(torrentId, Pair.with(torrentName, TorrentStatus.UPLOADING));
        manifests.put(torrentId, Pair.with(torrentName, torrent));
    }

    public void startDownload(Identifier torrentId, String torrentName, TorrentBuilder torrentBuilder) {
        torrentStatus.put(torrentId, Pair.with(torrentName, TorrentStatus.DOWNLOAD_1));
        pendingManifests.put(torrentId, Pair.with(torrentName, torrentBuilder));
    }

    public byte[] pendingDownload(Identifier torrentId, Manifest manifest) {
        Pair<String, TorrentStatus> ts = torrentStatus.get(torrentId);
        Pair<String, TorrentBuilder> torrentBuilder = pendingManifests.get(torrentId);
        torrentBuilder.getValue1().torrentBuilder.manifestBuilder.addBlocks(manifest.manifestBlocks);
        torrentStatus.put(torrentId, Pair.with(ts.getValue0(), TorrentStatus.DOWNLOAD_2));
        return manifest.manifestByte;
    }
    
    public MyTorrent download(Identifier torrentId, Map<String, FileExtendedDetails> extendedDetails) {
        Pair<String, TorrentStatus> status = torrentStatus.remove(torrentId);
        status = Pair.with(status.getValue0(), TorrentStatus.DOWNLOADING);
        torrentStatus.put(torrentId, status);
        Pair<String, TorrentBuilder> torrentBuilder = pendingManifests.remove(torrentId);
        torrentBuilder.getValue1().torrentBuilder.setExtended(extendedDetails);
        Torrent torrent = torrentBuilder.getValue1().build();
        manifests.put(torrentId, Pair.with(torrentBuilder.getValue0(), torrent));
        return torrent.torrent;
    }

    public Map<String, FileExtendedDetails> getExtendedDetails(Identifier torrentId) {
        Pair<String, Torrent> aux = manifests.get(torrentId);
        return aux.getValue1().torrent.extended;
    }
    
    public Pair<String, Torrent> getTorrent(Identifier torrentId) {
        return manifests.get(torrentId);
    }
    
    public Pair<String, TorrentBuilder> getTorrentBuilder(Identifier torrentId) {
        return pendingManifests.get(torrentId);
    }

//    public Optional<TorrentDetails> getTorrentDetails(Identifier torrentId) {
//        return Optional.fromNullable(pendingDownload.get(torrentId));
//    }
//
//    public Triplet<String, HDFSEndpoint, HDFSResource> getManifest(Identifier torrentId) {
//        return manifests.get(torrentId);
//    }

    public void finishDownload(Identifier torrentId) {
        Pair<String, TorrentStatus> status = torrentStatus.remove(torrentId);
        status = Pair.with(status.getValue0(), TorrentStatus.UPLOADING);
        torrentStatus.put(torrentId, status);
    }

    private void remove(Identifier torrentId) {
        manifests.remove(torrentId);
        pendingManifests.remove(torrentId);
    }

    public List<ElementSummary> getSummary() {
        List<ElementSummary> summary = new ArrayList<>();
        for (Map.Entry<Identifier, Pair<String, TorrentStatus>> e : torrentStatus.entrySet()) {
            ElementSummary es = new ElementSummary(e.getValue().getValue0(), e.getKey(), e.getValue().getValue1());
            summary.add(es);
        }
        return summary;
    }

    public List<ElementSummary> getSummary(int projectId) {
        List<ElementSummary> summary = new ArrayList<>();
//        for (Map.Entry<Identifier, Pair<FileInfo, TorrentInfo>> e : libraryContents.entrySet()) {
//            HopsResource hopsResource = hopsResources.get(e.getKey());
//            if (hopsResource.projectId == projectId) {
//                ElementSummary es = new ElementSummary(e.getValue().getValue0().name, e.getKey(), e.getValue().getValue1().getStatus());
//                summary.add(es);
//            }
//        }
        return summary;
    }
    
    public static class Torrent {
        public final HDFSEndpoint hdfsEndpoint;
        public final HDFSResource manifest;
        public final MyTorrent torrent;
        
        public Torrent(HDFSEndpoint hdfsEndpoint, HDFSResource manifest, MyTorrent torrent) {
            this.hdfsEndpoint = hdfsEndpoint;
            this.manifest = manifest;
            this.torrent = torrent;
        }
    }
    public static class TorrentBuilder {
        public final HDFSEndpoint hdfsEndpoint;
        public final HDFSResource manifestResource;
        private MyTorrent.Builder torrentBuilder;
        
        public TorrentBuilder(HDFSEndpoint hdfsEndpoint, HDFSResource manifest) {
            this.hdfsEndpoint = hdfsEndpoint;
            this.manifestResource = manifest;
        }
        
        public Torrent build() {
            return new Torrent(hdfsEndpoint, manifestResource, torrentBuilder.build());
        }
    }
}
