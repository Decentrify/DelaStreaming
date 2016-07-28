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

import com.google.common.base.Optional;
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
import se.sics.nstream.util.FileExtendedDetails;
import se.sics.nstream.util.TorrentDetails;
import se.sics.nstream.util.TransferDetails;

/**
 *
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class Library {
    private final Map<Identifier, TorrentStatus> torrentStatus = new HashMap<>();
    private final Map<Identifier, Pair<HDFSEndpoint, HDFSResource>> manifests = new HashMap<>();
    private final Map<Identifier, TorrentDetails> pendingDownload = new HashMap<>();
    private final Map<Identifier, TransferDetails> torrents = new HashMap<>();

    public boolean containsTorrent(Identifier torrentId) {
        return torrentStatus.containsKey(torrentId);
    }

    public TorrentStatus getStatus(Identifier torrentId) {
        TorrentStatus status = torrentStatus.get(torrentId);
        if(status == null) {
            status = TorrentStatus.NONE;
        }
        return status;
    }

    public void destroyed(Identifier torrentId) {
        remove(torrentId);
        torrentStatus.put(torrentId, TorrentStatus.DESTROYED);
    }
    
    public void upload(Identifier torrentId, Pair<HDFSEndpoint, HDFSResource> manifest, TransferDetails transferDetails) {
        torrentStatus.put(torrentId, TorrentStatus.UPLOADING);
        manifests.put(torrentId, manifest);
        torrents.put(torrentId, transferDetails);
    }
    
    public void startingDownload(Identifier torrentId, Pair<HDFSEndpoint, HDFSResource> manifest) {
        torrentStatus.put(torrentId, TorrentStatus.PENDING_DOWNLOAD);
        manifests.put(torrentId, manifest);
    }
    
    public void pendingDownload(Identifier torrentId, TorrentDetails torrentDetails) {
        pendingDownload.put(torrentId, torrentDetails);
    }
    
    public Map<String, FileExtendedDetails> getExtendedDetails(Identifier torrentId) {
        TransferDetails aux = torrents.get(torrentId);
        return aux.extended;
    }
    
    public Optional<TorrentDetails> getTorrentDetails(Identifier torrentId) {
        return Optional.fromNullable(pendingDownload.get(torrentId));
    }
    
    public Pair<HDFSEndpoint, HDFSResource> getManifest(Identifier torrentId) {
        return manifests.get(torrentId);
    }
    
    public TransferDetails download(Identifier torrentId, Map<String, FileExtendedDetails> extendedDetails) {
        torrentStatus.put(torrentId, TorrentStatus.DOWNLOADING);
        TorrentDetails torrentDetails = pendingDownload.remove(torrentId);
        TransferDetails transferDetails = new TransferDetails(torrentDetails, extendedDetails);
        torrents.put(torrentId, transferDetails);
        return transferDetails;
    }
    
    public void finishDownload(Identifier torrentId) {
        torrentStatus.put(torrentId, TorrentStatus.UPLOADING);
    }

    public void remove(Identifier torrentId) {
        manifests.remove(torrentId);
        pendingDownload.remove(torrentId);
        torrents.remove(torrentId);
        torrentStatus.remove(torrentId);
    }

    public List<ElementSummary> getSummary() {
        List<ElementSummary> summary = new ArrayList<>();
//        for (Map.Entry<Identifier, Pair<FileInfo, TorrentInfo>> e : libraryContents.entrySet()) {
//            ElementSummary es = new ElementSummary(e.getValue().getValue0().name, e.getKey(), e.getValue().getValue1().getStatus());
//            summary.add(es);
//        }
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
}
