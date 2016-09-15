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
import org.javatuples.Triplet;
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

    private final Map<Identifier, Pair<String, TorrentStatus>> torrentStatus = new HashMap<>();
    private final Map<Identifier, Triplet<String, HDFSEndpoint, HDFSResource>> manifests = new HashMap<>();
    private final Map<Identifier, TorrentDetails> pendingDownload = new HashMap<>();
    private final Map<Identifier, TransferDetails> torrents = new HashMap<>();

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

    public void upload(Identifier torrentId, String torrentName, HDFSEndpoint hdfsEndpoint, HDFSResource manifest, TransferDetails transferDetails) {
        torrentStatus.put(torrentId, Pair.with(torrentName, TorrentStatus.UPLOADING));
        manifests.put(torrentId, Triplet.with(torrentName, hdfsEndpoint, manifest));
        torrents.put(torrentId, transferDetails);
    }

    public void startingDownload(Identifier torrentId, String torrentName, HDFSEndpoint hdfsEndpoint, HDFSResource manifest) {
        torrentStatus.put(torrentId, Pair.with(torrentName, TorrentStatus.PENDING_DOWNLOAD));
        manifests.put(torrentId, Triplet.with(torrentName, hdfsEndpoint, manifest));
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

    public Triplet<String, HDFSEndpoint, HDFSResource> getManifest(Identifier torrentId) {
        return manifests.get(torrentId);
    }

    public TransferDetails download(Identifier torrentId, Map<String, FileExtendedDetails> extendedDetails) {
        Pair<String, TorrentStatus> status = torrentStatus.remove(torrentId);
        status = Pair.with(status.getValue0(), TorrentStatus.DOWNLOADING);
        torrentStatus.put(torrentId, status);
        TorrentDetails torrentDetails = pendingDownload.remove(torrentId);
        TransferDetails transferDetails = new TransferDetails(null, torrentDetails, extendedDetails);
        torrents.put(torrentId, transferDetails);
        return transferDetails;
    }

    public void finishDownload(Identifier torrentId) {
        Pair<String, TorrentStatus> status = torrentStatus.remove(torrentId);
        status = Pair.with(status.getValue0(), TorrentStatus.UPLOADING);
        torrentStatus.put(torrentId, status);
    }

    private void remove(Identifier torrentId) {
        manifests.remove(torrentId);
        pendingDownload.remove(torrentId);
        torrents.remove(torrentId);
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
}
