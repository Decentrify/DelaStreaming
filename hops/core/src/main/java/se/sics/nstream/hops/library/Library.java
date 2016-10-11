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
import se.sics.ktoolbox.util.identifiable.overlay.OverlayId;
import se.sics.nstream.library.util.TorrentState;
import se.sics.nstream.storage.durable.util.MyStream;
import se.sics.nstream.storage.durable.util.StreamEndpoint;
import se.sics.nstream.transfer.MyTorrent;

/**
 *
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class Library {

    private final Map<OverlayId, Pair<String, TorrentState>> torrentStatus = new HashMap<>();
    private final Map<OverlayId, Torrent> manifests = new HashMap<>();
    private final TorrentList torrentList;

    public Library(TorrentList torrentList) {
        this.torrentList = torrentList;
    }

    public boolean containsTorrent(OverlayId torrentId) {
        return torrentStatus.containsKey(torrentId);
    }

    public TorrentState getStatus(OverlayId torrentId) {
        Pair<String, TorrentState> status = torrentStatus.get(torrentId);
        if (status == null) {
            status = Pair.with("", TorrentState.NONE);
        }
        return status.getValue1();
    }

    public void destroyed(OverlayId torrentId) {
        remove(torrentId);
        Pair<String, TorrentState> status = torrentStatus.remove(torrentId);
        status = Pair.with(status.getValue0(), TorrentState.DESTROYED);
        torrentStatus.put(torrentId, status);
    }

    public void upload(OverlayId torrentId, String torrentName, Torrent torrent) {
        torrentStatus.put(torrentId, Pair.with(torrentName, TorrentState.UPLOADING));
        manifests.put(torrentId, torrent);
//        torrentList.write(TorrentSummary.getSummary(torrentId, torrentName, torrent), torrent.endpoint.hopsURL, torrent.endpoint.user);
    }

    public void download1(OverlayId torrentId, String torrentName) {
        torrentStatus.put(torrentId, Pair.with(torrentName, TorrentState.PRE_DOWNLOAD_1));
    }

    public void download2(OverlayId torrentId) {
        Pair<String, TorrentState> ts = torrentStatus.get(torrentId);
        torrentStatus.put(torrentId, Pair.with(ts.getValue0(), TorrentState.PRE_DOWNLOAD_2));
    }

    public void download3(OverlayId torrentId, Torrent torrent) {
        Pair<String, TorrentState> status = torrentStatus.remove(torrentId);
        status = Pair.with(status.getValue0(), TorrentState.DOWNLOADING);
        torrentStatus.put(torrentId, status);
        manifests.put(torrentId, torrent);
//        torrentList.write(TorrentSummary.getSummary(torrentId, torrentName, torrent), torrent.endpoint.hopsURL, torrent.endpoint.user);
    }


    public Torrent getTorrent(OverlayId torrentId) {
        return manifests.get(torrentId);
    }

    public void finishDownload(OverlayId torrentId) {
        Pair<String, TorrentState> status = torrentStatus.remove(torrentId);
        status = Pair.with(status.getValue0(), TorrentState.UPLOADING);
        torrentStatus.put(torrentId, status);
    }

    private void remove(OverlayId torrentId) {
        manifests.remove(torrentId);
    }

    public List<ElementSummary> getSummary() {
        List<ElementSummary> summary = new ArrayList<>();
        for (Map.Entry<OverlayId, Pair<String, TorrentState>> e : torrentStatus.entrySet()) {
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

        public final Map<Identifier, StreamEndpoint> endpoints;
        public final MyStream manifest;
        public final MyTorrent torrent;

        public Torrent(Map<Identifier, StreamEndpoint> endpoints, MyStream manifest, MyTorrent torrent) {
            this.endpoints = endpoints;
            this.manifest = manifest;
            this.torrent = torrent;
        }
    }
}
