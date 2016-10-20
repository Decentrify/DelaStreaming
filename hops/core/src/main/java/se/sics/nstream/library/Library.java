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
package se.sics.nstream.library;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.javatuples.Pair;
import se.sics.gvod.mngr.util.ElementSummary;
import se.sics.ktoolbox.util.identifiable.overlay.OverlayId;
import se.sics.ktoolbox.util.result.Result;
import se.sics.nstream.library.disk.LibrarySummaryHelper;
import se.sics.nstream.library.disk.LibrarySummaryJSON;
import se.sics.nstream.library.util.TorrentState;
import se.sics.nstream.storage.durable.util.MyStream;

/**
 *
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class Library {

    public static String LIBRARY_SUMMARY_FILE = "library.summary";

    private final Map<OverlayId, Pair<String, TorrentState>> torrentStatus = new HashMap<>();
    private final Map<OverlayId, Torrent> torrents = new HashMap<>();

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
        Pair<String, TorrentState> status = torrentStatus.remove(torrentId);
        status = Pair.with(status.getValue0(), TorrentState.DESTROYED);
        torrentStatus.put(torrentId, status);
        torrents.remove(torrentId);
        updateSummary();
    }

    public void prepareUpload(OverlayId torrentId, String torrentName) {
        torrentStatus.put(torrentId, Pair.with(torrentName, TorrentState.PREPARE_UPLOAD));
    }

    public void upload(OverlayId torrentId, MyStream manifestStream) {
        Pair<String, TorrentState> status = torrentStatus.remove(torrentId);
        status = Pair.with(status.getValue0(), TorrentState.UPLOADING);
        torrentStatus.put(torrentId, status);
        torrents.put(torrentId, new Torrent(manifestStream));
        updateSummary();
    }

    public void prepareDownload(OverlayId torrentId, String torrentName) {
        torrentStatus.put(torrentId, Pair.with(torrentName, TorrentState.PREPARE_DOWNLOAD));
    }

    public void download(OverlayId torrentId, MyStream manifestStream) {
        Pair<String, TorrentState> status = torrentStatus.remove(torrentId);
        status = Pair.with(status.getValue0(), TorrentState.DOWNLOADING);
        torrentStatus.put(torrentId, status);
        torrents.put(torrentId, new Torrent(manifestStream));
        updateSummary();
    }

    public void finishDownload(OverlayId torrentId) {
        Pair<String, TorrentState> status = torrentStatus.remove(torrentId);
        status = Pair.with(status.getValue0(), TorrentState.UPLOADING);
        torrentStatus.put(torrentId, status);
    }

    private void updateSummary() {
        LibrarySummaryJSON summaryResult = LibrarySummaryHelper.toSummary(torrents);
        Result<Boolean> writeResult = LibrarySummaryHelper.writeTorrentList(LIBRARY_SUMMARY_FILE, summaryResult);
        if(!writeResult.isSuccess()) {
            //TODO - try again next time?
        }
    }

    public List<ElementSummary> getSummary() {
        List<ElementSummary> summary = new ArrayList<>();
        for (Map.Entry<OverlayId, Pair<String, TorrentState>> e : torrentStatus.entrySet()) {
            ElementSummary es = new ElementSummary(e.getValue().getValue0(), e.getKey(), e.getValue().getValue1());
            summary.add(es);
        }
        return summary;
    }

    public static class Torrent {

        public final MyStream manifestStream;

        public Torrent(MyStream manifestStream) {
            this.manifestStream = manifestStream;
        }
    }
}
