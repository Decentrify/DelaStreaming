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

import com.google.common.base.Optional;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import se.sics.gvod.mngr.util.ElementSummary;
import se.sics.ktoolbox.util.identifiable.overlay.OverlayId;
import se.sics.ktoolbox.util.network.KAddress;
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

  private final String librarySumaryFile;

  private final Map<OverlayId, Torrent> torrents = new HashMap<>();

  public Library(String librarySummaryFile) {
    this.librarySumaryFile = librarySummaryFile;
  }

//  public void destroyed(OverlayId torrentId) {
//    Pair<String, TorrentState> status = torrentStatus.remove(torrentId);
//    status = Pair.with(status.getValue0(), TorrentState.DESTROYED);
//    torrentStatus.put(torrentId, status);
//    torrents.remove(torrentId);
//    updateSummary();
//  }
  public boolean containsTorrent(OverlayId tId) {
    return torrents.containsKey(tId);
  }

  public TorrentState stateOf(OverlayId tId) {
    Torrent t = torrents.get(tId);
    if (t == null) {
      return TorrentState.NONE;
    }
    return t.torrentStatus;
  }

  public Result<Boolean> checkState(OverlayId tId, TorrentState expectedState) {
    Torrent t = torrents.get(tId);
    if (expectedState.equals(t.torrentStatus)) {
      return Result.success(true);
    }
    return Result.badArgument("torrent:" + tId + " expected state:" + expectedState + " found state:" + t.torrentStatus);
  }

  public Result<Boolean> checkBasicDetails(OverlayId tId, Integer projectId, String tName) {
    Torrent t = torrents.get(tId);
    if (!t.projectId.equals(projectId)) {
      return Result.badArgument("torrent:" + tId + "already active in project:" + t.projectId);
    }
    if (!t.torrentName.equals(tName)) {
      return Result.badArgument("torrent:" + tId + "already active with name:" + t.torrentName);
    }
    return Result.success(true);
  }

  public void remove(OverlayId torrentId) {
    Torrent torrent = torrents.remove(torrentId);
    if (torrent != null) {
      updateSummary();
    }
  }

  public void prepareUpload(Integer projectId, OverlayId torrentId, String torrentName) {
    Torrent torrent = new Torrent(projectId, torrentName, TorrentState.PREPARE_UPLOAD);
    torrents.put(torrentId, torrent);
  }

  public void upload(OverlayId torrentId, MyStream manifestStream) {
    Torrent torrent = torrents.get(torrentId);
    torrent.setTorrentStatus(TorrentState.UPLOADING);
    torrent.setManifestStream(manifestStream);
    updateSummary();
  }

  public void prepareDownload(Integer projectId, OverlayId torrentId, String torrentName, List<KAddress> partners) {
    Torrent torrent = new Torrent(projectId, torrentName, TorrentState.PREPARE_DOWNLOAD);
    torrent.setPartners(partners);
    torrents.put(torrentId, torrent);
  }

  public void download(OverlayId torrentId, MyStream manifestStream) {
    Torrent torrent = torrents.get(torrentId);
    torrent.setTorrentStatus(TorrentState.DOWNLOADING);
    torrent.setManifestStream(manifestStream);
    updateSummary();
  }

  public void finishDownload(OverlayId torrentId) {
    Torrent torrent = torrents.get(torrentId);
    torrent.setTorrentStatus(TorrentState.UPLOADING);
    updateSummary();
  }

  private void updateSummary() {
    LibrarySummaryJSON summaryResult = LibrarySummaryHelper.toSummary(torrents);
    Result<Boolean> writeResult = LibrarySummaryHelper.writeTorrentList(librarySumaryFile, summaryResult);
    if (!writeResult.isSuccess()) {
      //TODO - try again next time?
    }
  }

  public Map<Integer, List<ElementSummary>> getAllSummary() {
    Map<Integer, List<ElementSummary>> summary = new TreeMap<>();
    for (Map.Entry<OverlayId, Torrent> e : torrents.entrySet()) {
      ElementSummary es = new ElementSummary(e.getValue().torrentName, e.getKey(), e.getValue().torrentStatus);
      List<ElementSummary> projectSummary = summary.get(e.getValue().projectId);
      if(projectSummary == null) {
        projectSummary = new LinkedList<>();
        summary.put(e.getValue().projectId, projectSummary);
      }
      projectSummary.add(es);
    }
    return summary;
  }

  public Map<Integer, List<ElementSummary>> getSummary(List<Integer> projectIds) {
    if(projectIds.isEmpty()) {
      return getAllSummary();
    }
    Map<Integer, List<ElementSummary>> summary = new TreeMap<>();
    for(Integer projectId : projectIds) {
      summary.put(projectId, new LinkedList<ElementSummary>());
    }
    for (Map.Entry<OverlayId, Torrent> e : torrents.entrySet()) {
      if (projectIds.contains(e.getValue().projectId)) {
        List<ElementSummary> projectSummary = summary.get(e.getValue().projectId);
        ElementSummary es = new ElementSummary(e.getValue().torrentName, e.getKey(), e.getValue().torrentStatus);
        projectSummary.add(es);
      }
    }
    return summary;
  }

  public static class Torrent {

    public final Integer projectId;
    public final String torrentName;
    private TorrentState torrentStatus = TorrentState.NONE;
    private Optional<MyStream> manifestStream = Optional.absent();
    private List<KAddress> partners = new LinkedList<>();

    public Torrent(Integer projectId, String torrentName, TorrentState torrentStatus) {
      this.projectId = projectId;
      this.torrentName = torrentName;
      this.torrentStatus = torrentStatus;
    }

    public TorrentState getTorrentStatus() {
      return torrentStatus;
    }

    public void setTorrentStatus(TorrentState torrentStatus) {
      this.torrentStatus = torrentStatus;
    }

    public MyStream getManifestStream() {
      return manifestStream.get();
    }

    public void setManifestStream(MyStream manifestStream) {
      this.manifestStream = Optional.of(manifestStream);
    }

    public List<KAddress> getPartners() {
      return partners;
    }

    public void setPartners(List<KAddress> partners) {
      this.partners = partners;
    }
  }
}
