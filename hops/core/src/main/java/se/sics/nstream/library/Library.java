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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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

  private final String librarySumaryFile;

  private final Map<OverlayId, Torrent> torrents = new HashMap<>();

  public Library(String librarySummaryFile) {
    this.librarySumaryFile = librarySummaryFile;
  }

//  public void destroyed(OverlayId torrentId) {
//    Pair<String, TorrentState> status = torrentStatus.destroy(torrentId);
//    status = Pair.with(status.getValue0(), TorrentState.DESTROYED);
//    torrentStatus.put(torrentId, status);
//    torrents.destroy(torrentId);
//    updateSummary();
//  }
  
  public boolean containsTorrent(OverlayId tId) {
    return torrents.containsKey(tId);
  }
  
  public TorrentState stateOf(OverlayId tId) {
    Torrent t = torrents.get(tId);
    if(t == null) {
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

  public Result<Boolean> checkBasicDetails(OverlayId tId, String projectId, String tName) {
    Torrent t = torrents.get(tId);
    if (!t.projectId.equals(projectId)) {
      return Result.badArgument("torrent:" + tId + "already active in project:" + t.projectId);
    }
    if (!t.torrentName.equals(tName)) {
      return Result.badArgument("torrent:" + tId + "already active with name:" + t.torrentName);
    }
    return Result.success(true);
  }

  public void destroy(OverlayId torrentId) {
    Torrent torrent = torrents.get(torrentId);
    if (torrent != null) {
      torrent.setTorrentStatus(TorrentState.DESTROYED);
      updateSummary();
    }
  }

  public void prepareUpload(String projectId, OverlayId torrentId, String torrentName) {
    Torrent torrent = new Torrent(projectId, torrentName, TorrentState.PREPARE_UPLOAD);
    torrents.put(torrentId, torrent);
  }

  public void upload(OverlayId torrentId, MyStream manifestStream) {
    Torrent torrent = torrents.get(torrentId);
    torrent.setTorrentStatus(TorrentState.UPLOADING);
    torrent.setManifestStream(manifestStream);
    updateSummary();
  }

  public void prepareDownload(String projectId, OverlayId torrentId, String torrentName) {
    Torrent torrent = new Torrent(projectId, projectId, TorrentState.PREPARE_DOWNLOAD);
    torrents.put(torrentId, torrent);
  }

  public void download(String projectId, OverlayId torrentId, String torrentName, MyStream manifestStream) {
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

  public List<ElementSummary> getSummary() {
    List<ElementSummary> summary = new ArrayList<>();
    for (Map.Entry<OverlayId, Torrent> e : torrents.entrySet()) {
      ElementSummary es = new ElementSummary(e.getValue().torrentName, e.
        getKey(), e.getValue().torrentStatus);
      summary.add(es);
    }
    return summary;
  }

  public static class Torrent {

    //TODO Alex - duplicate data torrentName
    public final String projectId;
    public final String torrentName;
    private TorrentState torrentStatus;
    private Optional<MyStream> manifestStream;

    private Torrent(String projectId, String torrentName,
      TorrentState torrentStatus, Optional<MyStream> manifestStream) {
      this.projectId = projectId;
      this.torrentName = torrentName;
      this.torrentStatus = torrentStatus;
      this.manifestStream = manifestStream;
    }

    public Torrent(String projectId, String torrentName, TorrentState torrentStatus, MyStream manifestStream) {
      this(projectId, torrentName, torrentStatus, Optional.of(manifestStream));
    }

    public Torrent(String projectId, String torrentName,TorrentState torrentStatus) {
      this(projectId, torrentName, torrentStatus, Optional.fromNullable((MyStream) null));
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
  }
}
