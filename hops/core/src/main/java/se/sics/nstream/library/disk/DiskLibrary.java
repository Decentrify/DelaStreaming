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
package se.sics.nstream.library.disk;

import java.util.List;
import java.util.Map;
import se.sics.gvod.hops.api.LibraryCtrl;
import se.sics.gvod.hops.api.Torrent;
import se.sics.kompics.config.Config;
import se.sics.ktoolbox.util.identifiable.overlay.OverlayId;
import se.sics.ktoolbox.util.identifiable.overlay.OverlayIdFactory;
import se.sics.ktoolbox.util.network.KAddress;
import se.sics.ktoolbox.util.result.Result;
import se.sics.nstream.library.util.TorrentState;
import se.sics.nstream.storage.durable.util.MyStream;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class DiskLibrary implements LibraryCtrl {

  private final DiskLibraryConfig config;
  private final OverlayIdFactory torrentIdFactory;

  private Map<OverlayId, Torrent> torrents;

  public DiskLibrary(OverlayIdFactory torrentIdFactory, Config config) {
    this.torrentIdFactory = torrentIdFactory;
    this.config = new DiskLibraryConfig(config);
    readTorrents();
  }
  
  private void readTorrents() {
     Result<LibrarySummaryJSON> librarySummary = LibrarySummaryHelper.readTorrentList(config.librarySummary);
      if (!librarySummary.isSuccess()) {
        throw new RuntimeException("TODO fix me - corrupted library");
      }
      torrents =  LibrarySummaryHelper.fromSummary(librarySummary.getValue(), torrentIdFactory);
  }

  @Override
  public Map<OverlayId, Torrent> getTorrents() {
    return torrents;
  }
  
  @Override
  public boolean containsTorrent(OverlayId tId) {
    return torrents.containsKey(tId);
  }

  @Override
  public TorrentState stateOf(OverlayId tId) {
    Torrent t = torrents.get(tId);
    if (t == null) {
      return TorrentState.NONE;
    }
    return t.getTorrentStatus();
  }

  @Override
  public void killing(OverlayId torrentId) {
    torrents.get(torrentId).setTorrentStatus(TorrentState.KILLING);
  }

  @Override
  public void killed(OverlayId torrentId) {
    Torrent torrent = torrents.remove(torrentId);
    if (torrent != null) {
      updateSummary();
    }
  }

  @Override
  public void prepareUpload(Integer projectId, OverlayId torrentId, String torrentName) {
    Torrent torrent = new Torrent(projectId, torrentName, TorrentState.PREPARE_UPLOAD);
    torrents.put(torrentId, torrent);
  }

  @Override
  public void upload(OverlayId torrentId, MyStream manifestStream) {
    Torrent torrent = torrents.get(torrentId);
    torrent.setTorrentStatus(TorrentState.UPLOADING);
    torrent.setManifestStream(manifestStream);
    updateSummary();
  }

  @Override
  public void prepareDownload(Integer projectId, OverlayId torrentId, String torrentName, List<KAddress> partners) {
    Torrent torrent = new Torrent(projectId, torrentName, TorrentState.PREPARE_DOWNLOAD);
    torrent.setPartners(partners);
    torrents.put(torrentId, torrent);
  }

  @Override
  public void download(OverlayId torrentId, MyStream manifestStream) {
    Torrent torrent = torrents.get(torrentId);
    torrent.setTorrentStatus(TorrentState.DOWNLOADING);
    torrent.setManifestStream(manifestStream);
    updateSummary();
  }

  @Override
  public void finishDownload(OverlayId torrentId) {
    Torrent torrent = torrents.get(torrentId);
    torrent.setTorrentStatus(TorrentState.UPLOADING);
    updateSummary();
  }

  private void updateSummary() {
    LibrarySummaryJSON summaryResult = LibrarySummaryHelper.toSummary(torrents);
    Result<Boolean> writeResult = LibrarySummaryHelper.writeTorrentList(config.librarySummary, summaryResult);
    if (!writeResult.isSuccess()) {
      //TODO - try again next time?
    }
  }
}
