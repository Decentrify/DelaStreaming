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
package se.sics.nstream.library;

import se.sics.ktoolbox.util.identifiable.overlay.OverlayId;
import se.sics.ktoolbox.util.result.Result;
import se.sics.nstream.library.util.TorrentState;

/**
 *
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class TorrentChecker {

  public static Result upload(Library library, Integer projectId, OverlayId torrentId, String torrentName) {
    if (!library.containsTorrent(torrentId)) {
      return Result.badArgument("torrent:" + torrentId + " is not active");
    }
    Result r1 = library.checkBasicDetails(torrentId, projectId, torrentName);
    if (!r1.isSuccess()) {
      return r1;
    }
    Result r2 = library.checkState(torrentId, TorrentState.PREPARE_UPLOAD);
    if (!r2.isSuccess()) {
      return r1;
    }
    return Result.success(true);
  }

  public static Result prepareDownload(Library library, Integer projectId, OverlayId torrentId, String torrentName) {
    if (!library.containsTorrent(torrentId)) {
      return Result.success(true);
    }
    Result r1 = library.checkBasicDetails(torrentId, projectId, torrentName);
    if (!r1.isSuccess()) {
      return r1;
    }
    Result r2 = library.checkState(torrentId, TorrentState.NONE);
    if (!r2.isSuccess()) {
      return r1;
    }
    return Result.success(true);
  }

  public static Result download(Library library, Integer projectId, OverlayId torrentId,
    String torrentName) {
    if (!library.containsTorrent(torrentId)) {
      return Result.badArgument("torrent:" + torrentId + " is not active");
    }
    Result r1 = library.checkBasicDetails(torrentId, projectId, torrentName);
    if (!r1.isSuccess()) {
      return r1;
    }
    Result r2 = library.checkState(torrentId, TorrentState.PREPARE_DOWNLOAD);
    if (!r2.isSuccess()) {
      return r1;
    }
    return Result.success(true);
  }
}
