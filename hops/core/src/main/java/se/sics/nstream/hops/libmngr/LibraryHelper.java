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
package se.sics.nstream.hops.libmngr;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import se.sics.ktoolbox.util.identifiable.overlay.OverlayId;
import se.sics.nstream.hops.library.LibraryCtrl;
import se.sics.nstream.hops.library.Torrent;
import se.sics.nstream.library.util.TorrentState;
import se.sics.nstream.mngr.util.ElementSummary;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class LibraryHelper {

  public static Map<Integer, List<ElementSummary>> getAllSummary(LibraryCtrl library) {
    Map<Integer, List<ElementSummary>> summary = new TreeMap<>();
    Map<OverlayId, Torrent> torrents = library.getTorrents();
    for (Map.Entry<OverlayId, Torrent> e : torrents.entrySet()) {
      long speed = (long) e.getValue().getStatus().getDownloadSpeed();
      double dynamic = e.getValue().getStatus().getPercentageComplete();
      TorrentState ts = e.getValue().getTorrentStatus();
      ElementSummary es;
      if (ts.equals(TorrentState.DOWNLOADING)) {
        es = new ElementSummary.Download(e.getValue().torrentName, e.getKey(), ts, speed, dynamic);
      } else {
        es = new ElementSummary.Upload(e.getValue().torrentName, e.getKey(), ts);
      }
      List<ElementSummary> projectSummary = summary.get(e.getValue().projectId);
      if (projectSummary == null) {
        projectSummary = new LinkedList<>();
        summary.put(e.getValue().projectId, projectSummary);
      }
      projectSummary.add(es);
    }
    return summary;
  }

  public static Map<Integer, List<ElementSummary>> getSummary(LibraryCtrl library, List<Integer> projectIds) {
    if (projectIds.isEmpty()) {
      return getAllSummary(library);
    }
    Map<Integer, List<ElementSummary>> summary = new TreeMap<>();
    Map<OverlayId, Torrent> torrents = library.getTorrents();
    for (Integer projectId : projectIds) {
      summary.put(projectId, new LinkedList<ElementSummary>());
    }
    for (Map.Entry<OverlayId, Torrent> e : torrents.entrySet()) {
      if (projectIds.contains(e.getValue().projectId)) {
        List<ElementSummary> projectSummary = summary.get(e.getValue().projectId);
        long speed = (long) e.getValue().getStatus().getDownloadSpeed();
        double dynamic = e.getValue().getStatus().getPercentageComplete();
        TorrentState ts = e.getValue().getTorrentStatus();
        ElementSummary es;
        if (ts.equals(TorrentState.DOWNLOADING)) {
          es = new ElementSummary.Download(e.getValue().torrentName, e.getKey(), ts, speed, dynamic);
        } else {
          es = new ElementSummary.Upload(e.getValue().torrentName, e.getKey(), ts);
        }
        projectSummary.add(es);
      }
    }
    return summary;
  }
}
