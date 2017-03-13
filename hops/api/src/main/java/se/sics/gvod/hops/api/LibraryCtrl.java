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
package se.sics.gvod.hops.api;

import java.util.List;
import java.util.Map;
import se.sics.ktoolbox.util.identifiable.overlay.OverlayId;
import se.sics.ktoolbox.util.network.KAddress;
import se.sics.nstream.library.util.TorrentState;
import se.sics.nstream.storage.durable.util.MyStream;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public interface LibraryCtrl {
  public Map<OverlayId, Torrent> getTorrents();
  public boolean containsTorrent(OverlayId tId);
  public TorrentState stateOf(OverlayId tId);
  public void prepareUpload(Integer projectId, OverlayId torrentId, String torrentName);
  public void upload(OverlayId torrentId, MyStream manifestStream);
  public void prepareDownload(Integer projectId, OverlayId torrentId, String torrentName, List<KAddress> partners);
  public void download(OverlayId torrentId, MyStream manifestStream);
  public void finishDownload(OverlayId torrentId);
  public void killing(OverlayId torrentId);
  public void killed(OverlayId torrentId);
}
