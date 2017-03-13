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

import com.google.common.base.Optional;
import java.util.LinkedList;
import java.util.List;
import se.sics.ktoolbox.util.network.KAddress;
import se.sics.nstream.library.util.TorrentState;
import se.sics.nstream.storage.durable.util.MyStream;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class Torrent {

  public final Integer projectId;
  public final Integer datasetId;
  public final String torrentName;
  private TorrentState torrentStatus = TorrentState.NONE;
  private Optional<MyStream> manifestStream = Optional.absent();
  private List<KAddress> partners = new LinkedList<>();

  public Torrent(Integer projectId, Integer datasetId, String torrentName, TorrentState torrentStatus) {
    this.projectId = projectId;
    this.datasetId = datasetId;
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
