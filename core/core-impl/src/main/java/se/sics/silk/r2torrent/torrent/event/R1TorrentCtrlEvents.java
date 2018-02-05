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
package se.sics.silk.r2torrent.torrent.event;

import java.util.Set;
import se.sics.ktoolbox.util.identifiable.BasicIdentifiers;
import se.sics.ktoolbox.util.identifiable.overlay.OverlayId;
import se.sics.ktoolbox.util.network.KAddress;
import se.sics.silk.event.SilkEvent;
import se.sics.silk.r2torrent.torrent.R1Torrent;
import se.sics.silk.r2torrent.torrent.util.R1TorrentDetails;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class R1TorrentCtrlEvents {

  public static class Upload extends SilkEvent.E3 implements R1Torrent.CtrlEvent {

    public final R1TorrentDetails torrentDetails;

    public Upload(OverlayId torrentId, R1TorrentDetails torrentDetails) {
      super(BasicIdentifiers.eventId(), torrentId);
      this.torrentDetails = torrentDetails;
    }
  }

  public static class Download extends SilkEvent.E3 implements R1Torrent.CtrlEvent {

    public final R1TorrentDetails torrentDetails;
    public final Set<KAddress> bootstrap;

    public Download(OverlayId torrentId, R1TorrentDetails torrentDetails, Set<KAddress> bootstrap) {
      super(BasicIdentifiers.eventId(), torrentId);
      this.torrentDetails = torrentDetails;
      this.bootstrap = bootstrap;
    }
  }
}
