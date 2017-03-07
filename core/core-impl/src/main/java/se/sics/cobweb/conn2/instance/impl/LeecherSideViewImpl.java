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
package se.sics.cobweb.conn2.instance.impl;

import se.sics.cobweb.conn2.instance.api.ConnectionState;
import se.sics.cobweb.conn2.instance.api.LeecherSideView;
import se.sics.cobweb.conn2.instance.api.SeederState;
import se.sics.cobweb.conn2.instance.api.TorrentState;
import se.sics.ktoolbox.util.identifiable.overlay.OverlayId;
import se.sics.ktoolbox.util.network.KAddress;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class LeecherSideViewImpl implements LeecherSideView {
  public SeederState getSeeder(KAddress seeder) {
    return new SeederState(seeder);
  }
  
  public TorrentState getTorrent(OverlayId torrentId) {
    return new TorrentState(torrentId);
  }
  
  public void setConnection(ConnectionState state, SeederState leecher, TorrentState torrent) {
  }
}
