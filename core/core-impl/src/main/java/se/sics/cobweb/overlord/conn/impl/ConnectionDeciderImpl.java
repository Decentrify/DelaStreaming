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
package se.sics.cobweb.overlord.conn.impl;

import java.util.Set;
import se.sics.cobweb.overlord.conn.api.ConnectionDecider;
import se.sics.cobweb.overlord.conn.api.ConnectionState;
import se.sics.cobweb.overlord.conn.api.LeecherState;
import se.sics.cobweb.overlord.conn.api.LocalLeechersView;
import se.sics.cobweb.overlord.conn.api.LocalSeedersView;
import se.sics.cobweb.overlord.conn.api.SeederState;
import se.sics.cobweb.overlord.conn.api.TorrentState;
import se.sics.cobweb.util.HandleId;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class ConnectionDeciderImpl {

  public static class SeederSide implements ConnectionDecider.SeederSide {

    @Override
    public ConnectionState canConnect(LocalSeedersView seedersView, LocalLeechersView leechersView, LeecherState leecher,
      TorrentState torrent) {
      return ConnectionState.CONNECT;
    }
  }

  public static class LeecherSide implements ConnectionDecider.LeecherSide {

    @Override
    public Set<HandleId> canConnect(LocalSeedersView seedersView, LocalLeechersView leechersView, SeederState seeder,
      TorrentState torrent) {
      Set<HandleId> interestedLeechers = leechersView.interested(seeder, torrent);
      return interestedLeechers;
    }
  }
}
