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
package se.sics.silk.r2torrent.conn.helper;

import se.sics.kompics.Port;
import se.sics.kompics.testing.Direction;
import se.sics.kompics.testing.TestContext;
import se.sics.kompics.util.Identifier;
import se.sics.ktoolbox.util.identifiable.overlay.OverlayId;
import se.sics.ktoolbox.util.network.KAddress;
import se.sics.silk.r2torrent.conn.event.R1TorrentLeecherEvents;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class R1TorrentLeecherHelper {
  public static TestContext torrentLeecherConnReq(TestContext tc, Port triggerP, R1TorrentLeecherEvents.ConnectReq req) {
    return tc.trigger(req, triggerP);
  }
  
  public static TestContext torrentLeecherConnSucc(TestContext tc, Port expectP) {
    return tc.expect(R1TorrentLeecherEvents.ConnectSucc.class, expectP, Direction.OUT);
  }
  
  public static TestContext torrentLeecherConnFail(TestContext tc, Port expectP) {
    return tc.expect(R1TorrentLeecherEvents.ConnectFail.class, expectP, Direction.OUT);
  }
  
  public static TestContext torrentLeecherDisconnect(TestContext tc, Port triggerP, OverlayId torrentId, Identifier fileId, KAddress leecher) {
    R1TorrentLeecherEvents.Disconnect req = new R1TorrentLeecherEvents.Disconnect(torrentId, fileId, leecher.getId());
    return tc.trigger(req, triggerP);
  }
  
  public static R1TorrentLeecherEvents.ConnectReq torrentLeecherConnReq(OverlayId torrentId, Identifier fileId, KAddress leecher) {
    return new R1TorrentLeecherEvents.ConnectReq(torrentId, fileId, leecher);
  }
  
  public static R1TorrentLeecherEvents.Disconnect torrentLeecherDisconnect(OverlayId torrentId, Identifier fileId, Identifier leecherId) {
    return new R1TorrentLeecherEvents.Disconnect(torrentId, fileId, leecherId);
  }
}
