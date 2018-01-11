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
package se.sics.silk.r2torrent.conn;

import se.sics.kompics.Port;
import se.sics.kompics.testing.Direction;
import se.sics.kompics.testing.TestContext;
import se.sics.kompics.util.Identifier;
import se.sics.ktoolbox.util.identifiable.overlay.OverlayId;
import se.sics.ktoolbox.util.network.KAddress;
import se.sics.silk.r2torrent.conn.event.R1TorrentSeederEvents;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class R1TorrentSeederHelper {
  public static TestContext torrentSeederConnReq(TestContext tc, Port triggerP, R1TorrentSeederEvents.ConnectReq req) {
    return tc.trigger(req, triggerP);
  }
  
  public static TestContext torrentSeederConnSucc(TestContext tc, Port expectP) {
    return tc.expect(R1TorrentSeederEvents.ConnectSucc.class, expectP, Direction.OUT);
  }
  
  public static TestContext torrentSeederConnFail(TestContext tc, Port expectP) {
    return tc.expect(R1TorrentSeederEvents.ConnectFail.class, expectP, Direction.OUT);
  }
  
  public static TestContext torrentSeederDisconnect(TestContext tc, Port triggerP, R1TorrentSeederEvents.Disconnect req) {
    return tc.trigger(req, triggerP);
  }
  
  public static R1TorrentSeederEvents.ConnectReq torrentSeederConnReq(OverlayId torrentId, Identifier fileId, KAddress seeder) {
    return new R1TorrentSeederEvents.ConnectReq(torrentId, fileId, seeder);
  }
  
  public static R1TorrentSeederEvents.Disconnect torrentSeederDisconnect(OverlayId torrentId, Identifier fileId, Identifier seederId) {
    return new R1TorrentSeederEvents.Disconnect(torrentId, fileId, seederId);
  }
}
