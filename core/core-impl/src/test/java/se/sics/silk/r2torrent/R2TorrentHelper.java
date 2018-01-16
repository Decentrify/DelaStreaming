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
package se.sics.silk.r2torrent;

import java.util.LinkedList;
import java.util.List;
import se.sics.kompics.Port;
import se.sics.kompics.testing.TestContext;
import se.sics.ktoolbox.util.identifiable.overlay.OverlayId;
import se.sics.ktoolbox.util.network.KAddress;
import se.sics.silk.r2torrent.event.R2TorrentCtrlEvents;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class R2TorrentHelper {
  public static TestContext ctrlMetadataGetReq(TestContext tc, Port ctrlP, OverlayId torrentId, KAddress seeder) {
    List<KAddress> seeders = new LinkedList<>();
    seeders.add(seeder);
    R2TorrentCtrlEvents.MetaGetReq r = new R2TorrentCtrlEvents.MetaGetReq(torrentId, seeders);
    return tc.trigger(r, ctrlP);
  }
}
