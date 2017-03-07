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
package se.sics.cobweb.ports;

import org.javatuples.Triplet;
import se.sics.cobweb.transfer.handlemngr.MockHandleEvent;
import se.sics.cobweb.util.HandleId;
import se.sics.kompics.network.Network;
import se.sics.kompics.network.Transport;
import se.sics.ktoolbox.util.identifiable.overlay.OverlayId;
import se.sics.ktoolbox.util.network.KAddress;
import se.sics.ktoolbox.util.network.basic.BasicContentMsg;
import se.sics.ktoolbox.util.network.basic.BasicHeader;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class NetworkPortHelper {

  public static Triplet mockMsg(OverlayId torrentId, HandleId handleId, KAddress src, KAddress dst) {
    MockHandleEvent e = new MockHandleEvent(torrentId, handleId);
    BasicHeader h = new BasicHeader(src, dst, Transport.UDP);
    BasicContentMsg msg = new BasicContentMsg(h, e);
    Triplet result = Triplet.with(msg, true, Network.class);
    return result;
  }
}
