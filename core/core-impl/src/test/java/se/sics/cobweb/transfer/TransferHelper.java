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
package se.sics.cobweb.transfer;

import com.google.common.base.Optional;
import se.sics.cobweb.MockNetwork;
import se.sics.kompics.Component;
import se.sics.kompics.Init;
import se.sics.kompics.network.Network;
import se.sics.kompics.testkit.TestContext;
import se.sics.kompics.testkit.Testkit;
import se.sics.ktoolbox.util.identifiable.overlay.OverlayId;
import se.sics.ktoolbox.util.network.KAddress;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class TransferHelper {

  public static TestContext<TransferComp> getContext(KAddress selfAdr, OverlayId torrentId, Optional torrent) {
    TransferComp.Init init = new TransferComp.Init(selfAdr, torrentId, torrent);
    TestContext<TransferComp> context = Testkit.newTestContext(TransferComp.class, init);
    return context;
  }

  public static void setNetwork(TestContext tc, Component leecher, Component seeder) {
    //cross connecting networks - shortcircuit
    Component seederNetwork = tc.create(MockNetwork.class, Init.NONE);
    Component leecherNetwork = tc.create(MockNetwork.class, Init.NONE);
    ((MockNetwork) seederNetwork.getComponent()).setPair((MockNetwork) leecherNetwork.getComponent());
    ((MockNetwork) leecherNetwork.getComponent()).setPair((MockNetwork) seederNetwork.getComponent());
    tc.connect(leecher.getNegative(Network.class), leecherNetwork.getPositive(Network.class));
    tc.connect(seeder.getNegative(Network.class), seederNetwork.getPositive(Network.class));
  }
}
