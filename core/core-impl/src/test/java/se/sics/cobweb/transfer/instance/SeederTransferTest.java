///*
// * Copyright (C) 2009 Swedish Institute of Computer Science (SICS) Copyright (C)
// * 2009 Royal Institute of Technology (KTH)
// *
// * GVoD is free software; you can redistribute it and/or
// * modify it under the terms of the GNU General Public License
// * as published by the Free Software Foundation; either version 2
// * of the License, or (at your option) any later version.
// *
// * This program is distributed in the hope that it will be useful,
// * but WITHOUT ANY WARRANTY; without even the implied warranty of
// * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// * GNU General Public License for more details.
// *
// * You should have received a copy of the GNU General Public License
// * along with this program; if not, write to the Free Software
// * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
// */
//package se.sics.cobweb.transfer.instance;
//
//import static org.junit.Assert.assertEquals;
//import org.junit.Before;
//import org.junit.Test;
//import se.sics.cobweb.conn2.ConnPort;
//import se.sics.cobweb.conn2.instance.event.ConnE;
//import se.sics.cobweb.transfer.TransferComp;
//import se.sics.cobweb.transfer.TransferHelper;
//import se.sics.cobweb.transfer.mngr.TransferCtrlPort;
//import se.sics.cobweb.transfer.mngr.event.TransferCtrlE;
//import se.sics.fsm.helper.FSMHelper;
//import se.sics.kompics.Component;
//import se.sics.kompics.Port;
//import se.sics.kompics.network.Network;
//import se.sics.kompics.testkit.Direction;
//import se.sics.kompics.testkit.TestContext;
//import se.sics.ktoolbox.nutil.fsm.ids.FSMIdRegistry;
//import se.sics.ktoolbox.util.Either;
//import se.sics.ktoolbox.util.identifiable.overlay.OverlayId;
//import se.sics.ktoolbox.util.identifiable.overlay.OverlayIdFactory;
//import se.sics.ktoolbox.util.identifiable.overlay.OverlayRegistry;
//import se.sics.ktoolbox.util.network.KAddress;
//import se.sics.nstream.transfer.MyTorrent;
//import se.sics.nstream.util.BlockDetails;
//
///**
// * @author Alex Ormenisan <aaor@kth.se>
// */
//public class SeederTransferTest {
//  private static final Direction incoming = Direction.INCOMING;
//  private static final Direction outgoing = Direction.OUTGOING;
//
//  private KAddress seederAdr;
//  private KAddress leecherAdr;
//  private static OverlayIdFactory torrentIdFactory;
//  private TestContext<TransferComp> tc;
//  private Component seeder;
//  private Component leecher;
//  private OverlayId torrentId;
//
//  @Before
//  public void setup() {
//    FSMIdRegistry.reset();
//    OverlayRegistry.reset();
//    String[] fsmNames = new String[]{SeederTransferFSM.NAME, LeecherTransferFSM.NAME};
//    torrentIdFactory = FSMHelper.systemSetup("src/test/resources/transfer/seeder1/application.conf", fsmNames);
//    seederAdr = FSMHelper.getAddress(0);
//    leecherAdr = FSMHelper.getAddress(1);
//    torrentId = torrentIdFactory.randomId();
//    MyTorrent.ManifestDef manifestDef = new MyTorrent.ManifestDef(5, new BlockDetails(1000, 2, 1024, 200));
//    MyTorrent torrent = null;
//    
//    tc = TransferHelper.getContext(seederAdr, torrentId, Either.right(torrent));
//    seeder = tc.getComponentUnderTest();
//    leecher = tc.create(TransferComp.class, new TransferComp.Init(leecherAdr, torrentId, Either.left(manifestDef)));
//    TransferHelper.setNetwork(tc, leecher, seeder);
//  }
//
//  @Test
//  public void test() {
//    Port<TransferCtrlPort> transferCtrl = seeder.getPositive(TransferCtrlPort.class);
//    Port<ConnPort> conn = seeder.getNegative(ConnPort.class);
//    Port<Network> network = seeder.getNegative(Network.class);
//    
//    TestContext<TransferComp> atc = tc.body()
//      .trigger(seederConnected(), conn)
//      .inspect(TransferHelper.inspectSeederState(torrentId, SeederTransferStates.ACTIVE))
//      .trigger(seederConnected(), conn)
//      .inspect(TransferHelper.inspectSeederState(torrentId, SeederTransferStates.ACTIVE))
//      .repeat(1).body().end();
//    assertEquals(tc.check(), tc.getFinalState());
//  }
//  
//    public TransferCtrlE.Start start() {
//    return new TransferCtrlE.Start(torrentId);
//  }
//  
//  public ConnE.LeecherConnect seederConnected() {
//    return new ConnE.LeecherConnect(torrentId, leecherAdr);
//  }
//}
