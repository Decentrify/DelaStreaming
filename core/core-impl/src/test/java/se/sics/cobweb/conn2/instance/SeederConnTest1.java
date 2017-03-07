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
//package se.sics.cobweb.conn2.instance;
//
//import se.sics.cobweb.conn2.instance.SeederConnStates;
//import se.sics.cobweb.conn2.instance.LeecherConnFSM;
//import se.sics.cobweb.conn2.instance.SeederConnFSM;
//import java.util.LinkedList;
//import java.util.List;
//import static org.junit.Assert.assertEquals;
//import org.junit.Before;
//import org.junit.Test;
//import se.sics.cobweb.conn2.ConnComp;
//import se.sics.cobweb.conn2.ConnHelper;
//import se.sics.cobweb.conn2.ConnId;
//import se.sics.cobweb.conn2.ConnPort;
//import se.sics.cobweb.conn2.instance.api.ConnectionDecider;
//import se.sics.cobweb.conn2.instance.event.ConnE;
//import se.sics.cobweb.conn2.instance.impl.ConnectionDeciderImpl;
//import se.sics.cobweb.conn2.instance.msg.TorrentMsg;
//import se.sics.fsm.helper.FSMHelper;
//import se.sics.kompics.Component;
//import se.sics.kompics.Port;
//import se.sics.kompics.network.Network;
//import se.sics.kompics.testkit.Direction;
//import se.sics.kompics.testkit.TestContext;
//import se.sics.ktoolbox.nutil.fsm.ids.FSMIdRegistry;
//import se.sics.ktoolbox.util.identifiable.overlay.OverlayId;
//import se.sics.ktoolbox.util.identifiable.overlay.OverlayIdFactory;
//import se.sics.ktoolbox.util.identifiable.overlay.OverlayRegistry;
//import se.sics.ktoolbox.util.network.KAddress;
//import se.sics.ktoolbox.util.network.basic.BasicContentMsg;
//
///**
// * @author Alex Ormenisan <aaor@kth.se>
// */
//public class SeederConnTest1 {
//
//  private static final Direction incoming = Direction.INCOMING;
//  private static final Direction outgoing = Direction.OUTGOING;
//
//  private KAddress seederAdr;
//  private KAddress leecherAdr;
//  private static OverlayIdFactory torrentIdFactory;
//  private TestContext<ConnComp> tc;
//  private Component seeder;
//  private Component leecher;
//  private OverlayId torrentId;
//
//  @Before
//  public void setup() {
//    FSMIdRegistry.reset();
//    OverlayRegistry.reset();
//    String[] fsmNames = new String[]{SeederConnFSM.NAME, LeecherConnFSM.NAME};
//    torrentIdFactory = FSMHelper.systemSetup("src/test/resources/conn/seeder1/application.conf", fsmNames);
//    seederAdr = FSMHelper.getAddress(0);
//    leecherAdr = FSMHelper.getAddress(1);
//    torrentId = torrentIdFactory.randomId();
//
//    ConnectionDecider.SeederSideConn seederDecider = new ConnectionDeciderImpl.SeederSideConn();
//    ConnectionDecider.LeecherSideConn leecherDecider = new ConnectionDeciderImpl.LeecherSideConn();
//
//    tc = ConnHelper.getContext(seederAdr, torrentId, seederDecider, leecherDecider);
//    seeder = tc.getComponentUnderTest();
//    leecher = tc.create(ConnComp.class, new ConnComp.Init(leecherAdr, torrentId, seederDecider, leecherDecider));
//    //mocks - transfer and network
//    ConnHelper.setSeederTransfer(tc, seeder, Transfer1.seederTransferScenario());
//    ConnHelper.setLeecherTransfer(tc, leecher, Transfer1.leecherTransferScenario());
//    ConnHelper.setNetwork(tc, leecher, seeder);
//    List<KAddress> peers = new LinkedList<>();
//    peers.add(seederAdr);
//    ConnHelper.setCroupier(tc, peers, leecher);
//  }
//
//  @Test
//  public void test() {
//    Port<Network> network = seeder.getNegative(Network.class);
//    Port<ConnPort> conn = seeder.getPositive(ConnPort.class);
//
//    ConnId connId = new ConnId(torrentId.baseId, leecherAdr.getId());
//    TestContext<ConnComp> atc = tc.body()
//      .expect(BasicContentMsg.class, FSMHelper.anyMsg(TorrentMsg.Connect.class), network, incoming)
//      .inspect(ConnHelper.inspectSeederState(connId, SeederConnStates.SETTING_UP))
//      .expect(ConnE.LeecherConnect.class, FSMHelper.anyEvent(ConnE.LeecherConnect.class), conn, outgoing)
//      .inspect(ConnHelper.inspectSeederState(connId, SeederConnStates.CONNECTED))
//      .expect(BasicContentMsg.class, FSMHelper.anyMsg(TorrentMsg.Connected.class), network, outgoing)
//      .repeat(1).body().end();
//    assertEquals(tc.check(), tc.getFinalState());
//  }
//}
