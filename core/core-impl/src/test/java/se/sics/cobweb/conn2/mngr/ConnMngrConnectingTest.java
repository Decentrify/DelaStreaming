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
//package se.sics.cobweb.conn2.mngr;
//
//import se.sics.cobweb.conn2.mngr.ConnMngrComp;
//import se.sics.cobweb.conn2.mngr.ConnMngrStates;
//import se.sics.cobweb.conn2.mngr.ConnMngrFSM;
//import se.sics.cobweb.conn2.mngr.ConnCtrlPort;
//import se.sics.cobweb.conn2.mngr.ConnMngrPort;
//import java.util.LinkedList;
//import static org.junit.Assert.assertEquals;
//import org.junit.Before;
//import org.junit.Test;
//import se.sics.cobweb.TestHelper;
//import se.sics.cobweb.conn2.mngr.event.ConnCtrlE;
//import se.sics.cobweb.conn2.mngr.event.ConnMngrE;
//import se.sics.fsm.helper.FSMHelper;
//import se.sics.kompics.Component;
//import se.sics.kompics.KompicsEvent;
//import se.sics.kompics.Port;
//import se.sics.kompics.testkit.Direction;
//import se.sics.kompics.testkit.TestContext;
//import se.sics.ktoolbox.nutil.fsm.ids.FSMIdRegistry;
//import se.sics.ktoolbox.util.identifiable.BasicBuilders;
//import se.sics.ktoolbox.util.identifiable.overlay.OverlayId;
//import se.sics.ktoolbox.util.identifiable.overlay.OverlayIdFactory;
//import se.sics.ktoolbox.util.identifiable.overlay.OverlayRegistry;
//import se.sics.ktoolbox.util.network.KAddress;
//
///**
// * @author Alex Ormenisan <aaor@kth.se>
// */
//public class ConnMngrConnectingTest {
//
//  private static final Direction incoming = Direction.INCOMING;
//  private static final Direction outgoing = Direction.OUTGOING;
//
//  private KAddress selfAdr;
//  private static OverlayIdFactory torrentIdFactory;
//  private TestContext<ConnMngrComp> tc;
//  private Component connMngr;
//  private Component driver;
//
//  @Before
//  public void setup() {
//    FSMIdRegistry.reset();
//    OverlayRegistry.reset();
//    String[] fsmNames = new String[]{ConnMngrFSM.NAME};
//    torrentIdFactory = FSMHelper.systemSetup("src/test/resources/conn/mngr/application.conf", fsmNames);
//    selfAdr = FSMHelper.getAddress(0);
//    tc = ConnMngrHelper.getContext(selfAdr);
//    connMngr = tc.getComponentUnderTest();
//  }
//
//  @Test
//  public void simpleStartStop() {
//    LinkedList<KompicsEvent> connMngrEvents = new LinkedList<>();
//    OverlayId torrentId1 = torrentIdFactory.id(new BasicBuilders.StringBuilder("0"));
//    OverlayId torrentId2 = torrentIdFactory.id(new BasicBuilders.StringBuilder("1"));
//    OverlayId torrentId3 = torrentIdFactory.id(new BasicBuilders.StringBuilder("2"));
//    connMngrEvents.add(new ConnMngrE.SetupReq(torrentId1));
//    connMngrEvents.add(new ConnMngrE.StartReq(torrentId1));
//    connMngrEvents.add(new ConnMngrE.SetupReq(torrentId2));
//    connMngrEvents.add(new ConnMngrE.StartReq(torrentId2));
//    connMngrEvents.add(new ConnMngrE.StopReq(torrentId1));
//    connMngrEvents.add(new ConnMngrE.SetupReq(torrentId3));
//    connMngrEvents.add(new ConnMngrE.StartReq(torrentId3));
//    connMngrEvents.add(new ConnMngrE.StopReq(torrentId2));
//    connMngrEvents.add(new ConnMngrE.SetupReq(torrentId1));
//    connMngrEvents.add(new ConnMngrE.StartReq(torrentId1));
//    connMngrEvents.add(new ConnMngrE.StopReq(torrentId3));
//    connMngrEvents.add(new ConnMngrE.StopReq(torrentId1));
//
//    driver = tc.create(ConnMngrDriver.class, new ConnMngrDriver.Init(connMngrEvents));
//    tc.connect(connMngr.getPositive(ConnMngrPort.class), driver.getNegative(ConnMngrPort.class));
//    tc.connect(connMngr.getNegative(ConnCtrlPort.class), driver.getPositive(ConnCtrlPort.class));
//
//    Port<ConnMngrPort> connMngrPort = connMngr.getPositive(ConnMngrPort.class);
//
//    TestContext<ConnMngrComp> atc = tc.body();
//    atc = startConn(torrentId1, atc.repeat(1));
//    atc = startConn(torrentId2, atc.repeat(1));
//    atc = stopConn(torrentId1, atc.repeat(1));
//    atc = startConn(torrentId3, atc.repeat(1));
//    atc = stopConn(torrentId2, atc.repeat(1));
//    atc = startConn(torrentId1, atc.repeat(1));
//    atc = stopConn(torrentId3, atc.repeat(1));
//    atc = stopConn(torrentId1, atc.repeat(1));
//    atc = inspectEmptyState(atc.repeat(1));
//    atc.repeat(1).body().end();
//    assertEquals(tc.check(), tc.getFinalState());
//  }
//  
//  private TestContext<ConnMngrComp> startConn(OverlayId torrentId, TestContext<ConnMngrComp> tc) {
//    Port<ConnMngrPort> connMngrPort = connMngr.getPositive(ConnMngrPort.class);
//    TestContext<ConnMngrComp> atc = tc.body()
//      .expect(ConnMngrE.SetupReq.class, TestHelper.anyPredicate(ConnMngrE.SetupReq.class), connMngrPort, incoming)
//      .inspect(ConnMngrHelper.inspectState(torrentId, ConnMngrStates.SETUP))
//      .expect(ConnMngrE.StartReq.class, TestHelper.anyPredicate(ConnMngrE.StartReq.class), connMngrPort, incoming)
//      .inspect(ConnMngrHelper.inspectState(torrentId, ConnMngrStates.ACTIVE))
//    .end();
//    return atc;
//  }
//  
//  private TestContext<ConnMngrComp> stopConn(OverlayId torrentId, TestContext<ConnMngrComp> tc) {
//    Port<ConnMngrPort> connMngrPort = connMngr.getPositive(ConnMngrPort.class);
//    Port<ConnCtrlPort> connCtrlPort = connMngr.getNegative(ConnCtrlPort.class);
//    TestContext<ConnMngrComp> atc = tc.body()
//      .expect(ConnMngrE.StopReq.class, TestHelper.anyPredicate(ConnMngrE.StopReq.class), connMngrPort, incoming)
//      .expect(ConnCtrlE.CleanReq.class, TestHelper.anyPredicate(ConnCtrlE.CleanReq.class), connCtrlPort, outgoing)
//    .end();
//    return atc;
//  }
//  
//  private TestContext<ConnMngrComp> inspectEmptyState(TestContext<ConnMngrComp> tc) {
//    TestContext<ConnMngrComp> atc = tc.body()
//      .inspect(ConnMngrHelper.emptyState())
//    .end();
//    return atc;
//  }
//}
