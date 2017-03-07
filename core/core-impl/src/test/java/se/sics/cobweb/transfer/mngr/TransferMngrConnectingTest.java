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
//package se.sics.cobweb.transfer.mngr;
//
//import java.util.LinkedList;
//import static org.junit.Assert.assertEquals;
//import org.junit.Before;
//import org.junit.Test;
//import se.sics.cobweb.TestHelper;
//import se.sics.cobweb.transfer.mngr.event.TransferCtrlE;
//import se.sics.cobweb.transfer.mngr.event.TransferMngrE;
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
//public class TransferMngrConnectingTest {
//  private static final Direction incoming = Direction.INCOMING;
//  private static final Direction outgoing = Direction.OUTGOING;
//
//  private KAddress selfAdr;
//  private static OverlayIdFactory torrentIdFactory;
//  private TestContext<TransferMngrComp> tc;
//  private Component transferMngr;
//  private Component driver;
//
//  @Before
//  public void setup() {
//    FSMIdRegistry.reset();
//    OverlayRegistry.reset();
//    String[] fsmNames = new String[]{TransferMngrFSM.NAME};
//    torrentIdFactory = FSMHelper.systemSetup("src/test/resources/conn/mngr/application.conf", fsmNames);
//    selfAdr = FSMHelper.getAddress(0);
//    tc = TransferMngrHelper.getContext(selfAdr);
//    transferMngr = tc.getComponentUnderTest();
//  }
//
//  @Test
//  public void simpleStartStop() {
//    LinkedList<KompicsEvent> transferMngrEvents = new LinkedList<>();
//    OverlayId torrentId1 = torrentIdFactory.id(new BasicBuilders.StringBuilder("0"));
//    OverlayId torrentId2 = torrentIdFactory.id(new BasicBuilders.StringBuilder("1"));
//    OverlayId torrentId3 = torrentIdFactory.id(new BasicBuilders.StringBuilder("2"));
//    transferMngrEvents.add(new TransferMngrE.SetupReq(torrentId1));
//    transferMngrEvents.add(new TransferMngrE.StartReq(torrentId1));
//    transferMngrEvents.add(new TransferMngrE.SetupReq(torrentId2));
//    transferMngrEvents.add(new TransferMngrE.StartReq(torrentId2));
//    transferMngrEvents.add(new TransferMngrE.StopReq(torrentId1));
//    transferMngrEvents.add(new TransferMngrE.SetupReq(torrentId3));
//    transferMngrEvents.add(new TransferMngrE.StartReq(torrentId3));
//    transferMngrEvents.add(new TransferMngrE.StopReq(torrentId2));
//    transferMngrEvents.add(new TransferMngrE.SetupReq(torrentId1));
//    transferMngrEvents.add(new TransferMngrE.StartReq(torrentId1));
//    transferMngrEvents.add(new TransferMngrE.StopReq(torrentId3));
//    transferMngrEvents.add(new TransferMngrE.StopReq(torrentId1));
//
//    driver = tc.create(TransferMngrDriver.class, new TransferMngrDriver.Init(transferMngrEvents));
//    tc.connect(transferMngr.getPositive(TransferMngrPort.class), driver.getNegative(TransferMngrPort.class));
//    tc.connect(transferMngr.getNegative(TransferCtrlPort.class), driver.getPositive(TransferCtrlPort.class));
//
//    Port<TransferMngrPort> transferMngrPort = transferMngr.getPositive(TransferMngrPort.class);
//
//    TestContext<TransferMngrComp> atc = tc.body();
//    atc = startConn(torrentId1, atc.repeat(1));
//    atc = startConn(torrentId2, atc.repeat(1));
//    atc = stopConn(torrentId1, atc.repeat(1));
//    atc = startConn(torrentId3, atc.repeat(1));
//    atc = stopConn(torrentId2, atc.repeat(1));
//    atc = startConn(torrentId1, atc.repeat(1));
//    atc = stopConn(torrentId3, atc.repeat(1));
//    atc = stopConn(torrentId1, atc.repeat(1));
//    atc = inspectEmptyState(atc.repeat(1));
//    atc = inspectEmptyState(atc.repeat(1));
//    atc.repeat(1).body().end();
//    assertEquals(tc.check(), tc.getFinalState());
//  }
//  
//  private TestContext<TransferMngrComp> startConn(OverlayId torrentId, TestContext<TransferMngrComp> tc) {
//    Port<TransferMngrPort> connMngrPort = transferMngr.getPositive(TransferMngrPort.class);
//    TestContext<TransferMngrComp> atc = tc.body()
//      .expect(TransferMngrE.SetupReq.class, TestHelper.anyPredicate(TransferMngrE.SetupReq.class), connMngrPort, incoming)
//      .inspect(TransferMngrHelper.inspectState(torrentId, TransferMngrStates.SETUP))
//      .expect(TransferMngrE.StartReq.class, TestHelper.anyPredicate(TransferMngrE.StartReq.class), connMngrPort, incoming)
//      .inspect(TransferMngrHelper.inspectState(torrentId, TransferMngrStates.ACTIVE))
//    .end();
//    return atc;
//  }
//  
//  private TestContext<TransferMngrComp> stopConn(OverlayId torrentId, TestContext<TransferMngrComp> tc) {
//    Port<TransferMngrPort> transferMngrPort = transferMngr.getPositive(TransferMngrPort.class);
//    Port<TransferCtrlPort> transferCtrlPort = transferMngr.getNegative(TransferCtrlPort.class);
//    TestContext<TransferMngrComp> atc = tc.body()
//      .expect(TransferMngrE.StopReq.class, TestHelper.anyPredicate(TransferMngrE.StopReq.class), transferMngrPort, incoming)
//      .expect(TransferCtrlE.CleanReq.class, TestHelper.anyPredicate(TransferCtrlE.CleanReq.class), transferCtrlPort, outgoing)
//    .end();
//    return atc;
//  }
//  
//  private TestContext<TransferMngrComp> inspectEmptyState(TestContext<TransferMngrComp> tc) {
//    TestContext<TransferMngrComp> atc = tc.body()
//      .inspect(TransferMngrHelper.emptyState())
//    .end();
//    return atc;
//  }
//}