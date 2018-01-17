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
//package se.sics.silk.r2torrent.pair;
//
//import org.junit.After;
//import static org.junit.Assert.assertTrue;
//import org.junit.Before;
//import org.junit.BeforeClass;
//import org.junit.Test;
//import se.sics.kompics.Component;
//import se.sics.kompics.Port;
//import se.sics.kompics.fsm.FSMException;
//import se.sics.kompics.testing.TestContext;
//import se.sics.ktoolbox.util.identifiable.BasicBuilders;
//import se.sics.ktoolbox.util.identifiable.overlay.OverlayId;
//import se.sics.ktoolbox.util.identifiable.overlay.OverlayIdFactory;
//import se.sics.ktoolbox.util.network.KAddress;
//import se.sics.silk.SystemHelper;
//import se.sics.silk.SystemSetup;
//import se.sics.silk.r2torrent.R2TorrentPort;
//import se.sics.silk.r2torrent.pair.helper.MockTorrentCtrlPort;
//import se.sics.silk.r2torrent.pair.helper.R2TorrentWrapperComp;
//
///**
// * @author Alex Ormenisan <aaor@kth.se>
// */
//public class R2TorrentWrapperTest {
//  private TestContext<R2TorrentWrapperComp> tc;
//  private Component comp;
//  private Port<MockTorrentCtrlPort> ctrlP;
//  private Port<R2TorrentPort> leecherP;
//  private static OverlayIdFactory torrentIdFactory;
//  private KAddress seeder;
//  private KAddress leecher;
//
//  @BeforeClass
//  public static void setup() throws FSMException {
//    torrentIdFactory = SystemSetup.systemSetup("src/test/resources/application.conf");
//  }
//
//  @Before
//  public void testSetup() {
//    tc = getContext();
//    comp = tc.getComponentUnderTest();
//    ctrlP = comp.getPositive(MockTorrentCtrlPort.class);
//  }
//
//  private TestContext<R2TorrentWrapperComp> getContext() {
//    seeder = SystemHelper.getAddress(0);
//    leecher = SystemHelper.getAddress(1);
//    R2TorrentWrapperComp.Init init = new R2TorrentWrapperComp.Init(seeder, leecher);
//    TestContext<R2TorrentWrapperComp> context = TestContext.newInstance(R2TorrentWrapperComp.class, init);
//    return context;
//  }
//
//  @After
//  public void clean() {
//  }
//  
//  @Test
//  public void testEmpty() {
//    tc = tc.body();
//    tc.repeat(1).body().end();
//    assertTrue(tc.check());
//  }
//
//  @Test
//  public void testSimple() {
//    OverlayId torrent = torrentIdFactory.id(new BasicBuilders.IntBuilder(1));
//    KAddress seeder = SystemHelper.getAddress(1);
//
//    
////    tc = tc.body();
////    tc = ctrlMetadataGetReq(tc, ctrlP, torrent, seeder); //1
////    tc = mngrMetaGetReq(tc, expectP); //2
////    tc = torrentSeederConnReq(tc, expectP); //3
////    tc = nodeSeederConnReqLoc(tc, expectP); //4
////    tc = netNodeConnAcc(tc, networkP); //5-6
////    tc = timerSchedulePeriodicTimeout(tc, timerP);//7
////    tc = nodeSeederConnSucc(tc, expectP); //8
////    tc = torrentSeederConnSucc(tc, expectP); //9
////    tc
////      .expect(R2NodeSeederEvents.ConnectSucc.class, expectP, Direction.OUT)
////      .expect(R1MetadataEvents.MetaGetSucc.class, expectP, Direction.OUT)
////      .expect(R2TorrentCtrlEvents.MetaGetSucc.class, ctrlP, Direction.OUT)
////      .trigger(ctrlDownload(torrent), ctrlP)
////      .expect(R1HashEvents.HashReq.class, expectP, Direction.OUT)
////      .expect(R1HashEvents.HashSucc.class, expectP, Direction.OUT)
////      .expect(R2TorrentCtrlEvents.TorrentBaseInfo.class, ctrlP, Direction.OUT);
//    tc.repeat(1).body().end();
//    assertTrue(tc.check());
//  }
//}
