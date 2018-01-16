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
package se.sics.silk.r2torrent.conn.test;

import java.util.Random;
import org.junit.After;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import se.sics.kompics.Component;
import se.sics.kompics.Port;
import se.sics.kompics.fsm.FSMException;
import se.sics.kompics.testing.Future;
import se.sics.kompics.testing.TestContext;
import se.sics.kompics.util.Identifier;
import se.sics.ktoolbox.util.identifiable.BasicBuilders;
import se.sics.ktoolbox.util.identifiable.basic.IntIdFactory;
import se.sics.ktoolbox.util.identifiable.overlay.OverlayId;
import se.sics.ktoolbox.util.identifiable.overlay.OverlayIdFactory;
import se.sics.ktoolbox.util.network.KAddress;
import se.sics.silk.SystemHelper;
import se.sics.silk.SystemSetup;
import se.sics.silk.r2torrent.R2TorrentPort;
import se.sics.silk.r2torrent.conn.R1TorrentLeecher.States;
import se.sics.silk.r2torrent.conn.helper.R1TorrentLeecherAuxComp;
import static se.sics.silk.r2torrent.conn.helper.R1TorrentLeecherHelper.torrentLeecherConnFail;
import static se.sics.silk.r2torrent.conn.helper.R1TorrentLeecherHelper.torrentLeecherConnReq;
import static se.sics.silk.r2torrent.conn.helper.R1TorrentLeecherHelper.torrentLeecherConnSucc;
import static se.sics.silk.r2torrent.conn.helper.R1TorrentLeecherHelper.torrentLeecherDisconnect;
import static se.sics.silk.r2torrent.conn.helper.R2NodeLeecherHelper.nodeConnFailLoc;
import static se.sics.silk.r2torrent.conn.helper.R2NodeLeecherHelper.nodeConnSuccLoc;
import static se.sics.silk.r2torrent.conn.helper.R2NodeLeecherHelper.nodeLeecherConnFailLoc;
import static se.sics.silk.r2torrent.conn.helper.R2NodeLeecherHelper.nodeLeecherConnReqLoc;
import static se.sics.silk.r2torrent.conn.helper.R2NodeLeecherHelper.nodeLeecherConnSuccLoc;
import static se.sics.silk.r2torrent.conn.helper.R2NodeLeecherHelper.nodeLeecherDisconnectLoc;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class R1TorrentLeecherTest {

  private TestContext<R1TorrentLeecherAuxComp> tc;
  private Component comp;
  private R1TorrentLeecherAuxComp compState;
  private Port<R2TorrentPort> triggerP;
  private Port<R2TorrentPort> expectP;
  private static OverlayIdFactory torrentIdFactory;
  private IntIdFactory intIdFactory;
  private KAddress selfAdr;

  @BeforeClass
  public static void setup() throws FSMException {
    torrentIdFactory = SystemSetup.systemSetup("src/test/resources/application.conf");
  }

  @Before
  public void testSetup() {
    tc = getContext();
    comp = tc.getComponentUnderTest();
    compState = (R1TorrentLeecherAuxComp) comp.getComponent();
    triggerP = comp.getNegative(R2TorrentPort.class);
    expectP = comp.getPositive(R2TorrentPort.class);
    intIdFactory = new IntIdFactory(new Random());
  }

  private TestContext<R1TorrentLeecherAuxComp> getContext() {
    selfAdr = SystemHelper.getAddress(0);
    R1TorrentLeecherAuxComp.Init init = new R1TorrentLeecherAuxComp.Init(selfAdr);
    TestContext<R1TorrentLeecherAuxComp> context = TestContext.newInstance(R1TorrentLeecherAuxComp.class, init);
    return context;
  }

  @After
  public void clean() {
  }

  @Test
  public void testEmpty() {
    tc = tc.body();
    tc.repeat(1).body().end();
    assertTrue(tc.check());
  }

  //*********************************************CONNECT TO CONNECT*****************************************************
  @Test
  public void testNewConnect1() {
    OverlayId torrent = torrentIdFactory.id(new BasicBuilders.IntBuilder(1));
    KAddress leecher = SystemHelper.getAddress(1);
    Identifier file1 = intIdFactory.id(new BasicBuilders.IntBuilder(1));
    Identifier file2 = intIdFactory.id(new BasicBuilders.IntBuilder(2));

    tc = tc.body();
    tc = torrentLeecherConnReq(tc, triggerP, torrentLeecherConnReq(torrent, file1, leecher));//2
    tc = nodeLeecherConnReqLoc(tc, expectP);//3
    tc = torrentLeecherConnReq(tc, triggerP, torrentLeecherConnReq(torrent, file2, leecher));//4
    tc.repeat(1).body().end();
    assertTrue(tc.check());
    assertEquals(States.CONNECT, compState.leecherState(torrent, leecher));
  }

  @Test
  public void testDisconnect1() {
    OverlayId torrent = torrentIdFactory.id(new BasicBuilders.IntBuilder(1));
    KAddress leecher = SystemHelper.getAddress(1);
    Identifier file1 = intIdFactory.id(new BasicBuilders.IntBuilder(1));
    Identifier file2 = intIdFactory.id(new BasicBuilders.IntBuilder(2));

    tc = tc.body();
    tc = torrentLeecherConnReq(tc, triggerP, torrentLeecherConnReq(torrent, file1, leecher));//2
    tc = nodeLeecherConnReqLoc(tc, expectP);//3
    tc = torrentLeecherConnReq(tc, triggerP, torrentLeecherConnReq(torrent, file2, leecher)); //4
    tc = torrentLeecherDisconnect(tc, triggerP, torrent, file2, leecher);//5
    tc.repeat(1).body().end();
    assertTrue(tc.check());
    assertEquals(States.CONNECT, compState.leecherState(torrent, leecher));
  }

  //********************************************CONNECT TO CONNECTED****************************************************

  @Test
  public void testConnected() {
    OverlayId torrent = torrentIdFactory.id(new BasicBuilders.IntBuilder(1));
    KAddress leecher = SystemHelper.getAddress(1);
    Identifier file1 = intIdFactory.id(new BasicBuilders.IntBuilder(1));

    tc = tc.body();
    tc = connected(tc, torrent, file1, leecher); //2-5
    tc.repeat(1).body().end();
    assertTrue(tc.check());
    assertEquals(States.CONNECTED, compState.leecherState(torrent, leecher));
  }

  private TestContext connected(TestContext tc, OverlayId torrentId, Identifier fileId, KAddress leecher) {
    tc = torrentLeecherConnReq(tc, triggerP, torrentLeecherConnReq(torrentId, fileId, leecher));//1 
    tc = nodeLeecherConnSuccLoc(tc, expectP, triggerP);//2-3
    tc = torrentLeecherConnSucc(tc, expectP);//4
    return tc;
  }

  //*******************************************CONNECTED TO CONNECTED***************************************************

  @Test
  public void testNewConnect2() {
    OverlayId torrent = torrentIdFactory.id(new BasicBuilders.IntBuilder(1));
    KAddress leecher = SystemHelper.getAddress(1);
    Identifier file1 = intIdFactory.id(new BasicBuilders.IntBuilder(1));
    Identifier file2 = intIdFactory.id(new BasicBuilders.IntBuilder(2));

    tc = tc.body();
    tc = connected(tc, torrent, file1, leecher); //2-5
    tc = torrentLeecherConnReq(tc, triggerP, torrentLeecherConnReq(torrent, file2, leecher));//6
    tc = torrentLeecherConnSucc(tc, expectP);//7
    tc.repeat(1).body().end();
    assertTrue(tc.check());
    assertEquals(States.CONNECTED, compState.leecherState(torrent, leecher));
  }

  @Test
  public void testDisconnect2() {
    OverlayId torrent = torrentIdFactory.id(new BasicBuilders.IntBuilder(1));
    KAddress leecher = SystemHelper.getAddress(1);
    Identifier file1 = intIdFactory.id(new BasicBuilders.IntBuilder(1));
    Identifier file2 = intIdFactory.id(new BasicBuilders.IntBuilder(2));

    tc = tc.body();
    tc = connected(tc, torrent, file1, leecher); //2-5
    tc = torrentLeecherConnReq(tc, triggerP, torrentLeecherConnReq(torrent, file2, leecher)); //6
    tc = torrentLeecherConnSucc(tc, expectP);//7
    tc = torrentLeecherDisconnect(tc, triggerP, torrent, file2, leecher);//8
    tc.repeat(1).body().end();
    assertTrue(tc.check());
    assertEquals(States.CONNECTED, compState.leecherState(torrent, leecher));
  }

  //*********************************************START TO CONNECTED*****************************************************

  @Test
  public void testNewConnect3() {
    OverlayId torrent = torrentIdFactory.id(new BasicBuilders.IntBuilder(1));
    KAddress leecher = SystemHelper.getAddress(1);
    Identifier file1 = intIdFactory.id(new BasicBuilders.IntBuilder(1));
    Identifier file2 = intIdFactory.id(new BasicBuilders.IntBuilder(2));

    Future nodeConnSucc = nodeConnSuccLoc();
    tc = tc.body();
    tc = torrentLeecherConnReq(tc, triggerP, torrentLeecherConnReq(torrent, file1, leecher));//2
    tc = nodeLeecherConnReqLoc(tc, expectP, nodeConnSucc);//3
    tc = torrentLeecherConnReq(tc, triggerP, torrentLeecherConnReq(torrent, file2, leecher));//4
    tc = nodeLeecherConnSuccLoc(tc, triggerP, nodeConnSucc); //5
    tc = tc.unordered();
    tc = torrentLeecherConnSucc(tc, expectP);//6
    tc = torrentLeecherConnSucc(tc, expectP);//6
    tc = tc.end();
    tc.repeat(1).body().end();
    assertTrue(tc.check());
    assertEquals(States.CONNECTED, compState.leecherState(torrent, leecher));
  }

  //********************************************CONNECT TO DISCONNECT***************************************************

  @Test
  public void testConnFail1() {
    OverlayId torrent = torrentIdFactory.id(new BasicBuilders.IntBuilder(1));
    KAddress leecher = SystemHelper.getAddress(1);
    Identifier file1 = intIdFactory.id(new BasicBuilders.IntBuilder(1));
    Identifier file2 = intIdFactory.id(new BasicBuilders.IntBuilder(2));

    Future disc = nodeConnFailLoc();
    tc = tc.body();
    tc = torrentLeecherConnReq(tc, triggerP, torrentLeecherConnReq(torrent, file1, leecher));//2
    tc = nodeLeecherConnReqLoc(tc, expectP, disc);//3
    tc = torrentLeecherConnReq(tc, triggerP, torrentLeecherConnReq(torrent, file2, leecher));//4
    tc = nodeLeecherConnFailLoc(tc, triggerP, disc); //5
    tc = tc.unordered();
    tc = torrentLeecherConnFail(tc, expectP); //6
    tc = torrentLeecherConnFail(tc, expectP); //6
    tc = tc.end();
    tc.repeat(1).body().end();
    assertTrue(tc.check());
    assertFalse(compState.activeLeecherFSM(torrent, leecher));
  }

  @Test
  public void testLocDisconnect1() {
    OverlayId torrent = torrentIdFactory.id(new BasicBuilders.IntBuilder(1));
    KAddress leecher = SystemHelper.getAddress(1);
    Identifier file1 = intIdFactory.id(new BasicBuilders.IntBuilder(1));

    tc = tc.body();
    tc = torrentLeecherConnReq(tc, triggerP, torrentLeecherConnReq(torrent, file1, leecher));//2
    tc = nodeLeecherConnReqLoc(tc, expectP);//3
    tc = torrentLeecherDisconnect(tc, triggerP, torrent, file1, leecher); //4
    tc = nodeLeecherDisconnectLoc(tc, expectP); //5
    tc.repeat(1).body().end();
    assertTrue(tc.check());
    assertFalse(compState.activeLeecherFSM(torrent, leecher));
  }

  //********************************************CONNECTED TO DISCONNECT*************************************************

  @Test
  public void testConnFail2() {
    OverlayId torrent = torrentIdFactory.id(new BasicBuilders.IntBuilder(1));
    KAddress leecher = SystemHelper.getAddress(1);
    Identifier file1 = intIdFactory.id(new BasicBuilders.IntBuilder(1));
    Identifier file2 = intIdFactory.id(new BasicBuilders.IntBuilder(2));

    tc = tc.body();
    tc = connected(tc, torrent, file1, leecher); //2-5
    tc = torrentLeecherConnReq(tc, triggerP, torrentLeecherConnReq(torrent, file2, leecher));//6
    tc = torrentLeecherConnSucc(tc, expectP); //7
    tc = nodeLeecherConnFailLoc(tc, triggerP, torrent, leecher);//8
    tc = tc.unordered();
    tc = torrentLeecherConnFail(tc, expectP);//9
    tc = torrentLeecherConnFail(tc, expectP);//9
    tc = tc.end();
    tc.repeat(1).body().end();
    assertTrue(tc.check());
    assertFalse(compState.activeLeecherFSM(torrent, leecher));
  }

  @Test
  public void testLocDisconnect2() {
    OverlayId torrent = torrentIdFactory.id(new BasicBuilders.IntBuilder(1));
    KAddress leecher = SystemHelper.getAddress(1);
    Identifier file1 = intIdFactory.id(new BasicBuilders.IntBuilder(1));

    tc = tc.body();
    tc = connected(tc, torrent, file1, leecher);//2-5
    tc = torrentLeecherDisconnect(tc, triggerP, torrent, file1, leecher); //6
    tc = nodeLeecherDisconnectLoc(tc, expectP); //7
    tc.repeat(1).body().end();
    assertTrue(tc.check());
    assertFalse(compState.activeLeecherFSM(torrent, leecher));
  }
}
