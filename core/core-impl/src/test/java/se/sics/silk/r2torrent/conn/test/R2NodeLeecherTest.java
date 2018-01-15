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

import com.google.common.base.Predicate;
import org.junit.After;
import static org.junit.Assert.assertTrue;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import se.sics.kompics.Component;
import se.sics.kompics.Port;
import se.sics.kompics.fsm.FSMException;
import se.sics.kompics.fsm.FSMStateName;
import se.sics.kompics.network.Network;
import se.sics.kompics.testing.TestContext;
import se.sics.kompics.timer.Timer;
import se.sics.ktoolbox.util.identifiable.BasicBuilders;
import se.sics.ktoolbox.util.identifiable.overlay.OverlayId;
import se.sics.ktoolbox.util.identifiable.overlay.OverlayIdFactory;
import se.sics.ktoolbox.util.network.KAddress;
import se.sics.silk.SystemHelper;
import se.sics.silk.SystemSetup;
import se.sics.silk.r2torrent.R2TorrentPort;
import static se.sics.silk.r2torrent.conn.R2NodeLeecher.HardCodedConfig.MAX_TORRENTS_PER_LEECHER;
import se.sics.silk.r2torrent.conn.R2NodeLeecher.States;
import se.sics.silk.r2torrent.conn.R2NodeLeecherAuxComp;
import static se.sics.silk.r2torrent.conn.R2NodeLeecherHelper.nodeLeecherConnReqNet;
import static se.sics.silk.r2torrent.conn.R2NodeLeecherHelper.nodeLeecherConnSuccNet;
import static se.sics.silk.r2torrent.conn.R2NodeLeecherHelper.nodeLeecherDisconnectNet;
import static se.sics.silk.r2torrent.conn.R2NodeLeecherHelper.nodeLeecherPing;
import static se.sics.silk.r2torrent.conn.R2NodeLeecherHelper.nodeLeecherPong;
import static se.sics.silk.r2torrent.conn.R2NodeLeecherHelper.torrentLeecherConnFail;
import static se.sics.silk.r2torrent.conn.R2NodeLeecherHelper.torrentLeecherConnReq;
import static se.sics.silk.r2torrent.conn.R2NodeLeecherHelper.torrentLeecherConnSucc;
import static se.sics.silk.r2torrent.conn.R2NodeLeecherHelper.torrentLeecherDisc;
import static se.sics.silk.r2torrent.conn.R2NodeLeecherTimeoutHelper.nodeLeecherCancelTimer;
import static se.sics.silk.r2torrent.conn.R2NodeLeecherTimeoutHelper.nodeLeecherSetTimer;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class R2NodeLeecherTest {

  private TestContext<R2NodeLeecherAuxComp> tc;
  private Component comp;
  private Port<R2TorrentPort> triggerP;
  private Port<R2TorrentPort> expectP;
  private Port<Network> networkP;
  private Port<Timer> timerP;
  private static OverlayIdFactory torrentIdFactory;
  private KAddress self;

  @BeforeClass
  public static void setup() throws FSMException {
    torrentIdFactory = SystemSetup.systemSetup("src/test/resources/application.conf");
  }

  @Before
  public void testSetup() {
    tc = getContext();
    comp = tc.getComponentUnderTest();
    triggerP = comp.getNegative(R2TorrentPort.class);
    expectP = comp.getPositive(R2TorrentPort.class);
    networkP = comp.getNegative(Network.class);
    timerP = comp.getNegative(Timer.class);
  }

  private TestContext<R2NodeLeecherAuxComp> getContext() {
    self = SystemHelper.getAddress(0);
    R2NodeLeecherAuxComp.Init init = new R2NodeLeecherAuxComp.Init(self);
    TestContext<R2NodeLeecherAuxComp> context = TestContext.newInstance(R2NodeLeecherAuxComp.class, init);
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

  //************************************************START TO END********************************************************
  @Test
  public void testStartEnd() {
    OverlayId torrent = torrentIdFactory.id(new BasicBuilders.IntBuilder(1));
    KAddress leecher = SystemHelper.getAddress(1);
    tc = tc.body();
    tc = inactiveFSM(tc, leecher); //1
    tc = torrentLeecherConnReq(tc, triggerP, torrent, leecher);
    tc = inactiveFSM(tc, leecher); 
    tc = torrentLeecherDisc(tc, triggerP, torrent, leecher);
    tc = inactiveFSM(tc, leecher); 
    tc = nodeLeecherPing(tc, networkP, self, leecher);
    tc = inactiveFSM(tc, leecher);
    tc = nodeLeecherDisconnectNet(tc, networkP, self, leecher);
    tc = inactiveFSM(tc, leecher); 
    tc.repeat(1).body().end();
    assertTrue(tc.check());
  }

  //************************************************START TO CONNECTED**************************************************

  @Test
  public void testStartConnect() {
    KAddress leecher = SystemHelper.getAddress(1);
    tc = tc.body();
    tc = inactiveFSM(tc, leecher); //1
    tc = connected(tc, leecher); //2-4
    tc = state(tc, leecher, States.CONNECTED); //5
    tc.repeat(1).body().end();
    assertTrue(tc.check());
  }

  private TestContext connected(TestContext tc, KAddress leecher) {
    tc = nodeLeecherConnReqNet(tc, networkP, self, leecher); //1
    tc = tc.unordered();
    tc = nodeLeecherSetTimer(tc, timerP, leecher); //2
    tc = nodeLeecherConnSuccNet(tc, networkP, leecher); //3
    tc = tc.end();
    return tc;
  }

  //************************************************CONNECTED TO CONNECTED**********************************************
  @Test
  public void testConnectedPing() {
    KAddress leecher = SystemHelper.getAddress(1);
    tc = tc.body();
    tc = inactiveFSM(tc, leecher); //1
    tc = connected(tc, leecher); //2-4
    tc = ping(tc, leecher); //5-6
    tc = state(tc, leecher, States.CONNECTED); //7
    tc.repeat(1).body().end();
    assertTrue(tc.check());
  }

  private TestContext ping(TestContext tc, KAddress leecher) {
    tc = nodeLeecherPing(tc, networkP, self, leecher);
    tc = nodeLeecherPong(tc, networkP, self, leecher);
    return tc;
  }

  @Test
  public void testConnectedConnect() {
    KAddress leecher = SystemHelper.getAddress(1);
    tc = tc.body();
    tc = inactiveFSM(tc, leecher); //1
    tc = connected(tc, leecher); //2-4
    tc = nodeLeecherConnReqNet(tc, networkP, self, leecher); //5
    tc = nodeLeecherConnSuccNet(tc, networkP, leecher);//6
    tc = state(tc, leecher, States.CONNECTED); //7
    tc.repeat(1).body().end();
    assertTrue(tc.check());
  }

  @Test
  public void testConnectedLeecherConnSucc1() {
    OverlayId torrent = torrentIdFactory.id(new BasicBuilders.IntBuilder(1));
    KAddress leecher = SystemHelper.getAddress(1);
    tc = tc.body();
    tc = inactiveFSM(tc, leecher); //1
    tc = connected(tc, leecher); //2-4
    tc = torrentLeecherConnReq(tc, triggerP, torrent, leecher); //5
    tc = torrentLeecherConnSucc(tc, expectP); //6
    tc = state(tc, leecher, States.CONNECTED); //7
    tc.repeat(1).body().end();
    assertTrue(tc.check());
  }

  @Test
  public void testConnectedLeecherConnFail() {
    OverlayId t = torrentIdFactory.id(new BasicBuilders.IntBuilder(MAX_TORRENTS_PER_LEECHER));
    KAddress leecher = SystemHelper.getAddress(1);
    tc = tc.body();
    tc = inactiveFSM(tc, leecher); //1
    tc = connected(tc, leecher); //2-4
    tc = torrentConnect(tc, leecher, 0, MAX_TORRENTS_PER_LEECHER);//5-24
    tc = torrentLeecherConnReq(tc, triggerP, t, leecher); //25
    tc = torrentLeecherConnFail(tc, expectP); //26
    tc = state(tc, leecher, States.CONNECTED); //27
    tc.repeat(1).body().end();
    assertTrue(tc.check());
  }

  @Test
  public void testConnectedLeecherDisc() {
    OverlayId torrent = torrentIdFactory.id(new BasicBuilders.IntBuilder(1));
    KAddress leecher = SystemHelper.getAddress(1);
    tc = tc.body();
    tc = inactiveFSM(tc, leecher); //1
    tc = connected(tc, leecher); //2-4
    tc = torrentLeecherConnReq(tc, triggerP, torrent, leecher); //5
    tc = torrentLeecherConnSucc(tc, expectP); //6
    tc = torrentLeecherDisc(tc, triggerP, torrent, leecher); //7
    tc = state(tc, leecher, States.CONNECTED); //8
    tc.repeat(1).body().end();
    assertTrue(tc.check());
  }

  @Test
  public void testConnectedLeecherConnSucc2() {
    OverlayId t = torrentIdFactory.id(new BasicBuilders.IntBuilder(MAX_TORRENTS_PER_LEECHER - 1));
    KAddress leecher = SystemHelper.getAddress(1);
    tc = tc.body();
    tc = inactiveFSM(tc, leecher); //1
    tc = connected(tc, leecher); //2-4
    tc = torrentConnect(tc, leecher, 0, MAX_TORRENTS_PER_LEECHER);//5-24
    tc = torrentLeecherDisc(tc, triggerP, t, leecher); //25
    tc = torrentLeecherConnReq(tc, triggerP, t, leecher); //26
    tc = torrentLeecherConnSucc(tc, expectP); //27
    tc = state(tc, leecher, States.CONNECTED); //28
    tc.repeat(1).body().end();
    assertTrue(tc.check());
  }

  //1-20
  public TestContext torrentConnect(TestContext tc, KAddress leecher, int startId, int nr) {
    for (int i = 0; i < nr; i++) {
      OverlayId torrent = torrentIdFactory.id(new BasicBuilders.IntBuilder(i));
      tc = torrentLeecherConnReq(tc, triggerP, torrent, leecher); //1
      tc = torrentLeecherConnSucc(tc, expectP);  //1
    }
    return tc;
  }

  //**************************************************CONNECTED TO FINAL************************************************

  @Test
  public void testConnectedDisconnect1() {
    KAddress leecher = SystemHelper.getAddress(1);
    tc = tc.body();
    tc = inactiveFSM(tc, leecher); //1
    tc = connected(tc, leecher); //2-4
    tc = nodeLeecherDisconnectNet(tc, networkP, self, leecher); //5
    tc = nodeLeecherCancelTimer(tc, timerP); //6
    tc = inactiveFSM(tc, leecher); //7
    tc.repeat(1).body().end();
    assertTrue(tc.check());
  }

  @Test
  public void testConnectedDisconnect2() {
    OverlayId torrent = torrentIdFactory.id(new BasicBuilders.IntBuilder(1));
    KAddress leecher = SystemHelper.getAddress(1);
    tc = tc.body();
    tc = inactiveFSM(tc, leecher); //1
    tc = connected(tc, leecher); //2-4
    tc = torrentLeecherConnReq(tc, triggerP, torrent, leecher); //5
    tc = torrentLeecherConnSucc(tc, expectP); //6
    tc = nodeLeecherDisconnectNet(tc, networkP, self, leecher); //7
    tc = tc.unordered();
    tc = nodeLeecherCancelTimer(tc, timerP); //8
    tc = torrentLeecherConnFail(tc, expectP);//9
    tc = tc.end();
    tc = inactiveFSM(tc, leecher); //10
    tc.repeat(1).body().end();
    assertTrue(tc.check());
  }
  //********************************************************************************************************************

  TestContext inactiveFSM(TestContext tc, KAddress seeder) {
    return tc.inspect(inactiveFSM(seeder));
  }

  TestContext activeFSM(TestContext tc, KAddress leecher) {
    return tc.inspect(activeFSM(leecher));
  }

  TestContext state(TestContext tc, KAddress leecher, FSMStateName expectedState) {
    return tc.inspect(state(leecher, expectedState));
  }

  Predicate<R2NodeLeecherAuxComp> inactiveFSM(KAddress leecher) {
    return (R2NodeLeecherAuxComp t) -> !t.activeLeecherFSM(leecher);
  }

  Predicate<R2NodeLeecherAuxComp> activeFSM(KAddress leecher) {
    return (R2NodeLeecherAuxComp t) -> t.activeLeecherFSM(leecher);
  }

  Predicate<R2NodeLeecherAuxComp> state(KAddress leecher, FSMStateName expectedState) {
    return (R2NodeLeecherAuxComp t) -> {
      FSMStateName currentState = t.leecherState(leecher);
      return currentState.equals(expectedState);
    };
  }
}
