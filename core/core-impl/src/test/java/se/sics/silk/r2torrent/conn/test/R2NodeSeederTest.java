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
import se.sics.silk.r2torrent.conn.R2NodeSeeder;
import se.sics.silk.r2torrent.conn.R2NodeSeeder.States;
import se.sics.silk.r2torrent.conn.helper.R2NodeSeederAuxComp;
import static se.sics.silk.r2torrent.conn.helper.R2NodeSeederHelper.nodeSeederCancelTimer;
import static se.sics.silk.r2torrent.conn.helper.R2NodeSeederHelper.nodeSeederConnFailLoc;
import static se.sics.silk.r2torrent.conn.helper.R2NodeSeederHelper.nodeSeederConnRejNet;
import static se.sics.silk.r2torrent.conn.helper.R2NodeSeederHelper.nodeSeederConnReqLoc;
import static se.sics.silk.r2torrent.conn.helper.R2NodeSeederHelper.nodeSeederConnReqNet;
import static se.sics.silk.r2torrent.conn.helper.R2NodeSeederHelper.nodeSeederConnSuccLoc;
import static se.sics.silk.r2torrent.conn.helper.R2NodeSeederHelper.nodeSeederConnSuccNet;
import static se.sics.silk.r2torrent.conn.helper.R2NodeSeederHelper.nodeSeederDisconnectLoc;
import static se.sics.silk.r2torrent.conn.helper.R2NodeSeederHelper.nodeSeederDisconnectNet;
import static se.sics.silk.r2torrent.conn.helper.R2NodeSeederHelper.nodeSeederPingNet;
import static se.sics.silk.r2torrent.conn.helper.R2NodeSeederHelper.nodeSeederPingTimeout;
import static se.sics.silk.r2torrent.conn.helper.R2NodeSeederHelper.nodeSeederPingTimer;
import static se.sics.silk.r2torrent.conn.helper.R2NodeSeederHelper.nodeSeederPongNet;
import static se.sics.silk.r2torrent.conn.helper.R2NodeSeederHelper.nodeSeederScheduleTimer;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class R2NodeSeederTest {
  private TestContext<R2NodeSeederAuxComp> tc;
  private Component comp;
  private Port<R2TorrentPort> triggerP;
  private Port<R2TorrentPort> expectP;
  private Port<Network> networkP;
  private Port<Timer> timerP;
  private static OverlayIdFactory torrentIdFactory;
  private KAddress selfAdr;

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

  private TestContext<R2NodeSeederAuxComp> getContext() {
    selfAdr = SystemHelper.getAddress(0);
    R2NodeSeederAuxComp.Init init = new R2NodeSeederAuxComp.Init(selfAdr);
    TestContext<R2NodeSeederAuxComp> context = TestContext.newInstance(R2NodeSeederAuxComp.class, init);
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

  //***************************************************START TO END*****************************************************
  @Test
  public void testStartEnd() {
    OverlayId torrent = torrentIdFactory.id(new BasicBuilders.IntBuilder(1));
    KAddress seeder = SystemHelper.getAddress(1);
    tc = tc.body();
    tc = inactiveFSM(tc, seeder); //1
    tc = nodeSeederDisconnectLoc(tc, triggerP, torrent, seeder); //2
    tc = inactiveFSM(tc, seeder); //3
    tc = nodeSeederDisconnectNet(tc, networkP, selfAdr, seeder);//4
    tc = inactiveFSM(tc, seeder); //5
    tc = nodeSeederPongNet(tc, networkP, selfAdr, seeder);//6
    tc = inactiveFSM(tc, seeder);//7
    tc = nodeSeederConnSuccNet(tc, networkP, selfAdr, seeder);//8
    tc = inactiveFSM(tc, seeder);//9
    tc = nodeSeederConnRejNet(tc, networkP, selfAdr, seeder);//10
    tc = inactiveFSM(tc, seeder); //11
    tc.repeat(1).body().end();
    assertTrue(tc.check());
  }
  //*************************************************CONNECT TO END*****************************************************
  //*****************************************************LOCAL**********************************************************
  @Test
  public void testConnectEndLocalDisc() {
    OverlayId torrent1 = torrentIdFactory.id(new BasicBuilders.IntBuilder(1));
    KAddress seeder = SystemHelper.getAddress(1);
    tc = tc.body();
    tc = nodeSeederConnReqLoc(tc, triggerP, nodeSeederConnReqLoc(torrent1, seeder)); //1
    tc = nodeSeederConnReqNet(tc, networkP, seeder);//2
    tc = nodeSeederDisconnectLoc(tc, triggerP, torrent1, seeder); //3
    tc = nodeSeederDisconnectNet(tc, networkP, seeder); //4
    tc = inactiveFSM(tc, seeder); //5
    tc.repeat(1).body().end();
    assertTrue(tc.check());
  }
  
  //*****************************************************SEEDER*********************************************************
  @Test
  public void testConnectEndNetRej() {
    OverlayId torrent1 = torrentIdFactory.id(new BasicBuilders.IntBuilder(1));
    KAddress seeder = SystemHelper.getAddress(1);
    tc = tc.body();
    tc = nodeSeederConnReqLoc(tc, triggerP, nodeSeederConnReqLoc(torrent1, seeder)); //1
    tc = nodeSeederConnRejNet(tc, networkP, networkP);//2-3
    tc = nodeSeederConnFailLoc(tc, expectP, torrent1); //4
    tc = inactiveFSM(tc, seeder); //5
    tc.repeat(1).body().end();
    assertTrue(tc.check());
  }
  
  @Test
  public void testConnectEndNetDisc() {
    OverlayId torrent1 = torrentIdFactory.id(new BasicBuilders.IntBuilder(1));
    KAddress seeder = SystemHelper.getAddress(1);
    tc = tc.body();
    tc = nodeSeederConnReqLoc(tc, triggerP, nodeSeederConnReqLoc(torrent1, seeder)); //1
    tc = nodeSeederConnReqNet(tc, networkP, seeder);//2
    tc = nodeSeederDisconnectNet(tc, networkP, selfAdr, seeder); //3
    tc = nodeSeederConnFailLoc(tc, expectP, torrent1); //4
    tc = inactiveFSM(tc, seeder); //5
    tc.repeat(1).body().end();
    assertTrue(tc.check());
  }
  //***********************************************CONNECT TO CONNECT***************************************************
  @Test
  public void testConnectNewConnect1() {
    OverlayId torrent1 = torrentIdFactory.id(new BasicBuilders.IntBuilder(1));
    OverlayId torrent2 = torrentIdFactory.id(new BasicBuilders.IntBuilder(2));
    KAddress seeder = SystemHelper.getAddress(1);
    tc = tc.body();
    tc = nodeSeederConnReqLoc(tc, triggerP, nodeSeederConnReqLoc(torrent1, seeder)); //1
    tc = nodeSeederConnReqNet(tc, networkP, seeder);//2
    tc = nodeSeederConnReqLoc(tc, triggerP, nodeSeederConnReqLoc(torrent2, seeder)); //3
    tc = state(tc, seeder, States.CONNECT); //4
    tc.repeat(1).body().end();
    assertTrue(tc.check());
  }
  //**********************************************CONNECT TO CONNECTED**************************************************
  @Test
  public void testConnectSucc() {
    OverlayId torrent1 = torrentIdFactory.id(new BasicBuilders.IntBuilder(1));
    KAddress seeder = SystemHelper.getAddress(1);
    tc = tc.body();
    tc = simpleConnSucc(tc, seeder, torrent1); //1-5
    tc = state(tc, seeder, States.CONNECTED); //6
    tc.repeat(1).body().end();
    assertTrue(tc.check());
  }
  
  private TestContext simpleConnSucc(TestContext tc, KAddress seeder, OverlayId torrentId) {
    tc = nodeSeederConnReqLoc(tc, triggerP, nodeSeederConnReqLoc(torrentId, seeder)); //1
    tc = nodeSeederConnSuccNet(tc, networkP, networkP);//2-3
    tc = nodeSeederScheduleTimer(tc, timerP); //4
    tc = nodeSeederConnSuccLoc(tc, expectP);//5
    return tc;
  }
  
  @Test
  public void testConnectNewConnect2() {
    OverlayId torrent1 = torrentIdFactory.id(new BasicBuilders.IntBuilder(1));
    OverlayId torrent2 = torrentIdFactory.id(new BasicBuilders.IntBuilder(2));
    KAddress seeder = SystemHelper.getAddress(1);
    tc = tc.body();
    tc = nodeSeederConnReqLoc(tc, triggerP, nodeSeederConnReqLoc(torrent1, seeder)); //1
    tc = nodeSeederConnReqLoc(tc, triggerP, nodeSeederConnReqLoc(torrent2, seeder)); //2
    tc = nodeSeederConnSuccNet(tc, networkP, networkP);//3-4
    tc = nodeSeederScheduleTimer(tc, timerP); //5
    tc = nodeSeederConnSuccLoc(tc, expectP);//6
    tc = nodeSeederConnSuccLoc(tc, expectP);//7
    tc = state(tc, seeder, States.CONNECTED); //8
    tc.repeat(1).body().end();
    assertTrue(tc.check());
  }
  //*************************************************CONNECTED TO CONNECTED*********************************************
  @Test
  public void testConnectedNewConnect() {
    OverlayId torrent1 = torrentIdFactory.id(new BasicBuilders.IntBuilder(1));
    OverlayId torrent2 = torrentIdFactory.id(new BasicBuilders.IntBuilder(2));
    KAddress seeder = SystemHelper.getAddress(1);
    tc = tc.body();
    tc = simpleConnSucc(tc, seeder, torrent1); //1-5
    tc = nodeSeederConnReqLoc(tc, triggerP, nodeSeederConnReqLoc(torrent2, seeder)); //6
    tc = nodeSeederConnSuccLoc(tc, expectP); //7
    tc = state(tc, seeder, States.CONNECTED); //8
    tc.repeat(1).body().end();
    assertTrue(tc.check());
  }
  
  @Test
  public void testConnectedDisconnect1() {
    OverlayId torrent1 = torrentIdFactory.id(new BasicBuilders.IntBuilder(1));
    OverlayId torrent2 = torrentIdFactory.id(new BasicBuilders.IntBuilder(2));
    KAddress seeder = SystemHelper.getAddress(1);
    tc = tc.body();
    tc = simpleConnSucc(tc, seeder, torrent1); //1-5
    tc = nodeSeederConnReqLoc(tc, triggerP, nodeSeederConnReqLoc(torrent2, seeder)); //6
    tc = nodeSeederConnSuccLoc(tc, expectP); //7
    tc = nodeSeederDisconnectLoc(tc, triggerP, torrent2, seeder); //8
    tc = state(tc, seeder, States.CONNECTED); //9
    tc.repeat(1).body().end();
    assertTrue(tc.check());
  }
  
  @Test
  public void testConnectedPingOk() {
    OverlayId torrent1 = torrentIdFactory.id(new BasicBuilders.IntBuilder(1));
    KAddress seeder = SystemHelper.getAddress(1);
    tc = tc.body();
    tc = simpleConnSucc(tc, seeder, torrent1); //1-5
    tc = missedPings(tc, seeder, R2NodeSeeder.HardCodedConfig.deadPings-1); //6-7
    tc = pingPong(tc, seeder); //8-9
    tc = state(tc, seeder, States.CONNECTED); //10
    tc.repeat(1).body().end();
    assertTrue(tc.check());
  }
  
  private TestContext missedPings(TestContext tc, KAddress seeder, int missedPings) {
    tc = tc.repeat(missedPings).body(); //1-2
    tc = nodeSeederPingTimer(tc, timerP, nodeSeederPingTimeout(seeder)); 
    tc = nodeSeederPingNet(tc, networkP, seeder);
    tc = tc.end();
    return tc;
  }
  
  private TestContext pingPong(TestContext tc, KAddress seeder) {
    tc = nodeSeederPingTimer(tc, timerP, nodeSeederPingTimeout(seeder)); //1
    tc = nodeSeederPingNet(tc, networkP); //2
    return tc;
  }
  //************************************************CONNECTED TO DISCONNECT*********************************************
  @Test
  public void testConnectedLocalDisconnect() {
    OverlayId torrent1 = torrentIdFactory.id(new BasicBuilders.IntBuilder(1));
    KAddress seeder = SystemHelper.getAddress(1);
    tc = tc.body();
    tc = simpleConnSucc(tc, seeder, torrent1); //1-5
    tc = nodeSeederDisconnectLoc(tc, triggerP, torrent1, seeder); //6
    tc = tc.unordered();
    tc = nodeSeederDisconnectNet(tc, networkP, seeder); //7
    tc = nodeSeederCancelTimer(tc, timerP); //8
    tc = tc.end();
    tc = inactiveFSM(tc, seeder); //9
    tc.repeat(1).body().end();
    assertTrue(tc.check());
  }
  
  
  @Test
  public void testConnectedNetDisconnect() {
    OverlayId torrent1 = torrentIdFactory.id(new BasicBuilders.IntBuilder(1));
    KAddress seeder = SystemHelper.getAddress(1);
    tc = tc.body();
    tc = simpleConnSucc(tc, seeder, torrent1); //1-5
    tc = nodeSeederDisconnectNet(tc, networkP, selfAdr, seeder); //6
    tc = tc.unordered();
    tc = nodeSeederConnFailLoc(tc, expectP, torrent1); //7
    tc = nodeSeederCancelTimer(tc, timerP); //8
    tc = tc.end();
    tc = inactiveFSM(tc, seeder); //9
    tc.repeat(1).body().end();
    assertTrue(tc.check());
  }
  
  @Test
  public void testConnectedPingDisc() {
    OverlayId torrent1 = torrentIdFactory.id(new BasicBuilders.IntBuilder(1));
    KAddress seeder = SystemHelper.getAddress(1);
    tc = tc.body();
    tc = simpleConnSucc(tc, seeder, torrent1); //1-5
    tc = missedPings(tc, seeder, R2NodeSeeder.HardCodedConfig.deadPings); //6-7
    tc = nodeSeederPingTimer(tc, timerP, nodeSeederPingTimeout(seeder)); 
    tc = tc.unordered();
    tc = nodeSeederCancelTimer(tc, timerP);
    tc = nodeSeederConnFailLoc(tc, expectP, torrent1);
    tc = tc.end();
    tc = inactiveFSM(tc, seeder); //8
    tc.repeat(1).body().end();
    assertTrue(tc.check());
  }
  
  TestContext inactiveFSM(TestContext tc, KAddress seeder) {
    return tc.inspect(inactiveFSM(seeder));
  }
  
  TestContext activeFSM(TestContext tc, KAddress seeder) {
    return tc.inspect(activeFSM(seeder));
  }
  
  TestContext state(TestContext tc, KAddress seeder, FSMStateName expectedState) {
    return tc.inspect(state(seeder, expectedState));
  }
  
  Predicate<R2NodeSeederAuxComp> inactiveFSM(KAddress seeder) {
    return (R2NodeSeederAuxComp t) -> !t.activeSeederFSM(seeder);
  }
  
  Predicate<R2NodeSeederAuxComp> activeFSM(KAddress seeder) {
    return (R2NodeSeederAuxComp t) -> t.activeSeederFSM(seeder);
  }
  
  Predicate<R2NodeSeederAuxComp> state(KAddress seeder, FSMStateName expectedState) {
    return (R2NodeSeederAuxComp t) -> {
      FSMStateName currentState = t.seederState(seeder);
      return currentState.equals(expectedState);
    };
  }
}
