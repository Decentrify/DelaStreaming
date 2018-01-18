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

import org.junit.After;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import se.sics.kompics.Component;
import se.sics.kompics.ComponentProxy;
import se.sics.kompics.Port;
import se.sics.kompics.config.Config;
import se.sics.kompics.fsm.FSMException;
import se.sics.kompics.fsm.MultiFSM;
import se.sics.kompics.fsm.OnFSMExceptionAction;
import se.sics.kompics.fsm.id.FSMIdentifierFactory;
import se.sics.kompics.network.Network;
import se.sics.kompics.testing.TestContext;
import se.sics.kompics.timer.Timer;
import se.sics.kompics.util.Identifier;
import se.sics.ktoolbox.util.identifiable.BasicBuilders;
import se.sics.ktoolbox.util.identifiable.overlay.OverlayId;
import se.sics.ktoolbox.util.identifiable.overlay.OverlayIdFactory;
import se.sics.ktoolbox.util.network.KAddress;
import se.sics.silk.SystemHelper;
import se.sics.silk.SystemSetup;
import static se.sics.silk.TorrentTestHelper.netNodeConnAcc;
import static se.sics.silk.TorrentTestHelper.eNodeSeederConnSucc;
import se.sics.silk.TorrentWrapperComp;
import se.sics.silk.r2torrent.R2TorrentComp;
import se.sics.silk.r2torrent.R2TorrentPort;
import se.sics.silk.r2torrent.conn.R2NodeSeeder;
import se.sics.silk.r2torrent.conn.R2NodeSeeder.States;
import static se.sics.silk.r2torrent.conn.helper.R2NodeSeederHelper.nodeSeederCancelTimer;
import static se.sics.silk.r2torrent.conn.helper.R2NodeSeederHelper.nodeSeederConnFailLoc;
import static se.sics.silk.r2torrent.conn.helper.R2NodeSeederHelper.nodeSeederConnRejNet;
import static se.sics.silk.r2torrent.conn.helper.R2NodeSeederHelper.nodeSeederConnReqLoc;
import static se.sics.silk.r2torrent.conn.helper.R2NodeSeederHelper.nodeSeederConnReqNet;
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
  private TestContext<TorrentWrapperComp> tc;
  private Component comp;
  private TorrentWrapperComp compState;
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
    compState = (TorrentWrapperComp)comp.getComponent();
    triggerP = comp.getNegative(R2TorrentPort.class);
    expectP = comp.getPositive(R2TorrentPort.class);
    networkP = comp.getNegative(Network.class);
    timerP = comp.getNegative(Timer.class);
  }

  private TestContext<TorrentWrapperComp> getContext() {
    selfAdr = SystemHelper.getAddress(0);
    TorrentWrapperComp.Setup setup = new TorrentWrapperComp.Setup() {
      @Override
      public MultiFSM setupFSM(ComponentProxy proxy, Config config, R2TorrentComp.Ports ports) {
        try {
          R2NodeSeeder.ES fsmEs = new R2NodeSeeder.ES(selfAdr);
          fsmEs.setProxy(proxy);
          fsmEs.setPorts(ports);
          
          OnFSMExceptionAction oexa = new OnFSMExceptionAction() {
            @Override
            public void handle(FSMException ex) {
              throw new RuntimeException(ex);
            }
          };
          FSMIdentifierFactory fsmIdFactory = config.getValue(FSMIdentifierFactory.CONFIG_KEY,
            FSMIdentifierFactory.class);
          return R2NodeSeeder.FSM.multifsm(fsmIdFactory, fsmEs, oexa);
        } catch (FSMException ex) {
          throw new RuntimeException(ex);
        }
      }
    };
    TorrentWrapperComp.Init init = new TorrentWrapperComp.Init(selfAdr, setup);
    TestContext<TorrentWrapperComp> context = TestContext.newInstance(TorrentWrapperComp.class, init);
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
  public void testStartEnd1() {
    OverlayId torrent = torrentIdFactory.id(new BasicBuilders.IntBuilder(1));
    KAddress seeder = SystemHelper.getAddress(1);
    tc = tc.body();
    tc = nodeSeederDisconnectLoc(tc, triggerP, torrent, seeder); //2
    tc.repeat(1).body().end();
    assertTrue(tc.check());
    Identifier fsmBaseId = R2NodeSeeder.fsmBaseId(seeder);
    assertFalse(compState.fsm.activeFSM(fsmBaseId));
  }
  
  @Test
  public void testStartEnd2() {
    OverlayId torrent = torrentIdFactory.id(new BasicBuilders.IntBuilder(1));
    KAddress seeder = SystemHelper.getAddress(1);
    tc = tc.body();
    tc = nodeSeederDisconnectNet(tc, networkP, selfAdr, seeder);//4
    tc.repeat(1).body().end();
    assertTrue(tc.check());
    Identifier fsmBaseId = R2NodeSeeder.fsmBaseId(seeder);
    assertFalse(compState.fsm.activeFSM(fsmBaseId));
  }
  
  @Test
  public void testStartEnd3() {
    OverlayId torrent = torrentIdFactory.id(new BasicBuilders.IntBuilder(1));
    KAddress seeder = SystemHelper.getAddress(1);
    tc = tc.body();
    tc = nodeSeederPongNet(tc, networkP, selfAdr, seeder);//6
    tc.repeat(1).body().end();
    assertTrue(tc.check());
    Identifier fsmBaseId = R2NodeSeeder.fsmBaseId(seeder);
    assertFalse(compState.fsm.activeFSM(fsmBaseId));
  }
  @Test
  public void testStartEnd4() {
    OverlayId torrent = torrentIdFactory.id(new BasicBuilders.IntBuilder(1));
    KAddress seeder = SystemHelper.getAddress(1);
    tc = tc.body();
    tc = nodeSeederConnSuccNet(tc, networkP, selfAdr, seeder);//8
    tc.repeat(1).body().end();
    assertTrue(tc.check());
    Identifier fsmBaseId = R2NodeSeeder.fsmBaseId(seeder);
    assertFalse(compState.fsm.activeFSM(fsmBaseId));
  }
  
  @Test
  public void testStartEnd5() {
    OverlayId torrent = torrentIdFactory.id(new BasicBuilders.IntBuilder(1));
    KAddress seeder = SystemHelper.getAddress(1);
    tc = tc.body();
    tc = nodeSeederConnRejNet(tc, networkP, selfAdr, seeder);//10
    tc.repeat(1).body().end();
    assertTrue(tc.check());
    Identifier fsmBaseId = R2NodeSeeder.fsmBaseId(seeder);
    assertFalse(compState.fsm.activeFSM(fsmBaseId));
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
    tc.repeat(1).body().end();
    assertTrue(tc.check());
    Identifier fsmBaseId = R2NodeSeeder.fsmBaseId(seeder);
    assertFalse(compState.fsm.activeFSM(fsmBaseId));
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
    tc.repeat(1).body().end();
    assertTrue(tc.check());
    Identifier fsmBaseId = R2NodeSeeder.fsmBaseId(seeder);
    assertFalse(compState.fsm.activeFSM(fsmBaseId));
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
    tc.repeat(1).body().end();
    assertTrue(tc.check());
    Identifier fsmBaseId = R2NodeSeeder.fsmBaseId(seeder);
    assertFalse(compState.fsm.activeFSM(fsmBaseId));
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
    tc.repeat(1).body().end();
    assertTrue(tc.check());
    Identifier fsmBaseId = R2NodeSeeder.fsmBaseId(seeder);
    assertEquals(States.CONNECT, compState.fsm.getFSMState(fsmBaseId));
  }
  //**********************************************CONNECT TO CONNECTED**************************************************
  @Test
  public void testConnectSucc() {
    OverlayId torrent1 = torrentIdFactory.id(new BasicBuilders.IntBuilder(1));
    KAddress seeder = SystemHelper.getAddress(1);
    tc = tc.body();
    tc = simpleConnSucc(tc, seeder, torrent1); //1-5
    tc.repeat(1).body().end();
    assertTrue(tc.check());
    Identifier fsmBaseId = R2NodeSeeder.fsmBaseId(seeder);
    assertEquals(States.CONNECTED, compState.fsm.getFSMState(fsmBaseId));
  }
  
  private TestContext simpleConnSucc(TestContext tc, KAddress seeder, OverlayId torrentId) {
    tc = nodeSeederConnReqLoc(tc, triggerP, nodeSeederConnReqLoc(torrentId, seeder)); //1
    tc = netNodeConnAcc(tc, networkP);//2-3
    tc = nodeSeederScheduleTimer(tc, timerP); //4
    tc = eNodeSeederConnSucc(tc, expectP);//5
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
    tc = netNodeConnAcc(tc, networkP);//3-4
    tc = nodeSeederScheduleTimer(tc, timerP); //5
    tc = eNodeSeederConnSucc(tc, expectP);//6
    tc = eNodeSeederConnSucc(tc, expectP);//7
    tc.repeat(1).body().end();
    assertTrue(tc.check());
    Identifier fsmBaseId = R2NodeSeeder.fsmBaseId(seeder);
    assertEquals(States.CONNECTED, compState.fsm.getFSMState(fsmBaseId));
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
    tc = eNodeSeederConnSucc(tc, expectP); //7
    tc.repeat(1).body().end();
    assertTrue(tc.check());
    Identifier fsmBaseId = R2NodeSeeder.fsmBaseId(seeder);
    assertEquals(States.CONNECTED, compState.fsm.getFSMState(fsmBaseId));
  }
  
  @Test
  public void testConnectedDisconnect1() {
    OverlayId torrent1 = torrentIdFactory.id(new BasicBuilders.IntBuilder(1));
    OverlayId torrent2 = torrentIdFactory.id(new BasicBuilders.IntBuilder(2));
    KAddress seeder = SystemHelper.getAddress(1);
    tc = tc.body();
    tc = simpleConnSucc(tc, seeder, torrent1); //1-5
    tc = nodeSeederConnReqLoc(tc, triggerP, nodeSeederConnReqLoc(torrent2, seeder)); //6
    tc = eNodeSeederConnSucc(tc, expectP); //7
    tc = nodeSeederDisconnectLoc(tc, triggerP, torrent2, seeder); //8
    tc.repeat(1).body().end();
    assertTrue(tc.check());
    Identifier fsmBaseId = R2NodeSeeder.fsmBaseId(seeder);
    assertEquals(States.CONNECTED, compState.fsm.getFSMState(fsmBaseId));
  }
  
  @Test
  public void testConnectedPingOk() {
    OverlayId torrent1 = torrentIdFactory.id(new BasicBuilders.IntBuilder(1));
    KAddress seeder = SystemHelper.getAddress(1);
    tc = tc.body();
    tc = simpleConnSucc(tc, seeder, torrent1); //1-5
    tc = missedPings(tc, seeder, R2NodeSeeder.HardCodedConfig.deadPings-1); //6-7
    tc = pingPong(tc, seeder); //8-9
    tc.repeat(1).body().end();
    assertTrue(tc.check());
    Identifier fsmBaseId = R2NodeSeeder.fsmBaseId(seeder);
    assertEquals(States.CONNECTED, compState.fsm.getFSMState(fsmBaseId));
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
    tc.repeat(1).body().end();
    assertTrue(tc.check());
    Identifier fsmBaseId = R2NodeSeeder.fsmBaseId(seeder);
    assertFalse(compState.fsm.activeFSM(fsmBaseId));
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
    tc.repeat(1).body().end();
    assertTrue(tc.check());
    Identifier fsmBaseId = R2NodeSeeder.fsmBaseId(seeder);
    assertFalse(compState.fsm.activeFSM(fsmBaseId));
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
    tc.repeat(1).body().end();
    assertTrue(tc.check());
    Identifier fsmBaseId = R2NodeSeeder.fsmBaseId(seeder);
    assertFalse(compState.fsm.activeFSM(fsmBaseId));
  }
}
