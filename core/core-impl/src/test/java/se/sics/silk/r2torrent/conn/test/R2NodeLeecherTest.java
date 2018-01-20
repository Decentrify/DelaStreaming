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
import se.sics.silk.TorrentWrapperComp;
import se.sics.silk.r2torrent.R2TorrentComp;
import se.sics.silk.SelfPort;
import se.sics.silk.r2torrent.conn.R2NodeLeecher;
import static se.sics.silk.r2torrent.conn.R2NodeLeecher.HardCodedConfig.MAX_TORRENTS_PER_LEECHER;
import se.sics.silk.r2torrent.conn.R2NodeLeecher.States;
import static se.sics.silk.r2torrent.conn.helper.R2NodeLeecherHelper.nodeLeecherConnFailLoc;
import static se.sics.silk.r2torrent.conn.helper.R2NodeLeecherHelper.nodeLeecherConnReqLoc;
import static se.sics.silk.r2torrent.conn.helper.R2NodeLeecherHelper.nodeLeecherConnReqNet;
import static se.sics.silk.r2torrent.conn.helper.R2NodeLeecherHelper.nodeLeecherConnSuccLoc;
import static se.sics.silk.r2torrent.conn.helper.R2NodeLeecherHelper.nodeLeecherConnSuccNet;
import static se.sics.silk.r2torrent.conn.helper.R2NodeLeecherHelper.nodeLeecherDisc;
import static se.sics.silk.r2torrent.conn.helper.R2NodeLeecherHelper.nodeLeecherDisconnectNet;
import static se.sics.silk.r2torrent.conn.helper.R2NodeLeecherHelper.nodeLeecherPing;
import static se.sics.silk.r2torrent.conn.helper.R2NodeLeecherHelper.nodeLeecherPong;
import static se.sics.silk.r2torrent.conn.helper.R2NodeLeecherTimeoutHelper.nodeLeecherCancelTimer;
import static se.sics.silk.r2torrent.conn.helper.R2NodeLeecherTimeoutHelper.nodeLeecherSetTimer;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class R2NodeLeecherTest {

  private TestContext<TorrentWrapperComp> tc;
  private Component comp;
  private TorrentWrapperComp compState;
  private Port<SelfPort> triggerP;
  private Port<SelfPort> expectP;
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
    compState = (TorrentWrapperComp)comp.getComponent();
    triggerP = comp.getNegative(SelfPort.class);
    expectP = comp.getPositive(SelfPort.class);
    networkP = comp.getNegative(Network.class);
    timerP = comp.getNegative(Timer.class);
  }

  private TestContext<TorrentWrapperComp> getContext() {
    self = SystemHelper.getAddress(0);
    TorrentWrapperComp.Setup setup = new TorrentWrapperComp.Setup() {
      @Override
      public MultiFSM setupFSM(ComponentProxy proxy, Config config, R2TorrentComp.Ports ports) {
        try {
          R2NodeLeecher.ES fsmEs = new R2NodeLeecher.ES(self);
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
          return R2NodeLeecher.FSM.multifsm(fsmIdFactory, fsmEs, oexa);
        } catch (FSMException ex) {
          throw new RuntimeException(ex);
        }
      }
    };
    TorrentWrapperComp.Init init = new TorrentWrapperComp.Init(self, setup);
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

  //************************************************START TO END********************************************************
  @Test
  public void testStartEnd1() {
    OverlayId torrent = torrentIdFactory.id(new BasicBuilders.IntBuilder(1));
    KAddress leecher = SystemHelper.getAddress(1);
    tc = tc.body();
    tc = nodeLeecherConnReqLoc(tc, triggerP, torrent, leecher);
    tc.repeat(1).body().end();
    assertTrue(tc.check());
    Identifier fsmBaseId = R2NodeLeecher.fsmBaseId(leecher);
    assertFalse(compState.fsm.activeFSM(fsmBaseId));
  }
  
  @Test
  public void testStartEnd2() {
    OverlayId torrent = torrentIdFactory.id(new BasicBuilders.IntBuilder(1));
    KAddress leecher = SystemHelper.getAddress(1);
    tc = tc.body();
    tc = nodeLeecherDisc(tc, triggerP, torrent, leecher);
    tc.repeat(1).body().end();
    assertTrue(tc.check());
    Identifier fsmBaseId = R2NodeLeecher.fsmBaseId(leecher);
    assertFalse(compState.fsm.activeFSM(fsmBaseId));
  }
  
  @Test
  public void testStartEnd3() {
    OverlayId torrent = torrentIdFactory.id(new BasicBuilders.IntBuilder(1));
    KAddress leecher = SystemHelper.getAddress(1);
    tc = tc.body();
    tc = nodeLeecherPing(tc, networkP, self, leecher);
    tc.repeat(1).body().end();
    assertTrue(tc.check());
    Identifier fsmBaseId = R2NodeLeecher.fsmBaseId(leecher);
    assertFalse(compState.fsm.activeFSM(fsmBaseId));
  }
  
  @Test
  public void testStartEnd4() {
    OverlayId torrent = torrentIdFactory.id(new BasicBuilders.IntBuilder(1));
    KAddress leecher = SystemHelper.getAddress(1);
    tc = tc.body();
    tc = nodeLeecherDisconnectNet(tc, networkP, self, leecher);
    tc.repeat(1).body().end();
    assertTrue(tc.check());
    Identifier fsmBaseId = R2NodeLeecher.fsmBaseId(leecher);
    assertFalse(compState.fsm.activeFSM(fsmBaseId));
  }

  //************************************************START TO CONNECTED**************************************************

  @Test
  public void testStartConnect() {
    KAddress leecher = SystemHelper.getAddress(1);
    tc = tc.body();
    tc = connected(tc, leecher); //2-4
    tc.repeat(1).body().end();
    assertTrue(tc.check());
    Identifier fsmBaseId = R2NodeLeecher.fsmBaseId(leecher);
    assertEquals(States.CONNECTED, compState.fsm.getFSMState(fsmBaseId));
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
    tc = connected(tc, leecher); //2-4
    tc = ping(tc, leecher); //5-6
    tc.repeat(1).body().end();
    assertTrue(tc.check());
    Identifier fsmBaseId = R2NodeLeecher.fsmBaseId(leecher);
    assertEquals(States.CONNECTED, compState.fsm.getFSMState(fsmBaseId));
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
    tc = connected(tc, leecher); //2-4
    tc = nodeLeecherConnReqNet(tc, networkP, self, leecher); //5
    tc = nodeLeecherConnSuccNet(tc, networkP, leecher);//6
    tc.repeat(1).body().end();
    assertTrue(tc.check());
    Identifier fsmBaseId = R2NodeLeecher.fsmBaseId(leecher);
    assertEquals(States.CONNECTED, compState.fsm.getFSMState(fsmBaseId));
  }

  @Test
  public void testConnectedLeecherConnSucc1() {
    OverlayId torrent = torrentIdFactory.id(new BasicBuilders.IntBuilder(1));
    KAddress leecher = SystemHelper.getAddress(1);
    tc = tc.body();
    tc = connected(tc, leecher); //2-4
    tc = nodeLeecherConnReqLoc(tc, triggerP, torrent, leecher); //5
    tc = nodeLeecherConnSuccLoc(tc, expectP); //6
    tc.repeat(1).body().end();
    assertTrue(tc.check());
    Identifier fsmBaseId = R2NodeLeecher.fsmBaseId(leecher);
    assertEquals(States.CONNECTED, compState.fsm.getFSMState(fsmBaseId));
  }

  @Test
  public void testConnectedLeecherConnFail() {
    OverlayId t = torrentIdFactory.id(new BasicBuilders.IntBuilder(MAX_TORRENTS_PER_LEECHER));
    KAddress leecher = SystemHelper.getAddress(1);
    tc = tc.body();
    tc = connected(tc, leecher); //2-4
    tc = torrentConnect(tc, leecher, 0, MAX_TORRENTS_PER_LEECHER);//5-24
    tc = nodeLeecherConnReqLoc(tc, triggerP, t, leecher); //25
    tc = nodeLeecherConnFailLoc(tc, expectP); //26
    tc.repeat(1).body().end();
    assertTrue(tc.check());
    Identifier fsmBaseId = R2NodeLeecher.fsmBaseId(leecher);
    assertEquals(States.CONNECTED, compState.fsm.getFSMState(fsmBaseId));
  }

  @Test
  public void testConnectedLeecherDisc() {
    OverlayId torrent = torrentIdFactory.id(new BasicBuilders.IntBuilder(1));
    KAddress leecher = SystemHelper.getAddress(1);
    tc = tc.body();
    tc = connected(tc, leecher); //2-4
    tc = nodeLeecherConnReqLoc(tc, triggerP, torrent, leecher); //5
    tc = nodeLeecherConnSuccLoc(tc, expectP); //6
    tc = nodeLeecherDisc(tc, triggerP, torrent, leecher); //7
    tc.repeat(1).body().end();
    assertTrue(tc.check());
    Identifier fsmBaseId = R2NodeLeecher.fsmBaseId(leecher);
    assertEquals(States.CONNECTED, compState.fsm.getFSMState(fsmBaseId));
  }

  @Test
  public void testConnectedLeecherConnSucc2() {
    OverlayId t = torrentIdFactory.id(new BasicBuilders.IntBuilder(MAX_TORRENTS_PER_LEECHER - 1));
    KAddress leecher = SystemHelper.getAddress(1);
    tc = tc.body();
    tc = connected(tc, leecher); //2-4
    tc = torrentConnect(tc, leecher, 0, MAX_TORRENTS_PER_LEECHER);//5-24
    tc = nodeLeecherDisc(tc, triggerP, t, leecher); //25
    tc = nodeLeecherConnReqLoc(tc, triggerP, t, leecher); //26
    tc = nodeLeecherConnSuccLoc(tc, expectP); //27
    tc.repeat(1).body().end();
    assertTrue(tc.check());
    Identifier fsmBaseId = R2NodeLeecher.fsmBaseId(leecher);
    assertEquals(States.CONNECTED, compState.fsm.getFSMState(fsmBaseId));
  }

  //1-20
  public TestContext torrentConnect(TestContext tc, KAddress leecher, int startId, int nr) {
    for (int i = 0; i < nr; i++) {
      OverlayId torrent = torrentIdFactory.id(new BasicBuilders.IntBuilder(i));
      tc = nodeLeecherConnReqLoc(tc, triggerP, torrent, leecher); //1
      tc = nodeLeecherConnSuccLoc(tc, expectP);  //1
    }
    return tc;
  }

  //**************************************************CONNECTED TO FINAL************************************************

  @Test
  public void testConnectedDisconnect1() {
    KAddress leecher = SystemHelper.getAddress(1);
    tc = tc.body();
    tc = connected(tc, leecher); //2-4
    tc = nodeLeecherDisconnectNet(tc, networkP, self, leecher); //5
    tc = nodeLeecherCancelTimer(tc, timerP); //6
    tc.repeat(1).body().end();
    assertTrue(tc.check());
    Identifier fsmBaseId = R2NodeLeecher.fsmBaseId(leecher);
    assertFalse(compState.fsm.activeFSM(fsmBaseId));
  }

  @Test
  public void testConnectedDisconnect2() {
    OverlayId torrent = torrentIdFactory.id(new BasicBuilders.IntBuilder(1));
    KAddress leecher = SystemHelper.getAddress(1);
    tc = tc.body();
    tc = connected(tc, leecher); //2-4
    tc = nodeLeecherConnReqLoc(tc, triggerP, torrent, leecher); //5
    tc = nodeLeecherConnSuccLoc(tc, expectP); //6
    tc = nodeLeecherDisconnectNet(tc, networkP, self, leecher); //7
    tc = tc.unordered();
    tc = nodeLeecherCancelTimer(tc, timerP); //8
    tc = nodeLeecherConnFailLoc(tc, expectP);//9
    tc = tc.end();
    tc.repeat(1).body().end();
    assertTrue(tc.check());
    Identifier fsmBaseId = R2NodeLeecher.fsmBaseId(leecher);
    assertFalse(compState.fsm.activeFSM(fsmBaseId));
  }
}
