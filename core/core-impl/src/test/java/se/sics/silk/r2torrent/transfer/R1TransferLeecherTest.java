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
package se.sics.silk.r2torrent.transfer;

import java.util.Random;
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
import se.sics.kompics.network.Msg;
import se.sics.kompics.network.Network;
import se.sics.kompics.testing.Direction;
import se.sics.kompics.testing.Future;
import se.sics.kompics.testing.TestContext;
import se.sics.kompics.timer.CancelPeriodicTimeout;
import se.sics.kompics.timer.SchedulePeriodicTimeout;
import se.sics.kompics.timer.Timer;
import se.sics.kompics.util.Identifier;
import se.sics.ktoolbox.util.identifiable.BasicBuilders;
import se.sics.ktoolbox.util.identifiable.basic.IntIdFactory;
import se.sics.ktoolbox.util.identifiable.overlay.OverlayId;
import se.sics.ktoolbox.util.identifiable.overlay.OverlayIdFactory;
import se.sics.ktoolbox.util.network.KAddress;
import se.sics.silk.FutureHelper;
import se.sics.silk.MsgHelper;
import se.sics.silk.SelfPort;
import se.sics.silk.SystemHelper;
import se.sics.silk.SystemSetup;
import static se.sics.silk.TorrentTestHelper.eCancelPeriodicTimer;
import static se.sics.silk.TorrentTestHelper.eNetPayload;
import static se.sics.silk.TorrentTestHelper.eSchedulePeriodicTimer;
import se.sics.silk.WrapperComp;
import se.sics.silk.r2torrent.R2TorrentComp;
import se.sics.silk.r2torrent.transfer.R1TransferLeecher.States;
import se.sics.silk.r2torrent.transfer.events.R1TransferLeecherEvents;
import se.sics.silk.r2torrent.transfer.events.R1TransferLeecherPing;
import se.sics.silk.r2torrent.transfer.msgs.R1TransferMsgs;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class R1TransferLeecherTest {

  private TestContext<WrapperComp> tc;
  private Component comp;
  private WrapperComp compState;
  private Port<SelfPort> triggerP;
  private Port<SelfPort> expectP;
  private Port<Network> networkP;
  private Port<Timer> timerP;
  private static OverlayIdFactory torrentIdFactory;
  private IntIdFactory intIdFactory;
  private KAddress self;
  private KAddress leecher;
  private OverlayId torrent;
  private Identifier file;

  @BeforeClass
  public static void setup() throws FSMException {
    torrentIdFactory = SystemSetup.systemSetup("src/test/resources/application.conf");
  }

  @Before
  public void testSetup() {
    self = SystemHelper.getAddress(0);
    leecher = SystemHelper.getAddress(1);
    intIdFactory = new IntIdFactory(new Random());
    torrent = torrentIdFactory.id(new BasicBuilders.IntBuilder(1));
    file = intIdFactory.id(new BasicBuilders.IntBuilder(1));

    tc = getContext();
    comp = tc.getComponentUnderTest();
    compState = (WrapperComp) comp.getComponent();
    triggerP = comp.getNegative(SelfPort.class);
    expectP = comp.getPositive(SelfPort.class);
    networkP = comp.getNegative(Network.class);
    timerP = comp.getNegative(Timer.class);
  }

  private TestContext<WrapperComp> getContext() {
    WrapperComp.Setup setup = new WrapperComp.Setup() {
      @Override
      public MultiFSM setupFSM(ComponentProxy proxy, Config config) {
        try {
          R2TorrentComp.Ports ports = new R2TorrentComp.Ports(proxy);
          R1TransferLeecher.ES fsmEs = new R1TransferLeecher.ES(self);
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
          return R1TransferLeecher.FSM.multifsm(fsmIdFactory, fsmEs, oexa);
        } catch (FSMException ex) {
          throw new RuntimeException(ex);
        }
      }
    };
    WrapperComp.Init init = new WrapperComp.Init(setup);
    TestContext<WrapperComp> context = TestContext.newInstance(WrapperComp.class, init);
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

  //***************************************************START TO CONNECT*************************************************
  @Test
  public void testStartToConnect() {
    tc = tc.body();
    tc = startToConnect(tc, torrent, file, leecher);//1-2
    tc.repeat(1).body().end();
    assertTrue(tc.check());
    Identifier fsmBaseId = R1TransferLeecher.fsmBasicId(torrent, file, leecher.getId());
    assertEquals(States.CONNECT, compState.fsm.getFSMState(fsmBaseId));
  }

  public TestContext startToConnect(TestContext tc, OverlayId torrentId, Identifier fileId, KAddress leecher) {
    tc = tc.trigger(netConnect(torrentId, fileId, leecher), networkP);//1
    tc = tc.expect(R1TransferLeecherEvents.ConnectReq.class, expectP, Direction.OUT);//2
    return tc;
  }

  //***************************************************START TO ACTIVE**************************************************
  @Test
  public void testStartToActive() {
    tc = tc.body();
    tc = connected(tc, torrent, file, leecher);//1-5
    tc.repeat(1).body().end();
    assertTrue(tc.check());
    Identifier fsmBaseId = R1TransferLeecher.fsmBasicId(torrent, file, leecher.getId());
    assertEquals(States.ACTIVE, compState.fsm.getFSMState(fsmBaseId));
  }
  //***************************************************DOUBLE CONNECT***************************************************
  @Test
  public void testDoubleConnect() {
    tc = tc.body();
    tc = connected(tc, torrent, file, leecher);//1-5
    tc = tc.trigger(netConnect(torrent, file, leecher), networkP);//6
    tc = eNetPayload(tc, R1TransferMsgs.ConnectAcc.class, networkP); //7
    tc.repeat(1).body().end();
    assertTrue(tc.check());
    Identifier fsmBaseId = R1TransferLeecher.fsmBasicId(torrent, file, leecher.getId());
    assertEquals(States.ACTIVE, compState.fsm.getFSMState(fsmBaseId));
  }
  //********************************************************PING********************************************************
  @Test
  public void testPingOk() {
    tc = tc.body();
    tc = connected(tc, torrent, file, leecher); //1-5
    tc = tc.repeat(R1TransferSeeder.HardCodedConfig.deadPings-1).body();
    tc = tc.trigger(timerPing(torrent, file, leecher), timerP); //6
    tc = tc.end();
    tc = tc.repeat(R1TransferSeeder.HardCodedConfig.deadPings).body();
    tc = pingPong(tc, torrent, file, leecher); //7-8
    tc = tc.end();
    tc.repeat(1).body().end();
    assertTrue(tc.check());
    Identifier fsmId = R1TransferLeecher.fsmBasicId(torrent, file, leecher.getId());
    assertEquals(States.ACTIVE, compState.fsm.getFSMState(fsmId));
  }
  
  @Test
  public void testPingDisc() {
    tc = tc.body();
    tc = connected(tc, torrent, file, leecher); //1-5
    tc = tc.repeat(R1TransferSeeder.HardCodedConfig.deadPings).body();
    tc = tc.trigger(timerPing(torrent, file, leecher), timerP);//6
    tc = tc.end();
    tc = pingDisc(tc, torrent, file, leecher); //7-8
    tc.repeat(1).body().end();
    assertTrue(tc.check());
    Identifier fsmId = R1TransferLeecher.fsmBasicId(torrent, file, leecher.getId());
    assertFalse(compState.fsm.activeFSM(fsmId));
  }

  //***************************************************CONNECT TO DISCONNECT********************************************
  @Test
  public void testConnectToNetDisconnect() {
    tc = tc.body();
    tc = startToConnect(tc, torrent, file, leecher);//1-2
    tc = tc.trigger(netDisconnect(torrent, file, leecher), networkP);//3
    tc = tc.expect(R1TransferLeecherEvents.Disconnected.class, expectP, Direction.OUT);//4
    tc.repeat(1).body().end();
    assertTrue(tc.check());
    Identifier fsmBaseId = R1TransferLeecher.fsmBasicId(torrent, file, leecher.getId());
    assertFalse(compState.fsm.activeFSM(fsmBaseId));
  }

  //***************************************************ACTIVE TO DISCONNECT********************************************
  @Test
  public void testActiveToNetDisconnect() {
    tc = tc.body();
    tc = connected(tc, torrent, file, leecher);//1-2
    tc = tc.trigger(netDisconnect(torrent, file, leecher), networkP);//3
    tc = tc.expect(R1TransferLeecherEvents.Disconnected.class, expectP, Direction.OUT);//4
    tc = eCancelPeriodicTimer(tc, timerP); //5
    tc.repeat(1).body().end();
    assertTrue(tc.check());
    Identifier fsmBaseId = R1TransferLeecher.fsmBasicId(torrent, file, leecher.getId());
    assertFalse(compState.fsm.activeFSM(fsmBaseId));
  }

  @Test
  public void testActiveToLocaltDisconnect() {
    tc = tc.body();
    tc = connected(tc, torrent, file, leecher);//1-2
    tc = tc.trigger(localDisconnect(torrent, file, leecher), triggerP);//3
    tc = eNetPayload(tc, R1TransferLeecherEvents.Disconnected.class, networkP);//4
    tc = eCancelPeriodicTimer(tc, timerP); //5
    tc.repeat(1).body().end();
    assertTrue(tc.check());
    Identifier fsmBaseId = R1TransferLeecher.fsmBasicId(torrent, file, leecher.getId());
    assertFalse(compState.fsm.activeFSM(fsmBaseId));
  }

  //********************************************************************************************************************
  public TestContext connected(TestContext tc, OverlayId torrentId, Identifier fileId, KAddress leecher) {
    tc = tc.trigger(netConnect(torrentId, fileId, leecher), networkP);//1
    tc = localConnectAcc(tc); //2-3
    tc = eNetPayload(tc, R1TransferMsgs.ConnectAcc.class, networkP); //4
    tc = eSchedulePeriodicTimer(tc, R1TransferLeecherPing.class, timerP); //5
    return tc;
  }
  
  private TestContext pingPong(TestContext tc, OverlayId torrentId, Identifier fileId, KAddress leecher) {
    tc = tc.trigger(netPing(torrentId, fileId, leecher), networkP);
    tc = eNetPayload(tc, R1TransferMsgs.Pong.class, networkP);
    return tc;
  }
  
  private TestContext pingDisc(TestContext tc, OverlayId torrentId, Identifier fileId, KAddress leecher) {
    tc = tc.trigger(timerPing(torrentId, fileId, leecher), timerP); //1
    tc = tc.expect(R1TransferLeecherEvents.Disconnected.class, expectP, Direction.OUT); //2
    tc = eNetPayload(tc, R1TransferMsgs.Disconnect.class, networkP);//3
    tc = tc.expect(CancelPeriodicTimeout.class, timerP, Direction.OUT); //4
    return tc;
  }
  
  public TestContext localConnectAcc(TestContext tc) {
    Future f = new FutureHelper.BasicFuture<R1TransferLeecherEvents.ConnectReq, R1TransferLeecherEvents.ConnectAcc>() {

      @Override
      public R1TransferLeecherEvents.ConnectAcc get() {
        return event.accept();
      }

    };
    return tc
      .answerRequest(R1TransferLeecherEvents.ConnectReq.class, expectP, f)
      .trigger(f, triggerP);
  }

  public Msg netConnect(OverlayId torrentId, Identifier fileId, KAddress leecher) {
    R1TransferMsgs.Connect payload = new R1TransferMsgs.Connect(torrentId, fileId);
    return MsgHelper.msg(leecher, self, payload);
  }

  public Msg netDisconnect(OverlayId torrentId, Identifier fileId, KAddress leecher) {
    R1TransferMsgs.Disconnect payload = new R1TransferMsgs.Disconnect(torrentId, fileId);
    return MsgHelper.msg(leecher, self, payload);
  }
  
  public Msg netPing(OverlayId torrentId, Identifier fileId, KAddress leecher) {
    R1TransferMsgs.Ping payload = new R1TransferMsgs.Ping(torrentId, fileId);
    return MsgHelper.msg(leecher, self, payload);
  }

  public R1TransferLeecherEvents.Disconnect localDisconnect(OverlayId torrentId, Identifier fileId, KAddress leecher) {
    return new R1TransferLeecherEvents.Disconnect(torrentId, fileId, leecher.getId());
  }
  
  public R1TransferLeecherPing timerPing(OverlayId torrentId, Identifier fileId, KAddress leecher) {
    SchedulePeriodicTimeout spt = new SchedulePeriodicTimeout(R1TransferSeeder.HardCodedConfig.pingTimerPeriod, 
      R1TransferSeeder.HardCodedConfig.pingTimerPeriod);
    return new R1TransferLeecherPing(spt, torrentId, fileId, leecher.getId());
  }
}
