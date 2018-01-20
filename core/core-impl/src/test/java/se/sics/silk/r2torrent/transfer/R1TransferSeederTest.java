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
import se.sics.ktoolbox.util.network.basic.BasicContentMsg;
import se.sics.silk.FutureHelper;
import se.sics.silk.MsgHelper;
import se.sics.silk.SelfPort;
import se.sics.silk.SystemHelper;
import se.sics.silk.SystemSetup;
import static se.sics.silk.TorrentTestHelper.eNetPayload;
import static se.sics.silk.TorrentTestHelper.eSchedulePeriodicTimer;
import se.sics.silk.WrapperComp;
import se.sics.silk.r2torrent.R2TorrentComp;
import se.sics.silk.r2torrent.transfer.R1TransferSeeder.States;
import se.sics.silk.r2torrent.transfer.events.R1TransferSeederEvents;
import se.sics.silk.r2torrent.transfer.events.R1TransferSeederPing;
import se.sics.silk.r2torrent.transfer.msgs.R1TransferMsgs;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class R1TransferSeederTest {
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
  private KAddress seeder;
  private OverlayId torrent;
  private Identifier file;

  @BeforeClass
  public static void setup() throws FSMException {
    torrentIdFactory = SystemSetup.systemSetup("src/test/resources/application.conf");
  }

  @Before
  public void testSetup() {
    self = SystemHelper.getAddress(0);
    seeder = SystemHelper.getAddress(1);
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
          R1TransferSeeder.ES fsmEs = new R1TransferSeeder.ES(self);
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
          return R1TransferSeeder.FSM.multifsm(fsmIdFactory, fsmEs, oexa);
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
    tc = startToConnect(tc); //1-2
    tc.repeat(1).body().end();
    assertTrue(tc.check());
    Identifier fsmId = R1TransferSeeder.fsmBasicId(torrent, file, seeder.getId());
    assertEquals(States.CONNECT, compState.fsm.getFSMState(fsmId));
  }
  
  private TestContext startToConnect(TestContext tc) {
    tc = tc.trigger(connect(torrent, file, seeder), triggerP); //1
    tc = eNetPayload(tc, R1TransferMsgs.Connect.class, networkP); //2
    return tc;
  }
  
  //****************************************************START TO ACTIVE*************************************************
  @Test
  public void testStartToActive() {
    tc = tc.body();
    tc = startToActive(tc); //1-5
    tc.repeat(1).body().end();
    assertTrue(tc.check());
    Identifier fsmId = R1TransferSeeder.fsmBasicId(torrent, file, seeder.getId());
    assertEquals(States.ACTIVE, compState.fsm.getFSMState(fsmId));
  }
  
  private TestContext startToActive(TestContext tc) {
    tc = connect(tc, torrent, file, seeder); //1-5
    return tc;
  }
  
  //***********************************************************PING*****************************************************
  @Test
  public void testPingToActive() {
    tc = tc.body();
    tc = connect(tc, torrent, file, seeder); //1-5
    tc = tc.repeat(R1TransferSeeder.HardCodedConfig.deadPings-1).body();
    tc = pingFail(tc, torrent, file, seeder); //7-8
    tc = tc.end();
    tc = tc.repeat(R1TransferSeeder.HardCodedConfig.deadPings).body();
    tc = pingPong(tc, torrent, file, seeder); //9-11
    tc = tc.end();
    tc.repeat(1).body().end();
    assertTrue(tc.check());
    Identifier fsmId = R1TransferSeeder.fsmBasicId(torrent, file, seeder.getId());
    assertEquals(States.ACTIVE, compState.fsm.getFSMState(fsmId));
  }
  
  @Test
  public void testPingDisconnected() {
    tc = tc.body();
    tc = connect(tc, torrent, file, seeder); //1-5
    tc = tc.repeat(R1TransferSeeder.HardCodedConfig.deadPings).body();
    tc = pingFail(tc, torrent, file, seeder); //7-8
    tc = tc.end();
    tc = pingDisc(tc, torrent, file, seeder); //9-11
    tc.repeat(1).body().end();
    assertTrue(tc.check());
    Identifier fsmId = R1TransferSeeder.fsmBasicId(torrent, file, seeder.getId());
    assertFalse(compState.fsm.activeFSM(fsmId));
  }
  
  //**************************************************CONNECT TO DISCONNECT*********************************************
  @Test
  public void testConnectToDisconnect() {
    tc = tc.body();
    tc = startToConnect(tc); //1-2
    tc = localDisconnect2(tc, torrent, file, seeder); //3-4
    tc.repeat(1).body().end();
    assertTrue(tc.check());
    Identifier fsmId = R1TransferSeeder.fsmBasicId(torrent, file, seeder.getId());
    assertFalse(compState.fsm.activeFSM(fsmId));
  }
  
  //**************************************************ACTIVE TO DISCONNECT*********************************************
  @Test
  public void testActiveToLocalDisconnect() {
    tc = tc.body();
    tc = startToActive(tc); //1-5
    tc = localDisconnect1(tc, torrent, file, seeder); //6-8
    tc.repeat(1).body().end();
    assertTrue(tc.check());
    Identifier fsmId = R1TransferSeeder.fsmBasicId(torrent, file, seeder.getId());
    assertFalse(compState.fsm.activeFSM(fsmId));
  }
  
  @Test
  public void testActiveToNetDisconnect() {
    tc = tc.body();
    tc = startToActive(tc); //1-5
    tc = netDisconnect(tc, torrent, file, seeder); //6-8
    tc.repeat(1).body().end();
    assertTrue(tc.check());
    Identifier fsmId = R1TransferSeeder.fsmBasicId(torrent, file, seeder.getId());
    assertFalse(compState.fsm.activeFSM(fsmId));
  }
  //********************************************************************************************************************
  private TestContext connect(TestContext tc, OverlayId torrentId, Identifier fileId, KAddress seeder) {
    tc = tc.trigger(connect(torrentId, fileId, seeder), triggerP); //1
    tc = netConnectAcc(tc); //2-3
    tc = tc.expect(R1TransferSeederEvents.Connected.class, expectP, Direction.OUT);//4
    tc = eSchedulePeriodicTimer(tc, R1TransferSeederPing.class, timerP); //5
    return tc;
  }
  
  private TestContext localDisconnect1(TestContext tc, OverlayId torrentId, Identifier fileId, KAddress seeder) {
    tc = tc.trigger(localDisconnect(torrentId, fileId, seeder), triggerP); //1
    tc = eNetPayload(tc, R1TransferMsgs.Disconnect.class, networkP); //2
    tc = tc.expect(CancelPeriodicTimeout.class, timerP, Direction.OUT); //3
    return tc;
  }
  
  private TestContext localDisconnect2(TestContext tc, OverlayId torrentId, Identifier fileId, KAddress seeder) {
    tc = tc.trigger(localDisconnect(torrentId, fileId, seeder), triggerP); //1
    tc = eNetPayload(tc, R1TransferMsgs.Disconnect.class, networkP); //2
    return tc;
  }
  
  private TestContext netDisconnect(TestContext tc, OverlayId torrentId, Identifier fileId, KAddress seeder) {
    tc = tc.trigger(netDisconnect(torrentId, fileId, seeder), networkP); //1
    tc = tc.expect(R1TransferSeederEvents.Disconnected.class, expectP, Direction.OUT); //2
    tc = tc.expect(CancelPeriodicTimeout.class, timerP, Direction.OUT); //3
    return tc;
  }
  
  private TestContext pingPong(TestContext tc, OverlayId torrentId, Identifier fileId, KAddress seeder) {
    tc = tc.trigger(ping(torrentId, fileId, seeder), timerP); //1
    tc = pingPong(tc, networkP); //2-3
    return tc;
  }
  
  private TestContext pingFail(TestContext tc, OverlayId torrentId, Identifier fileId, KAddress seeder) {
    tc = tc.trigger(ping(torrentId, fileId, seeder), timerP); //1
    tc = eNetPayload(tc, R1TransferMsgs.Ping.class, networkP); //2
    return tc;
  }
  
  private TestContext pingDisc(TestContext tc, OverlayId torrentId, Identifier fileId, KAddress seeder) {
    tc = tc.trigger(ping(torrentId, fileId, seeder), timerP); //1
    tc = tc.expect(R1TransferSeederEvents.Disconnected.class, expectP, Direction.OUT); //2
    tc = eNetPayload(tc, R1TransferMsgs.Disconnect.class, networkP);//3
    tc = tc.expect(CancelPeriodicTimeout.class, timerP, Direction.OUT); //4
    return tc;
  }
  
  private TestContext netConnectAcc(TestContext tc) {
    Future f = new FutureHelper.NetBEFuture<R1TransferMsgs.Connect>(R1TransferMsgs.Connect.class) {
      @Override
      public Msg get() {
        return msg.answer(content.accept());
      }
    };
    tc = tc.answerRequest(BasicContentMsg.class, networkP, f);
    tc = tc.trigger(f, networkP);
    return tc;
  }
  
  private TestContext pingPong(TestContext tc, Port networkP) {
    Future f = new FutureHelper.NetMsgFuture<R1TransferMsgs.Ping>(R1TransferMsgs.Ping.class) {
      @Override
      public Msg get() {
        return msg.answer(content.pong());
      }
    };
    tc = tc.answerRequest(BasicContentMsg.class, networkP, f);
    tc = tc.trigger(f, networkP);
    return tc;
  }
  
  public R1TransferSeederEvents.Connect connect(OverlayId torrentId, Identifier fileId, KAddress seeder) {
    return new R1TransferSeederEvents.Connect(torrentId, fileId, seeder);
  }
  
  public R1TransferSeederEvents.Disconnect localDisconnect(OverlayId torrentId, Identifier fileId, KAddress seeder) {
    return new R1TransferSeederEvents.Disconnect(torrentId, fileId, seeder.getId());
  }
  
  public Msg netDisconnect(OverlayId torrentId, Identifier fileId, KAddress seeder) {
    R1TransferMsgs.Disconnect disconnect = new R1TransferMsgs.Disconnect(torrentId, fileId);
    return MsgHelper.msg(seeder, self, disconnect);
  }
  
  public R1TransferSeederPing ping(OverlayId torrentId, Identifier fileId, KAddress seeder) {
    SchedulePeriodicTimeout spt = new SchedulePeriodicTimeout(R1TransferSeeder.HardCodedConfig.pingTimerPeriod, 
      R1TransferSeeder.HardCodedConfig.pingTimerPeriod);
    return new R1TransferSeederPing(spt, torrentId, fileId, seeder.getId());
  }
}
