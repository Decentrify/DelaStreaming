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
package se.sics.silk.r2conn;

import com.google.common.base.Predicate;
import java.util.Arrays;
import org.junit.After;
import static org.junit.Assert.assertTrue;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import se.sics.kompics.Component;
import se.sics.kompics.KompicsEvent;
import se.sics.kompics.Port;
import se.sics.kompics.fsm.FSMException;
import se.sics.kompics.fsm.FSMStateName;
import se.sics.kompics.fsm.event.FSMWrongState;
import se.sics.kompics.network.Msg;
import se.sics.kompics.network.Network;
import se.sics.kompics.testing.Direction;
import se.sics.kompics.testing.Future;
import se.sics.kompics.testing.TestContext;
import se.sics.kompics.util.Identifier;
import se.sics.ktoolbox.util.identifiable.BasicBuilders;
import se.sics.ktoolbox.util.identifiable.overlay.OverlayId;
import se.sics.ktoolbox.util.identifiable.overlay.OverlayIdFactory;
import se.sics.ktoolbox.util.network.KAddress;
import se.sics.ktoolbox.util.network.basic.BasicContentMsg;
import se.sics.nutil.network.bestEffort.event.BestEffortMsg;
import static se.sics.silk.MsgHelper.msg;
import se.sics.silk.SystemHelper;
import se.sics.silk.SystemSetup;
import se.sics.silk.mocktimer.MockTimerComp;
import se.sics.silk.r2conn.R2ConnSeeder;
import se.sics.silk.r2conn.R2ConnSeederPort;
import se.sics.silk.r2conn.R2MngrWrapperComp;
import se.sics.silk.r2conn.event.R2ConnSeederEvents;
import se.sics.silk.r2conn.msg.R2ConnMsgs;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class R2ConnSeederTest {

  private TestContext<R2MngrWrapperComp> tc;
  private Component r2MngrComp;
  private Port<R2ConnSeederPort> connP;
  private Port<Network> networkP;
  private Port<MockTimerComp.Port> auxP;
  private static OverlayIdFactory torrentIdFactory;
  private KAddress selfAdr;

  @BeforeClass
  public static void setup() throws FSMException {
    torrentIdFactory = SystemSetup.systemSetup("src/test/resources/application.conf");
  }

  @Before
  public void testSetup() {
    tc = getContext();
    r2MngrComp = tc.getComponentUnderTest();
    connP = r2MngrComp.getPositive(R2ConnSeederPort.class);
    networkP = r2MngrComp.getNegative(Network.class);
    auxP = r2MngrComp.getPositive(MockTimerComp.Port.class);
  }

  private TestContext<R2MngrWrapperComp> getContext() {
    selfAdr = SystemHelper.getAddress(0);
    R2MngrWrapperComp.Init init = new R2MngrWrapperComp.Init(selfAdr);
    TestContext<R2MngrWrapperComp> context = TestContext.newInstance(R2MngrWrapperComp.class, init);
    return context;
  }

  @After
  public void clean() {
  }

  @Test
  public void testBadStatesAtStart() {
    OverlayId torrent1 = torrentIdFactory.id(new BasicBuilders.IntBuilder(1));
    KAddress seeder = SystemHelper.getAddress(1);

    tc = tc.body();
    tc = connWrongEventAtStart(tc, localDiscP(seeder, torrent1)); //1-4
    tc = netWrongMsgAtStart(tc, msg(selfAdr, seeder, netBETimeout())); //5-7
    tc = netWrongMsgAtStart(tc, msg(seeder, selfAdr, netConnAcc())); //8-10
    tc = netWrongMsgAtStart(tc, msg(seeder, selfAdr, netConnRej())); //11-13
    tc = netWrongMsgAtStart(tc, msg(seeder, selfAdr, netPong())); //14-16
    tc = netWrongMsgAtStart(tc, msg(seeder, selfAdr, netDiscAck())); //17-19
    tc = prepareAuxTimer(tc, seeder, torrent1);
    tc = timerWrongAtStart(tc, seeder.getId());
    tc.repeat(1).body().end();
    assertTrue(tc.check());
  }

  @Test
  public void testTimeline1() {
    OverlayId torrent1 = torrentIdFactory.id(new BasicBuilders.IntBuilder(1));
    OverlayId torrent2 = torrentIdFactory.id(new BasicBuilders.IntBuilder(2));
    OverlayId torrent3 = torrentIdFactory.id(new BasicBuilders.IntBuilder(3));
    OverlayId torrent4 = torrentIdFactory.id(new BasicBuilders.IntBuilder(4));
    KAddress seeder = SystemHelper.getAddress(1);

    tc = tc.body();
    tc = compConnSuccAux1(tc, seeder, new OverlayId[]{torrent1, torrent2, torrent3}); //1-13
    tc = localConnSucc(tc, seeder, torrent4); //14-17
    tc = localDisc(tc, seeder, new OverlayId[]{torrent2, torrent3, torrent4}); //18-23
    tc = netDiskAckAux1(tc, seeder, torrent1);
    tc = compConnSucc(tc, seeder, torrent1);
    tc.repeat(1).body().end();
    assertTrue(tc.check());
  }

  /*
   * networkP timeout on connect
   */
  @Test
  public void testTimeline2() {
    OverlayId torrent1 = torrentIdFactory.id(new BasicBuilders.IntBuilder(1));
    KAddress seeder = SystemHelper.getAddress(1);

    tc = tc.body();
    tc = compConnTimeout(tc, seeder, torrent1);
    tc.repeat(1).body().end();
    assertTrue(tc.check());
  }

  @Test
  public void testConnRej() {
    OverlayId torrent1 = torrentIdFactory.id(new BasicBuilders.IntBuilder(1));
    KAddress seeder = SystemHelper.getAddress(1);

    tc = tc.body();
    tc = compConnFail(tc, seeder, torrent1);
    tc.repeat(1).body().end();
    assertTrue(tc.check());
  }

  @Test
  public void testConnPing() {
    OverlayId torrent1 = torrentIdFactory.id(new BasicBuilders.IntBuilder(1));
    KAddress seeder = SystemHelper.getAddress(1);

    tc = tc.body();
    tc = compConnSucc(tc, seeder, torrent1);
    tc = netPingSuccess(tc, seeder.getId());
    tc = netPingMissed(tc, 2);
    tc = netPingSuccess(tc, seeder.getId());
    tc = netPingMissed(tc, 5); //6 consecutive missed pings triggers disconnect
    tc = netPingDisc(tc, seeder, new OverlayId[]{torrent1});
    tc = tc.inspect(inactiveFSM(seeder.getId()));
    tc.repeat(1).body().end();
    assertTrue(tc.check());
  }

  private TestContext<R2MngrWrapperComp> connectRej(TestContext<R2MngrWrapperComp> tc, Identifier seederId) {
    Future<Msg, Msg> connRej = connRejF();
    return tc
      .inspect(state(seederId, R2ConnSeeder.States.CONNECTING))
      .answerRequest(Msg.class, networkP, connRej)
      .trigger(connRej, networkP);
  }

  private TestContext<R2MngrWrapperComp> connectTout(TestContext<R2MngrWrapperComp> tc, Identifier seederId) {
    Future<Msg, Msg> connTout = connToutF();
    return tc
      .inspect(state(seederId, R2ConnSeeder.States.CONNECTING))
      .answerRequest(Msg.class, networkP, connTout)
      .trigger(connTout, networkP);
  }

  private TestContext<R2MngrWrapperComp> prepareAuxTimer(TestContext<R2MngrWrapperComp> tc,
    KAddress seeder, OverlayId torrentId) {
    tc = compConnSucc(tc, seeder, torrentId); //required to setup the timer
    tc = compDiscSucc(tc, seeder, torrentId); //destroying the created fsm - the aux component maintains the timer to emulate a late timeout
    return tc;
  }

  private TestContext<R2MngrWrapperComp> compConnSucc(TestContext<R2MngrWrapperComp> tc,
    KAddress seeder, OverlayId torrentId) {
    tc = tc
      .inspect(inactiveFSM(seeder.getId()))
      .trigger(localConnReq(seeder, torrentId), connP);
    tc = netConnSucc(tc, seeder.getId());
    tc = tc
      .expect(R2ConnSeederEvents.ConnectSuccess.class, connSuccP(seeder.getId(), torrentId), connP, Direction.OUT);
    return tc;
  }
  
  private TestContext<R2MngrWrapperComp> compConnSuccAux1(TestContext<R2MngrWrapperComp> tc,
    KAddress seeder, OverlayId[] torrents) {
    assertTrue(torrents.length > 0);
    Future<Msg, Msg> connAcc = connAccF();
    tc = tc
      .inspect(inactiveFSM(seeder.getId()))
      .trigger(localConnReq(seeder, torrents[0]), connP)
      .inspect(state(seeder.getId(), R2ConnSeeder.States.CONNECTING))
      .answerRequest(Msg.class, networkP, connAcc);
    for(int i = 1; i < torrents.length; i++ ) {
      tc = tc.trigger(localConnReq(seeder, torrents[i]), connP);
    }
    tc
      .trigger(localDiscP(seeder, torrents[0]), connP)
      .trigger(connAcc, networkP)
      .inspect(state(seeder.getId(), R2ConnSeeder.States.CONNECTED));
    OverlayId[] expectedT = Arrays.copyOfRange(torrents, 1, torrents.length);
    tc = localUnnorderedConnSucc(tc, seeder, expectedT);
    tc = tc.inspect(connected(seeder.getId(), torrents.length-1));
    return tc;
  }
  
  private TestContext<R2MngrWrapperComp> compConnFail(TestContext<R2MngrWrapperComp> tc,
    KAddress seeder, OverlayId torrentId) {
    tc = tc
      .inspect(inactiveFSM(seeder.getId()))
      .trigger(localConnReq(seeder, torrentId), connP);
    tc = connectRej(tc, seeder.getId());
    tc = tc
      .expect(R2ConnSeederEvents.ConnectFail.class, connFailP(seeder.getId(), torrentId), connP, Direction.OUT)
      .inspect(inactiveFSM(seeder.getId()));
    return tc;
  }
  
  private TestContext<R2MngrWrapperComp> compConnTimeout(TestContext<R2MngrWrapperComp> tc,
    KAddress seeder, OverlayId torrentId) {
    tc = tc
      .inspect(inactiveFSM(seeder.getId()))
      .trigger(localConnReq(seeder, torrentId), connP);
    tc = connectTout(tc, seeder.getId());
    tc = tc
      .expect(R2ConnSeederEvents.ConnectFail.class, connFailP(seeder.getId(), torrentId), connP, Direction.OUT)
      .inspect(inactiveFSM(seeder.getId()));
    return tc;
  }
  
  private TestContext<R2MngrWrapperComp> compDiscSucc(TestContext<R2MngrWrapperComp> tc,
    KAddress seeder, OverlayId torrentId) {
    Future<Msg, Msg> discAck = discAckF();
    return tc
      .inspect(state(seeder.getId(), R2ConnSeeder.States.CONNECTED))
      .trigger(localDiscP(seeder, torrentId), connP)
      .answerRequest(Msg.class, networkP, discAck)
      .inspect(state(seeder.getId(), R2ConnSeeder.States.DISCONNECTING))
      .trigger(discAck, networkP)
      .inspect(inactiveFSM(seeder.getId()));
  }
  
  private TestContext<R2MngrWrapperComp> localConnSucc(TestContext<R2MngrWrapperComp> tc,
    KAddress seeder, OverlayId torrentId) {
    tc
      .inspect(state(seeder.getId(), R2ConnSeeder.States.CONNECTED))
      .trigger(localConnReq(seeder, torrentId), connP)
      .expect(R2ConnSeederEvents.ConnectSuccess.class, connSuccP(seeder.getId(), torrentId), connP, Direction.OUT)
      .inspect(state(seeder.getId(), R2ConnSeeder.States.CONNECTED));
    return tc;
  }

  private TestContext<R2MngrWrapperComp> localUnnorderedConnSucc(TestContext<R2MngrWrapperComp> tc,
    KAddress seeder, OverlayId[] torrentIds) {
    tc = tc.unordered();
    for(OverlayId torrentId : torrentIds) {
      tc = tc.expect(R2ConnSeederEvents.ConnectSuccess.class, connSuccP(seeder.getId(), torrentId), connP, Direction.OUT);
    }
    tc = tc.end();
    return tc;
  }
  
  private TestContext<R2MngrWrapperComp> localDisc(TestContext<R2MngrWrapperComp> tc,
    KAddress seeder, OverlayId torrentId) {

    return tc
      .inspect(state(seeder.getId(), R2ConnSeeder.States.CONNECTED))
      .trigger(localDiscP(seeder, torrentId), connP);
  }
  
  private TestContext<R2MngrWrapperComp> localDisc(TestContext<R2MngrWrapperComp> tc,
    KAddress seeder, OverlayId[] torrentIds) {
    for(OverlayId torrentId : torrentIds) {
      tc = localDisc(tc, seeder, torrentId);
    }
    return tc;
  }
  
  private TestContext<R2MngrWrapperComp> localUnnorderedDisc(TestContext<R2MngrWrapperComp> tc,
    KAddress seeder, OverlayId[] torrentIds) {
    tc = tc.unordered();
    for(OverlayId torrentId : torrentIds) {
      tc = tc.expect(R2ConnSeederEvents.ConnectFail.class, connFailP(seeder.getId(), torrentId), connP, Direction.OUT);
    }
    tc = tc.end();
    return tc;
  }

  private TestContext<R2MngrWrapperComp> netConnSucc(TestContext<R2MngrWrapperComp> tc, Identifier seederId) {
    Future<Msg, Msg> connAcc = connAccF();
    return tc
      .inspect(state(seederId, R2ConnSeeder.States.CONNECTING))
      .answerRequest(Msg.class, networkP, connAcc)
      .trigger(connAcc, networkP)
      .inspect(state(seederId, R2ConnSeeder.States.CONNECTED));
  }
  
  
  private TestContext<R2MngrWrapperComp> netDiskAck(TestContext<R2MngrWrapperComp> tc, Identifier seederId) {
    Future<Msg, Msg> discAck = discAckF();
    return tc
      .inspect(state(seederId, R2ConnSeeder.States.DISCONNECTING))
      .answerRequest(Msg.class, networkP, discAck)
      .trigger(discAck, networkP)
      .inspect(inactiveFSM(seederId));
  }
  
  private TestContext<R2MngrWrapperComp> netDiskAckAux1(TestContext<R2MngrWrapperComp> tc, 
    KAddress seeder, OverlayId torrentId) {
    Future<Msg, Msg> discAck = discAckF();
    return tc
      .answerRequest(Msg.class, networkP, discAck)
      .inspect(state(seeder.getId(), R2ConnSeeder.States.DISCONNECTING))
      .trigger(localConnReq(seeder, torrentId), connP)
      .expect(R2ConnSeederEvents.ConnectFail.class, connFailP(seeder.getId(), torrentId), connP, Direction.OUT)
      .trigger(discAck, networkP)
      .inspect(inactiveFSM(seeder.getId()));
  }
  
  private TestContext<R2MngrWrapperComp> netPingMissed(TestContext<R2MngrWrapperComp> tc, int missedPings) {
    return netPingMissed(tc.repeat(missedPings).body()).end();
  }

  private TestContext<R2MngrWrapperComp> netPingMissed(TestContext<R2MngrWrapperComp> tc) {
    return tc.trigger(timerAux(), auxP)
      .answerRequest(Msg.class, networkP, connPingF());
  }

  private TestContext<R2MngrWrapperComp> netPingDisc(TestContext<R2MngrWrapperComp> tc, 
    KAddress seeder, OverlayId[] torrentIds) {
    tc = tc.trigger(timerAux(), auxP);
    tc = localUnnorderedDisc(tc, seeder, torrentIds);
    return tc;
  }

  private TestContext<R2MngrWrapperComp> netPingSuccess(TestContext<R2MngrWrapperComp> tc, Identifier seederId) {
    Future<Msg, Msg> connPing = connPingF();
    return tc
      .inspect(state(seederId, R2ConnSeeder.States.CONNECTED))
      .trigger(timerAux(), auxP)
      .answerRequest(Msg.class, networkP, connPing)
      .trigger(connPing, networkP)
      .inspect(state(seederId, R2ConnSeeder.States.CONNECTED));
  }

  private TestContext<R2MngrWrapperComp> timerWrongAtStart(TestContext<R2MngrWrapperComp> tc, Identifier seederId) {
    return tc
      .inspect(inactiveFSM(seederId))
      .trigger(timerAux(), auxP)
      .inspect(inactiveFSM(seederId));
  }

  private TestContext<R2MngrWrapperComp> netWrongMsgAtStart(TestContext<R2MngrWrapperComp> tc, BasicContentMsg msg) {
    return tc
      .inspect(inactiveFSM(msg.getSource().getId()))
      .trigger(msg, networkP)
      .inspect(inactiveFSM(msg.getSource().getId()));
  }

  private TestContext<R2MngrWrapperComp> connWrongEventAtStart(TestContext<R2MngrWrapperComp> tc,
    R2ConnSeeder.Event event) {
    return tc
      .inspect(inactiveFSM(event.getConnSeederFSMId()))
      .trigger(event, connP)
      .expect(FSMWrongState.class, connP, Direction.OUT)
      .inspect(inactiveFSM(event.getConnSeederFSMId()));
  }

  public static abstract class BestEffortFuture<C extends KompicsEvent> extends Future<Msg, Msg> {

    private Class<C> contentType;
    protected BasicContentMsg msg;
    protected BestEffortMsg.Request wrap;
    protected C content;

    BestEffortFuture(Class<C> contentType) {
      this.contentType = contentType;
    }

    @Override
    public boolean set(Msg r) {
      if (!(r instanceof BasicContentMsg)) {
        return false;
      }
      this.msg = (BasicContentMsg) r;
      if (!(msg.getContent() instanceof BestEffortMsg.Request)) {
        return false;
      }
      wrap = (BestEffortMsg.Request) msg.getContent();
      if (!(contentType.isAssignableFrom(wrap.extractValue().getClass()))) {
        return false;
      }
      this.content = (C) wrap.extractValue();
      return true;
    }

  }

  Future<Msg, Msg> connAccF() {
    return new BestEffortFuture<R2ConnMsgs.ConnectReq>(R2ConnMsgs.ConnectReq.class) {
      @Override
      public BasicContentMsg get() {
        return msg.answer(content.accept());
      }
    };
  }

  Future<Msg, Msg> discAckF() {
    return new BestEffortFuture<R2ConnMsgs.Disconnect>(R2ConnMsgs.Disconnect.class) {
      @Override
      public BasicContentMsg get() {
        return msg.answer(content.ack());
      }
    };
  }

  Future<Msg, Msg> connToutF() {
    return new BestEffortFuture<R2ConnMsgs.ConnectReq>(R2ConnMsgs.ConnectReq.class) {
      @Override
      public BasicContentMsg get() {
        return msg.answer(wrap.timeout());
      }
    };
  }

  Future<Msg, Msg> connRejF() {
    return new BestEffortFuture<R2ConnMsgs.ConnectReq>(R2ConnMsgs.ConnectReq.class) {
      @Override
      public BasicContentMsg get() {
        return msg.answer(content.reject());
      }
    };
  }

  Future<Msg, Msg> connPingF() {
    return new BestEffortFuture<R2ConnMsgs.Ping>(R2ConnMsgs.Ping.class) {
      @Override
      public BasicContentMsg get() {
        return msg.answer(content.ack());
      }
    };
  }

  Predicate<R2ConnSeederEvents.ConnectSuccess> connSuccP(Identifier seederId, OverlayId torrentId) {
    return (R2ConnSeederEvents.ConnectSuccess t) -> {
      return t.seederId.equals(seederId) && t.torrentId.equals(torrentId);
    };
  }

  Predicate<R2ConnSeederEvents.ConnectFail> connFailP(Identifier seederId, OverlayId torrentId) {
    return (R2ConnSeederEvents.ConnectFail t) -> {
      return t.seederId.equals(seederId) && t.torrentId.equals(torrentId);
    };
  }
  
  Predicate<R2MngrWrapperComp> state(Identifier seederId, FSMStateName expectedState) {
    return (R2MngrWrapperComp t) -> {
      FSMStateName currentState = t.getConnSeederState(seederId);
      return currentState.equals(expectedState);
    };
  }

  Predicate<R2MngrWrapperComp> connected(Identifier seederId, int nrTorrents) {
    return (R2MngrWrapperComp t) -> {
      R2ConnSeeder.IS is = (R2ConnSeeder.IS) t.getConnSeederIS(seederId);
      return is.connected.size() == nrTorrents;
    };
  }

  Predicate<R2MngrWrapperComp> inactiveFSM(Identifier seederId) {
    return (R2MngrWrapperComp t) -> !t.activeSeederFSM(seederId);
  }
  
  private BestEffortMsg.Timeout netBETimeout() {
    R2ConnMsgs.Ping content = new R2ConnMsgs.Ping();
    BestEffortMsg.Request be = new BestEffortMsg.Request(content, 1, 1);
    return be.timeout();
  }

  private R2ConnMsgs.ConnectAcc netConnAcc() {
    R2ConnMsgs.ConnectReq req = new R2ConnMsgs.ConnectReq();
    return req.accept();
  }

  private R2ConnMsgs.ConnectRej netConnRej() {
    R2ConnMsgs.ConnectReq req = new R2ConnMsgs.ConnectReq();
    return req.reject();
  }

  private R2ConnMsgs.Pong netPong() {
    R2ConnMsgs.Ping req = new R2ConnMsgs.Ping();
    return req.ack();
  }

  private R2ConnMsgs.DisconnectAck netDiscAck() {
    R2ConnMsgs.Disconnect req = new R2ConnMsgs.Disconnect();
    return req.ack();
  }

  private R2ConnSeederEvents.ConnectReq localConnReq(KAddress seeder, OverlayId torrent) {
    return new R2ConnSeederEvents.ConnectReq(torrent, seeder);
  }

  private R2ConnSeederEvents.Disconnect localDiscP(KAddress seeder, OverlayId torrent) {
    return new R2ConnSeederEvents.Disconnect(torrent, seeder.getId());
  }

  private MockTimerComp.TriggerTimeout timerAux() {
    return new MockTimerComp.TriggerTimeout();
  }
}
