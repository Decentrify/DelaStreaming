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
package se.sics.silk.r2mngr;

import com.google.common.base.Predicate;
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
import se.sics.silk.SystemHelper;
import se.sics.silk.SystemSetup;
import se.sics.silk.r2mngr.event.ConnSeederEvents;
import se.sics.silk.r2mngr.msg.ConnMsgs;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class ConnSeederTest {

  private TestContext<R2MngrWrapperComp> tc;
  private Component r2MngrComp;
  private Port<ConnSeederPort> connP;
  private Port<Network> networkP;
  private Port<R2MngrWrapperComp.Port> auxP;
  private static OverlayIdFactory torrentIdFactory;

  @BeforeClass
  public static void setup() throws FSMException {
    torrentIdFactory = SystemSetup.systemSetup("src/test/resources/application.conf");
  }

  @Before
  public void testSetup() {
    tc = getContext();
    r2MngrComp = tc.getComponentUnderTest();
    connP = r2MngrComp.getPositive(ConnSeederPort.class);
    networkP = r2MngrComp.getNegative(Network.class);
    auxP = r2MngrComp.getPositive(R2MngrWrapperComp.Port.class);
  }

  private static TestContext<R2MngrWrapperComp> getContext() {
    KAddress selfAdr = SystemHelper.getAddress(0);
    R2MngrWrapperComp.Init init = new R2MngrWrapperComp.Init(selfAdr);
    TestContext<R2MngrWrapperComp> context = TestContext.newInstance(R2MngrWrapperComp.class, init);
    return context;
  }

  @After
  public void clean() {
  }

  @Test
  public void testTimeline1() {
    OverlayId torrent1 = torrentIdFactory.id(new BasicBuilders.IntBuilder(1));
    OverlayId torrent2 = torrentIdFactory.id(new BasicBuilders.IntBuilder(2));
    OverlayId torrent3 = torrentIdFactory.id(new BasicBuilders.IntBuilder(3));
    OverlayId torrent4 = torrentIdFactory.id(new BasicBuilders.IntBuilder(4));
    KAddress seeder = SystemHelper.getAddress(1);

    Future<Msg, Msg> connAcc1 = connAcc();
    Future<Msg, Msg> discAck1 = discAck();
    tc = tc.body()
      .trigger(new ConnSeederEvents.Connect(torrent1, seeder), connP) //1 
      .inspect(state(seeder.getId(), ConnSeeder.States.CONNECTING)) //2
      .answerRequest(Msg.class, networkP, connAcc1) //3
      .trigger(new ConnSeederEvents.Connect(torrent2, seeder), connP) //4
      .trigger(new ConnSeederEvents.Disconnect(torrent2, seeder.getId()), connP) //5
      .trigger(new ConnSeederEvents.Connect(torrent3, seeder), connP) //6
      .inspect(connected(seeder.getId(), 0)) //7
      .trigger(connAcc1, networkP) //8
      .inspect(state(seeder.getId(), ConnSeeder.States.CONNECTED)) //9
      .unordered()
      .expect(ConnSeederEvents.ConnectSuccess.class, connSucc(torrent1), connP, Direction.OUT) //10
      .expect(ConnSeederEvents.ConnectSuccess.class, connSucc(torrent3), connP, Direction.OUT) //11
      .end()
      .inspect(connected(seeder.getId(), 2)) //12
      .trigger(new ConnSeederEvents.Connect(torrent4, seeder), connP) //13
      .expect(ConnSeederEvents.ConnectSuccess.class, connSucc(torrent4), connP, Direction.OUT) //14
      .trigger(new ConnSeederEvents.Disconnect(torrent4, seeder.getId()), connP) //15
      .inspect(connected(seeder.getId(), 2)) //16
      .trigger(new ConnSeederEvents.Disconnect(torrent1, seeder.getId()), connP) //17
      .trigger(new ConnSeederEvents.Disconnect(torrent3, seeder.getId()), connP) //18
      .inspect(connected(seeder.getId(), 0)) //19
      .inspect(state(seeder.getId(), ConnSeeder.States.DISCONNECTING)) //20
      .answerRequest(Msg.class, networkP, discAck1) //21
      .trigger(new ConnSeederEvents.Connect(torrent1, seeder), connP) //22
      .expect(ConnSeederEvents.ConnectFail.class, connFail(torrent1), connP, Direction.OUT) //23
      .trigger(discAck1, networkP) //24
      .inspect(inactiveSeederFSM(seeder.getId())); //25
      //previous FSM is destroyed and we can create a new one
    tc = tc
      .trigger(new ConnSeederEvents.Connect(torrent1, seeder), connP); //26
    tc = connectAcc(tc, seeder.getId()); //27-30
    tc
      .expect(ConnSeederEvents.ConnectSuccess.class, connSucc(torrent1), connP, Direction.OUT) //31
      .repeat(1).body().end();

    assertTrue(tc.check());
  }

  /*
   * networkP timeout on connect
   */
  @Test
  public void testTimeline2() {
    OverlayId torrent1 = torrentIdFactory.id(new BasicBuilders.IntBuilder(1));
    KAddress seeder = SystemHelper.getAddress(1);

    Future<Msg, Msg> connTimeout1 = connTout();
    tc = tc.body()
      .trigger(new ConnSeederEvents.Connect(torrent1, seeder), connP);
    tc = connectTout(tc, seeder.getId());
    tc
      .expect(ConnSeederEvents.ConnectFail.class, connFail(torrent1), connP, Direction.OUT)
      .inspect(inactiveSeederFSM(seeder.getId()))
      .repeat(1).body().end();

    assertTrue(tc.check());
  }

  @Test
  public void testConnRej() {
    OverlayId torrent1 = torrentIdFactory.id(new BasicBuilders.IntBuilder(1));
    KAddress seeder = SystemHelper.getAddress(1);

    tc = tc.body()
      .trigger(new ConnSeederEvents.Connect(torrent1, seeder), connP);
    tc = connectRej(tc, seeder.getId());
    tc
      .expect(ConnSeederEvents.ConnectFail.class, connFail(torrent1), connP, Direction.OUT)
      .inspect(inactiveSeederFSM(seeder.getId()))
      .repeat(1).body().end();

    assertTrue(tc.check());
  }

  @Test
  public void testConnPing() {
    OverlayId torrent1 = torrentIdFactory.id(new BasicBuilders.IntBuilder(1));
    KAddress seeder = SystemHelper.getAddress(1);

    tc = tc.body()
      .trigger(new ConnSeederEvents.Connect(torrent1, seeder), connP);
    tc = connectAcc(tc, seeder.getId());
    tc = tc
      .expect(ConnSeederEvents.ConnectSuccess.class, connSucc(torrent1), connP, Direction.OUT);
    tc = pingSuccess(tc, seeder.getId());
    tc = pingMissed(tc);
    tc = pingSuccess(tc, seeder.getId());
    //missed five ping timeouts
    tc = pingMissed(tc.repeat(5).body()).end();
    //kill ping
    tc
      .trigger(new R2MngrWrapperComp.TriggerTimeout(), auxP)
      .expect(ConnSeederEvents.ConnectFail.class, connFail(torrent1), connP, Direction.OUT)
      .repeat(1).body().end();

    assertTrue(tc.check());
  }

  private TestContext<R2MngrWrapperComp> connectAcc(TestContext<R2MngrWrapperComp> tc, Identifier seederId) {
    Future<Msg, Msg> connAcc = connAcc();
    return tc
      .inspect(state(seederId, ConnSeeder.States.CONNECTING))
      .answerRequest(Msg.class, networkP, connAcc)
      .trigger(connAcc, networkP)
      .inspect(state(seederId, ConnSeeder.States.CONNECTED));
  }

  private TestContext<R2MngrWrapperComp> connectRej(TestContext<R2MngrWrapperComp> tc, Identifier seederId) {
    Future<Msg, Msg> connRej = connRej();
    return tc
      .inspect(state(seederId, ConnSeeder.States.CONNECTING))
      .answerRequest(Msg.class, networkP, connRej)
      .trigger(connRej, networkP);
  }

  private TestContext<R2MngrWrapperComp> connectTout(TestContext<R2MngrWrapperComp> tc, Identifier seederId) {
    Future<Msg, Msg> connTout = connTout();
    return tc
      .inspect(state(seederId, ConnSeeder.States.CONNECTING))
      .answerRequest(Msg.class, networkP, connTout)
      .trigger(connTout, networkP);
  }

  private TestContext<R2MngrWrapperComp> pingMissed(TestContext<R2MngrWrapperComp> tc) {
    return tc.trigger(new R2MngrWrapperComp.TriggerTimeout(), auxP)
      .answerRequest(Msg.class, networkP, connPing());
  }

  private TestContext<R2MngrWrapperComp> pingSuccess(TestContext<R2MngrWrapperComp> tc, Identifier seederId) {
    Future<Msg, Msg> connPing = connPing();
    return tc
      .inspect(state(seederId, ConnSeeder.States.CONNECTED))
      .trigger(new R2MngrWrapperComp.TriggerTimeout(), auxP)
      .answerRequest(Msg.class, networkP, connPing)
      .trigger(connPing, networkP)
      .inspect(state(seederId, ConnSeeder.States.CONNECTED));
  }

  Predicate<R2MngrWrapperComp> state(Identifier seederId, FSMStateName expectedState) {
    return (R2MngrWrapperComp t) -> {
      FSMStateName currentState = t.getConnSeederState(seederId);
      return currentState.equals(expectedState);
    };
  }

  Predicate<R2MngrWrapperComp> connected(Identifier seederId, int nrTorrents) {
    return (R2MngrWrapperComp t) -> {
      ConnSeeder.IS is = (ConnSeeder.IS) t.getConnSeederIS(seederId);
      return is.connected.size() == nrTorrents;
    };
  }

  Predicate<R2MngrWrapperComp> inactiveSeederFSM(Identifier seederId) {
    return (R2MngrWrapperComp t) ->  !t.activeSeederFSM(seederId);
  }

  Predicate<ConnSeederEvents.ConnectSuccess> connSucc(OverlayId torrentId) {
    return (ConnSeederEvents.ConnectSuccess t) -> t.torrentId.equals(torrentId);
  }

  Predicate<ConnSeederEvents.ConnectFail> connFail(OverlayId torrentId) {
    return (ConnSeederEvents.ConnectFail t) ->  t.torrentId.equals(torrentId);
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

  Future<Msg, Msg> connAcc() {
    return new BestEffortFuture<ConnMsgs.ConnectReq>(ConnMsgs.ConnectReq.class) {
      @Override
      public BasicContentMsg get() {
        return msg.answer(content.accept());
      }
    };
  }

  Future<Msg, Msg> discAck() {
    return new BestEffortFuture<ConnMsgs.Disconnect>(ConnMsgs.Disconnect.class) {
      @Override
      public BasicContentMsg get() {
        return msg.answer(content.ack());
      }
    };
  }

  Future<Msg, Msg> connTout() {
    return new BestEffortFuture<ConnMsgs.ConnectReq>(ConnMsgs.ConnectReq.class) {
      @Override
      public BasicContentMsg get() {
        return msg.answer(wrap.timeout());
      }
    };
  }

  Future<Msg, Msg> connRej() {
    return new BestEffortFuture<ConnMsgs.ConnectReq>(ConnMsgs.ConnectReq.class) {
      @Override
      public BasicContentMsg get() {
        return msg.answer(content.reject());
      }
    };
  }

  Future<Msg, Msg> connPing() {
    return new BestEffortFuture<ConnMsgs.Ping>(ConnMsgs.Ping.class) {
      @Override
      public BasicContentMsg get() {
        return msg.answer(content.ack());
      }
    };
  }
}
