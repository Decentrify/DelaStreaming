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
import se.sics.silk.r2mngr.msg.ConnSeederMsgs;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class ConnMngrCompTest {

  private TestContext<R2MngrWrapperComp> tc;
  private Component connMngrComp;
  private static OverlayIdFactory torrentIdFactory;

  @BeforeClass
  public static void setup() throws FSMException {
    torrentIdFactory = SystemSetup.systemSetup("src/test/resources/application.conf");
  }

  @Before
  public void testSetup() {
    tc = getContext();
    connMngrComp = tc.getComponentUnderTest();
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
    Port<ConnPort> port1 = connMngrComp.getPositive(ConnPort.class);
    Port<Network> port2 = connMngrComp.getNegative(Network.class);
    OverlayId torrent1 = torrentIdFactory.id(new BasicBuilders.IntBuilder(1));
    OverlayId torrent2 = torrentIdFactory.id(new BasicBuilders.IntBuilder(2));
    OverlayId torrent3 = torrentIdFactory.id(new BasicBuilders.IntBuilder(3));
    OverlayId torrent4 = torrentIdFactory.id(new BasicBuilders.IntBuilder(4));
    KAddress seeder = SystemHelper.getAddress(1);

    Future<Msg, Msg> connAcc1 = connAcc();
    Future<Msg, Msg> discAck1 = discAck();
    Future<Msg, Msg> connAcc2 = connAcc();
    tc.body()
      .trigger(new ConnSeederEvents.Connect(torrent1, seeder), port1) //1 
      .inspect(state(seeder.getId(), ConnSeeder.States.CONNECTING)) //2
      .answerRequest(Msg.class, port2, connAcc1) //3
      .trigger(new ConnSeederEvents.Connect(torrent2, seeder), port1) //4
      .trigger(new ConnSeederEvents.Disconnect(torrent2, seeder.getId()), port1) //5
      .trigger(new ConnSeederEvents.Connect(torrent3, seeder), port1) //6
      .inspect(connected(seeder.getId(), 0)) //7
      .trigger(connAcc1, port2) //8
      .inspect(state(seeder.getId(), ConnSeeder.States.CONNECTED)) //9
      .unordered()
      .expect(ConnSeederEvents.ConnectSuccess.class, connSucc(torrent1), port1, Direction.OUT) //10
      .expect(ConnSeederEvents.ConnectSuccess.class, connSucc(torrent3), port1, Direction.OUT) //11
      .end()
      .inspect(connected(seeder.getId(), 2)) //12
      .trigger(new ConnSeederEvents.Connect(torrent4, seeder), port1) //13
      .expect(ConnSeederEvents.ConnectSuccess.class, connSucc(torrent4), port1, Direction.OUT) //14
      .trigger(new ConnSeederEvents.Disconnect(torrent4, seeder.getId()), port1) //15
      .inspect(connected(seeder.getId(), 2)) //16
      .trigger(new ConnSeederEvents.Disconnect(torrent1, seeder.getId()), port1) //17
      .trigger(new ConnSeederEvents.Disconnect(torrent3, seeder.getId()), port1) //18
      .inspect(connected(seeder.getId(), 0)) //19
      .inspect(state(seeder.getId(), ConnSeeder.States.DISCONNECTING)) //20
      .answerRequest(Msg.class, port2, discAck1) //21
      .trigger(new ConnSeederEvents.Connect(torrent1, seeder), port1) //22
      .expect(ConnSeederEvents.ConnectFail.class, connFail(torrent1), port1, Direction.OUT) //23
      .trigger(discAck1, port2) //24
      .inspect(inactiveSeederFSM(seeder.getId())) //25
      //previous FSM is destroyed and we can create a new one
      .trigger(new ConnSeederEvents.Connect(torrent1, seeder), port1) //26
      .inspect(state(seeder.getId(), ConnSeeder.States.CONNECTING)) //27
      .answerRequest(Msg.class, port2, connAcc2) //28
      .trigger(connAcc2, port2) //29
      .inspect(state(seeder.getId(), ConnSeeder.States.CONNECTED)) //30
      .expect(ConnSeederEvents.ConnectSuccess.class, connSucc(torrent1), port1, Direction.OUT) //31
      .repeat(1).body().end();

    assertTrue(tc.check());
  }

  /*
   * network timeout on connect
   */
  @Test
  public void testTimeline2() {
    Port<ConnPort> port1 = connMngrComp.getPositive(ConnPort.class);
    Port<Network> port2 = connMngrComp.getNegative(Network.class);
    OverlayId torrent1 = torrentIdFactory.id(new BasicBuilders.IntBuilder(1));
    KAddress seeder = SystemHelper.getAddress(1);

    Future<Msg, Msg> connTimeout1 = connTout();
    tc.body()
      .trigger(new ConnSeederEvents.Connect(torrent1, seeder), port1)
      .inspect(state(seeder.getId(), ConnSeeder.States.CONNECTING))
      .answerRequest(Msg.class, port2, connTimeout1)
      .trigger(connTimeout1, port2)
      .expect(ConnSeederEvents.ConnectFail.class, connFail(torrent1), port1, Direction.OUT)
      .inspect(inactiveSeederFSM(seeder.getId()))
      .repeat(1).body().end();

    assertTrue(tc.check());
  }

  @Test
  public void testConnRej() {
    Port<ConnPort> port1 = connMngrComp.getPositive(ConnPort.class);
    Port<Network> port2 = connMngrComp.getNegative(Network.class);
    OverlayId torrent1 = torrentIdFactory.id(new BasicBuilders.IntBuilder(1));
    KAddress seeder = SystemHelper.getAddress(1);

    Future<Msg, Msg> connRej1 = connRej();
    tc.body()
      .trigger(new ConnSeederEvents.Connect(torrent1, seeder), port1)
      .inspect(state(seeder.getId(), ConnSeeder.States.CONNECTING))
      .answerRequest(Msg.class, port2, connRej1)
      .trigger(connRej1, port2)
      .expect(ConnSeederEvents.ConnectFail.class, connFail(torrent1), port1, Direction.OUT)
      .inspect(inactiveSeederFSM(seeder.getId()))
      .repeat(1).body().end();

    assertTrue(tc.check());
  }

  @Test
  public void testConnPing() {
    Port<ConnPort> port1 = connMngrComp.getPositive(ConnPort.class);
    Port<Network> port2 = connMngrComp.getNegative(Network.class);
    Port<R2MngrWrapperComp.Port> port3 = connMngrComp.getPositive(R2MngrWrapperComp.Port.class);
    OverlayId torrent1 = torrentIdFactory.id(new BasicBuilders.IntBuilder(1));
    KAddress seeder = SystemHelper.getAddress(1);

    Future<Msg, Msg> connAcc1 = connAcc();
    Future<Msg, Msg> connPing1 = connPing();
    Future<Msg, Msg> connPing2 = connPing();
    tc.body()
      .trigger(new ConnSeederEvents.Connect(torrent1, seeder), port1)
      .inspect(state(seeder.getId(), ConnSeeder.States.CONNECTING))
      .answerRequest(Msg.class, port2, connAcc1)
      .trigger(connAcc1, port2)
      .inspect(state(seeder.getId(), ConnSeeder.States.CONNECTED))
      .expect(ConnSeederEvents.ConnectSuccess.class, connSucc(torrent1), port1, Direction.OUT)
      //ping timeout
      .trigger(new R2MngrWrapperComp.TriggerTimeout(), port3)
      .answerRequest(Msg.class, port2, connPing1)
      .trigger(connPing1, port2)
      //missed one ping timeout
      .trigger(new R2MngrWrapperComp.TriggerTimeout(), port3)
      .answerRequest(Msg.class, port2, connPing())
      .trigger(new R2MngrWrapperComp.TriggerTimeout(), port3)
      .answerRequest(Msg.class, port2, connPing2)
      .trigger(connPing2, port2)
      //missed five ping timeouts - on the sixth - conn dead
      .trigger(new R2MngrWrapperComp.TriggerTimeout(), port3)
      .answerRequest(Msg.class, port2, connPing())
      .trigger(new R2MngrWrapperComp.TriggerTimeout(), port3)
      .answerRequest(Msg.class, port2, connPing())
      .trigger(new R2MngrWrapperComp.TriggerTimeout(), port3)
      .answerRequest(Msg.class, port2, connPing())
      .trigger(new R2MngrWrapperComp.TriggerTimeout(), port3)
      .answerRequest(Msg.class, port2, connPing())
      .trigger(new R2MngrWrapperComp.TriggerTimeout(), port3)
      .answerRequest(Msg.class, port2, connPing())
      .trigger(new R2MngrWrapperComp.TriggerTimeout(), port3)
      .expect(ConnSeederEvents.ConnectFail.class, connFail(torrent1), port1, Direction.OUT)
      .repeat(1).body().end();

    assertTrue(tc.check());
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
    return (R2MngrWrapperComp t) -> {
      return !t.activeSeederFSM(seederId);
    };
  }

  Predicate<ConnSeederEvents.ConnectSuccess> connSucc(OverlayId torrentId) {
    return (ConnSeederEvents.ConnectSuccess t) -> {
      return t.torrentId.equals(torrentId);
    };
  }

  Predicate<ConnSeederEvents.ConnectFail> connFail(OverlayId torrentId) {
    return (ConnSeederEvents.ConnectFail t) -> {
      return t.torrentId.equals(torrentId);
    };
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
    return new BestEffortFuture<ConnSeederMsgs.Connect>(ConnSeederMsgs.Connect.class) {
      @Override
      public BasicContentMsg get() {
        return msg.answer(content.accept());
      }
    };
  }

  Future<Msg, Msg> discAck() {
    return new BestEffortFuture<ConnSeederMsgs.Disconnect>(ConnSeederMsgs.Disconnect.class) {
      @Override
      public BasicContentMsg get() {
        return msg.answer(content.ack());
      }
    };
  }

  Future<Msg, Msg> connTout() {
    return new BestEffortFuture<ConnSeederMsgs.Connect>(ConnSeederMsgs.Connect.class) {
      @Override
      public BasicContentMsg get() {
        return msg.answer(wrap.timeout());
      }
    };
  }

  Future<Msg, Msg> connRej() {
    return new BestEffortFuture<ConnSeederMsgs.Connect>(ConnSeederMsgs.Connect.class) {
      @Override
      public BasicContentMsg get() {
        return msg.answer(content.reject());
      }
    };
  }

  Future<Msg, Msg> connPing() {
    return new BestEffortFuture<ConnSeederMsgs.Ping>(ConnSeederMsgs.Ping.class) {
      @Override
      public BasicContentMsg get() {
        return msg.answer(content.ack());
      }
    };
  }
}
