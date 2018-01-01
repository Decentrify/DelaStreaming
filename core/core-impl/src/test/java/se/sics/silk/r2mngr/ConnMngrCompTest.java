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

  private TestContext<R2MngrComp> tc;
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

  private static TestContext<R2MngrComp> getContext() {
    KAddress selfAdr = SystemHelper.getAddress(0);
    R2MngrComp.Init init = new R2MngrComp.Init(selfAdr);
    TestContext<R2MngrComp> context = TestContext.newInstance(R2MngrComp.class, init);
    return context;
  }

  @After
  public void clean() {
  }

//  @Test
//  public void testTimeline1() {
//    Port<ConnPort> port1 = connMngrComp.getPositive(ConnPort.class);
//    Port<Network> port2 = connMngrComp.getNegative(Network.class);
//    OverlayId torrent1 = torrentIdFactory.id(new BasicBuilders.IntBuilder(1));
//    OverlayId torrent2 = torrentIdFactory.id(new BasicBuilders.IntBuilder(2));
//    OverlayId torrent3 = torrentIdFactory.id(new BasicBuilders.IntBuilder(3));
//    OverlayId torrent4 = torrentIdFactory.id(new BasicBuilders.IntBuilder(4));
//    KAddress seeder = SystemHelper.getAddress(1);
//
//    Future<Msg, Msg> connReq1 = connReq();
//    Future<Msg, Msg> discReq1 = discReq();
//    Future<Msg, Msg> connReq2 = connReq();
//    tc.body()
//      .trigger(new ConnSeederEvents.Connect(torrent1, seeder), port1)
//      .inspect(state(seeder.getId(), ConnSeeder.States.CONNECTING))
//      .answerRequest(Msg.class, port2, connReq1)
//      .trigger(new ConnSeederEvents.Connect(torrent2, seeder), port1)
//      .trigger(new ConnSeederEvents.Disconnect(torrent2, seeder.getId()), port1)
//      .trigger(new ConnSeederEvents.Connect(torrent3, seeder), port1)
//      .inspect(connected(seeder.getId(), 0))
//      .trigger(connReq1, port2)
//      .inspect(state(seeder.getId(), ConnSeeder.States.CONNECTED))
//      .unordered()
//      .expect(ConnSeederEvents.ConnectSuccess.class, connSucc(torrent1), port1, Direction.OUT)
//      .expect(ConnSeederEvents.ConnectSuccess.class, connSucc(torrent3), port1, Direction.OUT)
//      .end()
//      .inspect(connected(seeder.getId(), 2))
//      .trigger(new ConnSeederEvents.Connect(torrent4, seeder), port1)
//      .expect(ConnSeederEvents.ConnectSuccess.class, connSucc(torrent4), port1, Direction.OUT)
//      .trigger(new ConnSeederEvents.Disconnect(torrent4, seeder.getId()), port1)
//      .inspect(connected(seeder.getId(), 2))
//      .trigger(new ConnSeederEvents.Disconnect(torrent1, seeder.getId()), port1)
//      .trigger(new ConnSeederEvents.Disconnect(torrent3, seeder.getId()), port1)
//      .inspect(connected(seeder.getId(), 0))
//      .inspect(state(seeder.getId(), ConnSeeder.States.DISCONNECTING))
//      .answerRequest(Msg.class, port2, discReq1)
//      .trigger(new ConnSeederEvents.Connect(torrent1, seeder), port1)
//      .expect(ConnSeederEvents.ConnectFail.class, connFail(torrent1), port1, Direction.OUT)
//      .trigger(discReq1, port2)
//      //previous FSM is destroyed and we can create a new one
//      .trigger(new ConnSeederEvents.Connect(torrent1, seeder), port1)
//      .inspect(state(seeder.getId(), ConnSeeder.States.CONNECTING))
//      .answerRequest(Msg.class, port2, connReq2)
//      .trigger(connReq2, port2)
//      .inspect(state(seeder.getId(), ConnSeeder.States.CONNECTED))
//      .expect(ConnSeederEvents.ConnectSuccess.class, connSucc(torrent1), port1, Direction.OUT)
//      .repeat(1).body().end();
//
//    assertTrue(tc.check());
//  }

  /*
   * network timeout on connect
   */
  @Test
  public void testTimeline2() {
    Port<ConnPort> port1 = connMngrComp.getPositive(ConnPort.class);
    Port<Network> port2 = connMngrComp.getNegative(Network.class);
    OverlayId torrent1 = torrentIdFactory.id(new BasicBuilders.IntBuilder(1));
    KAddress seeder = SystemHelper.getAddress(1);

    Future<Msg, Msg> timeReq1 = timeReq();
    tc.body()
      .trigger(new ConnSeederEvents.Connect(torrent1, seeder), port1)
      .inspect(state(seeder.getId(), ConnSeeder.States.CONNECTING))
      .answerRequest(Msg.class, port2, timeReq1)
      .expect(ConnSeederEvents.ConnectFail.class, connFail(torrent1), port1, Direction.OUT)
//      .inspect(state(seeder.getId(), ConnSeeder.States.DISCONNECTING))
      .repeat(1).body().end();
    
    assertTrue(tc.check());
  }

  Predicate<R2MngrComp> state(Identifier seederId, FSMStateName expectedState) {
    return (R2MngrComp t) -> {
      FSMStateName currentState = t.getConnSeederState(seederId);
      return currentState.equals(expectedState);
    };
  }

  Predicate<R2MngrComp> connected(Identifier seederId, int nrTorrents) {
    return (R2MngrComp t) -> {
      ConnSeeder.IS is = (ConnSeeder.IS) t.getConnSeederIS(seederId);
      return is.connected.size() == nrTorrents;
    };
  }
  
  Predicate<ConnSeederEvents.ConnectSuccess> connSucc(OverlayId torrentId) {
    return (ConnSeederEvents.ConnectSuccess t) -> t.torrentId.equals(torrentId);
  }

  Predicate<ConnSeederEvents.ConnectFail> connFail(OverlayId torrentId) {
    return (ConnSeederEvents.ConnectFail t) -> t.torrentId.equals(torrentId);
  }

  Future<Msg, Msg> connReq() {
    return new Future<Msg, Msg>() {
      private BasicContentMsg msg;
      private ConnSeederMsgs.Connect content;

      @Override
      public boolean set(Msg r) {
        if (!(r instanceof BasicContentMsg)) {
          return false;
        }
        this.msg = (BasicContentMsg) r;
        if (!(msg.getContent() instanceof BestEffortMsg.Request)) {
          return false;
        }
        BestEffortMsg.Request aux = (BestEffortMsg.Request) msg.getContent();
        if (!(aux.extractValue()instanceof ConnSeederMsgs.Connect)) {
          return false;
        }
        this.msg = msg;
        this.content = (ConnSeederMsgs.Connect) aux.extractValue();
        return true;
      }

      @Override
      public BasicContentMsg get() {
        return msg.answer(content.accept());
      }
    };
  }

  Future<Msg, Msg> discReq() {
    return new Future<Msg, Msg>() {
      private BasicContentMsg msg;
      private ConnSeederMsgs.Disconnect content;

      @Override
      public boolean set(Msg r) {
        if (!(r instanceof BasicContentMsg)) {
          return false;
        }
        this.msg = (BasicContentMsg) r;
        if (!(msg.getContent() instanceof BestEffortMsg.Request)) {
          return false;
        }
        BestEffortMsg.Request aux = (BestEffortMsg.Request) msg.getContent();
        if (!(aux.extractValue()instanceof ConnSeederMsgs.Disconnect)) {
          return false;
        }
        this.msg = msg;
        this.content = (ConnSeederMsgs.Disconnect) aux.extractValue();
        return true;
      }

      @Override
      public BasicContentMsg get() {
        return msg.answer(content.ack());
      }
    };
  }
  
  Future<Msg, Msg> timeReq() {
    return new Future<Msg, Msg>() {
      private BasicContentMsg msg;
      private BestEffortMsg.Request wrap;
      private ConnSeederMsgs.Connect content;

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
        if (!(wrap.extractValue()instanceof ConnSeederMsgs.Disconnect)) {
          return false;
        }
        this.msg = msg;
        this.content = (ConnSeederMsgs.Connect) wrap.extractValue();
        return true;
      }

      @Override
      public BasicContentMsg get() {
        return msg.answer(wrap.timeout());
      }
    };
  }
}
