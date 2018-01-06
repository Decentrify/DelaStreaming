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
import se.sics.kompics.fsm.event.FSMWrongState;
import se.sics.kompics.network.Msg;
import se.sics.kompics.network.Network;
import se.sics.kompics.testing.Direction;
import se.sics.kompics.testing.TestContext;
import se.sics.kompics.util.Identifier;
import se.sics.ktoolbox.util.identifiable.BasicBuilders;
import se.sics.ktoolbox.util.identifiable.overlay.OverlayId;
import se.sics.ktoolbox.util.identifiable.overlay.OverlayIdFactory;
import se.sics.ktoolbox.util.network.KAddress;
import se.sics.ktoolbox.util.network.basic.BasicContentMsg;
import static se.sics.silk.MsgHelper.incNetP;
import static se.sics.silk.MsgHelper.msg;
import se.sics.silk.SystemHelper;
import se.sics.silk.SystemSetup;
import se.sics.silk.r2mngr.event.R2ConnLeecherEvents;
import se.sics.silk.r2mngr.msg.R2ConnMsgs;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class R2ConnLeecherTest {

  private TestContext<R2MngrWrapperComp> tc;
  private Component r2MngrComp;
  private Port<R2ConnLeecherPort> connP;
  private Port<Network> networkP;
  private Port<R2MngrWrapperComp.Port> auxP;
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
    connP = r2MngrComp.getPositive(R2ConnLeecherPort.class);
    networkP = r2MngrComp.getNegative(Network.class);
    auxP = r2MngrComp.getPositive(R2MngrWrapperComp.Port.class);
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
  public void testBadStateAtStart() {
    OverlayId torrent1 = torrentIdFactory.id(new BasicBuilders.IntBuilder(1));
    KAddress leecher = SystemHelper.getAddress(1);

    tc = tc.body();
    tc = connWrongEventAtStart(tc, localConnReq(leecher, torrent1));
    tc = connWrongEventAtStart(tc, localDisc(leecher, torrent1));
    tc = netWrongMsgAtStart(tc, msg(leecher, selfAdr, netDisc()));
    tc = netWrongMsgAtStart(tc, msg(leecher, selfAdr, netPing()));
    tc = prepareAuxTimer(tc, leecher, torrent1);
    tc = timerWrongAtStart(tc, leecher.getId());
    tc.repeat(1).body().end();

    assertTrue(tc.check());
  }

  @Test
  public void testTimeline1() {
    OverlayId torrent1 = torrentIdFactory.id(new BasicBuilders.IntBuilder(1));
    OverlayId torrent2 = torrentIdFactory.id(new BasicBuilders.IntBuilder(2));
    KAddress leecher = SystemHelper.getAddress(1);

    tc = tc.body();
    tc = netConnAcc(tc, leecher);
    tc = localConnAcc(tc, leecher, torrent1);
    tc = localConnAcc(tc, leecher, torrent2);
    tc = localDisc(tc, leecher, torrent2);
    tc = netPing(tc, leecher);
    tc = netDisc(tc, leecher, new OverlayId[]{torrent1});
    tc = netConnAcc(tc, leecher);
    tc.repeat(1).body().end();

    assertTrue(tc.check());
  }

  @Test
  public void testPing() {
    OverlayId torrent1 = torrentIdFactory.id(new BasicBuilders.IntBuilder(1));
    OverlayId torrent2 = torrentIdFactory.id(new BasicBuilders.IntBuilder(2));
    KAddress leecher = SystemHelper.getAddress(1);

    tc = tc.body();
    tc = netConnAcc(tc, leecher);
    tc = localConnAcc(tc, leecher, torrent1);
    tc = netPing(tc, leecher);
    tc = timerOk(tc, leecher.getId());
    tc = netPing(tc, leecher);
    tc = timerOk(tc, leecher.getId(), 3);
    tc = netPing(tc, leecher);
    tc = timerOk(tc, leecher.getId(), 5);
    tc = timerDisc(tc, leecher.getId(), new OverlayId[]{torrent1});
    tc.repeat(1).body().end();

    assertTrue(tc.check());
  }

  private TestContext<R2MngrWrapperComp> prepareAuxTimer(TestContext<R2MngrWrapperComp> tc,
    KAddress seeder, OverlayId torrentId) {
    tc = netConnAcc(tc, seeder); //required to setup the timer
    tc = netDisc(tc, seeder); //destroying the created fsm - the aux component maintains the timer to emulate a late timeout
    return tc;
  }

  private TestContext<R2MngrWrapperComp> timerWrongAtStart(TestContext<R2MngrWrapperComp> tc, Identifier leecherId) {
    return tc
      .inspect(inactiveFSM(leecherId))
      .trigger(timerAux(), auxP)
      .inspect(inactiveFSM(leecherId));
  }

  private TestContext<R2MngrWrapperComp> timerOk(TestContext<R2MngrWrapperComp> tc, Identifier leecherId, int nr) {
    return timerOk(tc.repeat(5).body(), leecherId).end();
  }
  
  private TestContext<R2MngrWrapperComp> timerOk(TestContext<R2MngrWrapperComp> tc, Identifier leecherId) {
    return tc
      .inspect(state(leecherId, R2ConnLeecher.States.CONNECTED))
      .trigger(timerAux(), auxP)
      .inspect(state(leecherId, R2ConnLeecher.States.CONNECTED));
  }
  
  private TestContext<R2MngrWrapperComp> timerDisc(TestContext<R2MngrWrapperComp> tc, Identifier leecherId, 
    OverlayId[] torrents) {
    tc = tc
      .inspect(state(leecherId, R2ConnLeecher.States.CONNECTED))
      .trigger(timerAux(), auxP);
    tc = localUnorderedDisc(tc, leecherId, torrents);
    tc = tc.inspect(inactiveFSM(leecherId));
    return tc;
  }

  private TestContext<R2MngrWrapperComp> netConnAcc(TestContext<R2MngrWrapperComp> tc, KAddress leecher) {
    return tc
      .inspect(inactiveFSM(leecher.getId()))
      .trigger(msg(leecher, selfAdr, netConnReq()), networkP)
      .expect(Msg.class, incNetP(leecher, R2ConnMsgs.ConnectAcc.class), networkP, Direction.OUT)
      .inspect(state(leecher.getId(), R2ConnLeecher.States.CONNECTED));
  }

  private TestContext<R2MngrWrapperComp> netPing(TestContext<R2MngrWrapperComp> tc, KAddress leecher) {
    return tc
      .inspect(state(leecher.getId(), R2ConnLeecher.States.CONNECTED))
      .trigger(msg(leecher, selfAdr, netPing()), networkP)
      .expect(Msg.class, incNetP(leecher, R2ConnMsgs.Pong.class), networkP, Direction.OUT)
      .inspect(state(leecher.getId(), R2ConnLeecher.States.CONNECTED));
  }

  private TestContext<R2MngrWrapperComp> netDisc(TestContext<R2MngrWrapperComp> tc, KAddress leecher) {
    return tc
      .inspect(state(leecher.getId(), R2ConnLeecher.States.CONNECTED))
      .trigger(msg(leecher, selfAdr, netDisc()), networkP)
      .expect(Msg.class, incNetP(leecher, R2ConnMsgs.DisconnectAck.class), networkP, Direction.OUT)
      .inspect(inactiveFSM(leecher.getId()));
  }

  private TestContext<R2MngrWrapperComp> netDisc(TestContext<R2MngrWrapperComp> tc, KAddress leecher,
    OverlayId[] torrentIds) {
    if (torrentIds.length == 0) {
      return netDisc(tc, leecher);
    }
    tc = tc
      .inspect(state(leecher.getId(), R2ConnLeecher.States.CONNECTED))
      .trigger(msg(leecher, selfAdr, netDisc()), networkP)
      .expect(Msg.class, incNetP(leecher, R2ConnMsgs.DisconnectAck.class), networkP, Direction.OUT);
    tc = localUnorderedDisc(tc, leecher.getId(), torrentIds);
    return tc.inspect(inactiveFSM(leecher.getId()));
  }

  private TestContext<R2MngrWrapperComp> localUnorderedDisc(TestContext<R2MngrWrapperComp> tc, Identifier leecherId,
    OverlayId[] torrentIds) {
    tc = tc.unordered();
    for (OverlayId torrentId : torrentIds) {
      tc.expect(R2ConnLeecherEvents.Disconnect.class, localDiscP(torrentId, leecherId), connP, Direction.OUT);
    }
    return tc.end();
  }

  private TestContext<R2MngrWrapperComp> localConnAcc(TestContext<R2MngrWrapperComp> tc,
    KAddress leecher, OverlayId torrentId) {

    return tc
      .inspect(state(leecher.getId(), R2ConnLeecher.States.CONNECTED))
      .trigger(localConnReq(leecher, torrentId), connP)
      .expect(R2ConnLeecherEvents.ConnectAcc.class, localConnAccP(torrentId, leecher.getId()), connP, Direction.OUT)
      .inspect(state(leecher.getId(), R2ConnLeecher.States.CONNECTED));
  }

  private TestContext<R2MngrWrapperComp> localDisc(TestContext<R2MngrWrapperComp> tc,
    KAddress leecher, OverlayId torrentId) {

    return tc
      .inspect(state(leecher.getId(), R2ConnLeecher.States.CONNECTED))
      .trigger(localDisc(leecher, torrentId), connP)
      .inspect(state(leecher.getId(), R2ConnLeecher.States.CONNECTED));
  }

  Predicate<R2ConnLeecherEvents.ConnectAcc> localConnAccP(OverlayId torrentId, Identifier leecherId) {
    return (R2ConnLeecherEvents.ConnectAcc m) -> {
      return m.torrentId.equals(torrentId) && m.leecherId.equals(leecherId);
    };
  }

  Predicate<R2ConnLeecherEvents.Disconnect> localDiscP(OverlayId torrentId, Identifier leecherId) {
    return (R2ConnLeecherEvents.Disconnect m) -> {
      return m.torrentId.equals(torrentId) && m.leecherId.equals(leecherId);
    };
  }

  private R2ConnMsgs.ConnectReq netConnReq() {
    return new R2ConnMsgs.ConnectReq();
  }

  private R2ConnMsgs.Disconnect netDisc() {
    return new R2ConnMsgs.Disconnect();
  }

  private R2ConnMsgs.Ping netPing() {
    return new R2ConnMsgs.Ping();
  }

  private R2ConnLeecherEvents.ConnectReq localConnReq(KAddress leecher, OverlayId torrent) {
    return new R2ConnLeecherEvents.ConnectReq(torrent, leecher.getId());
  }

  private R2ConnLeecherEvents.Disconnect localDisc(KAddress leecher, OverlayId torrent) {
    return new R2ConnLeecherEvents.Disconnect(torrent, leecher.getId());
  }

  private R2MngrWrapperComp.TriggerTimeout timerAux() {
    return new R2MngrWrapperComp.TriggerTimeout();
  }

  private TestContext<R2MngrWrapperComp> netWrongMsgAtStart(TestContext<R2MngrWrapperComp> tc, BasicContentMsg msg) {
    return tc
      .inspect(inactiveFSM(msg.getSource().getId()))
      .trigger(msg, networkP)
      .inspect(inactiveFSM(msg.getSource().getId()));
  }

  private TestContext<R2MngrWrapperComp> connWrongEventAtStart(TestContext<R2MngrWrapperComp> tc,
    R2ConnLeecher.Event event) {
    return tc
      .inspect(inactiveFSM(event.getConnLeecherFSMId()))
      .trigger(event, connP)
      .expect(FSMWrongState.class, connP, Direction.OUT)
      .inspect(inactiveFSM(event.getConnLeecherFSMId()));
  }

  Predicate<R2MngrWrapperComp> inactiveFSM(Identifier leecherId) {
    return (R2MngrWrapperComp t) -> {
      return !t.activeLeecherFSM(leecherId);
    };
  }

  Predicate<R2MngrWrapperComp> state(Identifier seederId, FSMStateName expectedState) {
    return (R2MngrWrapperComp t) -> {
      FSMStateName currentState = t.getConnLeecherState(seederId);
      return currentState.equals(expectedState);
    };
  }
}
