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
import se.sics.kompics.network.Transport;
import se.sics.kompics.testing.Direction;
import se.sics.kompics.testing.TestContext;
import se.sics.kompics.util.Identifier;
import se.sics.ktoolbox.util.identifiable.BasicBuilders;
import se.sics.ktoolbox.util.identifiable.overlay.OverlayId;
import se.sics.ktoolbox.util.identifiable.overlay.OverlayIdFactory;
import se.sics.ktoolbox.util.network.KAddress;
import se.sics.ktoolbox.util.network.basic.BasicContentMsg;
import se.sics.ktoolbox.util.network.basic.BasicHeader;
import static se.sics.silk.MsgHelper.incNetP;
import se.sics.silk.SystemHelper;
import se.sics.silk.SystemSetup;
import se.sics.silk.r2mngr.event.ConnLeecherEvents;
import se.sics.silk.r2mngr.msg.ConnMsgs;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class ConnLeecherTest {

  private TestContext<R2MngrWrapperComp> tc;
  private Component r2MngrComp;
  private Port<ConnLeecherPort> connP;
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
    connP = r2MngrComp.getPositive(ConnLeecherPort.class);
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
  public void testTimeline1() {
    OverlayId torrent1 = torrentIdFactory.id(new BasicBuilders.IntBuilder(1));
    OverlayId torrent2 = torrentIdFactory.id(new BasicBuilders.IntBuilder(2));
    KAddress leecher = SystemHelper.getAddress(1);

    tc = tc.body();
    tc = netConnAcc(tc, leecher);
    tc = localConnAcc(tc, torrent1, leecher);
    tc = localConnAcc(tc, torrent2, leecher);
    tc = localDisc(tc, torrent2, leecher);
    tc = netPing(tc, leecher);
    tc = netDisc(tc, leecher, new OverlayId[]{torrent1});
    tc = netConnAcc(tc, leecher);
    tc.repeat(1).body().end();

    assertTrue(tc.check());
  }

  @Test
  public void testBadState1() {
    OverlayId torrent1 = torrentIdFactory.id(new BasicBuilders.IntBuilder(1));
    KAddress leecher = SystemHelper.getAddress(1);

    tc = tc.body();
    tc = connWrongEventAtStart(tc, localConnReq(torrent1, leecher));
    tc = connWrongEventAtStart(tc, localDisc(torrent1, leecher));
    tc = netWrongMsgAtStart(tc, msg(leecher, selfAdr, netDisc(leecher)));
    tc = netWrongMsgAtStart(tc, msg(leecher, selfAdr, netPing(leecher)));
    tc.repeat(1).body().end();

    assertTrue(tc.check());
  }

  private TestContext<R2MngrWrapperComp> netConnAcc(TestContext<R2MngrWrapperComp> tc, KAddress leecher) {
    return tc
      .inspect(inactiveLeecherFSM(leecher.getId()))
      .trigger(msg(leecher, selfAdr, netConnReq(leecher)), networkP)
      .expect(Msg.class, incNetP(leecher, ConnMsgs.ConnectAcc.class), networkP, Direction.OUT)
      .inspect(state(leecher.getId(), ConnLeecher.States.CONNECTED));
  }

  private TestContext<R2MngrWrapperComp> netPing(TestContext<R2MngrWrapperComp> tc, KAddress leecher) {
    return tc
      .inspect(state(leecher.getId(), ConnLeecher.States.CONNECTED))
      .trigger(msg(leecher, selfAdr, netPing(leecher)), networkP)
      .expect(Msg.class, incNetP(leecher, ConnMsgs.Pong.class), networkP, Direction.OUT)
      .inspect(state(leecher.getId(), ConnLeecher.States.CONNECTED));
  }

  private TestContext<R2MngrWrapperComp> netDisc(TestContext<R2MngrWrapperComp> tc, KAddress leecher) {
    return tc
      .inspect(state(leecher.getId(), ConnLeecher.States.CONNECTED))
      .trigger(msg(leecher, selfAdr, netDisc(leecher)), networkP)
      .expect(Msg.class, incNetP(leecher, ConnMsgs.DisconnectAck.class), networkP, Direction.OUT)
      .inspect(inactiveLeecherFSM(leecher.getId()));
  }

  private TestContext<R2MngrWrapperComp> netDisc(TestContext<R2MngrWrapperComp> tc, KAddress leecher,
    OverlayId[] torrentIds) {
    if (torrentIds.length == 0) {
      return netDisc(tc, leecher);
    }
    tc = tc
      .inspect(state(leecher.getId(), ConnLeecher.States.CONNECTED))
      .trigger(msg(leecher, selfAdr, netDisc(leecher)), networkP)
      .expect(Msg.class, incNetP(leecher, ConnMsgs.DisconnectAck.class), networkP, Direction.OUT);
    tc = unorderedDisc(tc, leecher, torrentIds);
    return tc.inspect(inactiveLeecherFSM(leecher.getId()));
  }

  private TestContext<R2MngrWrapperComp> unorderedDisc(TestContext<R2MngrWrapperComp> tc, KAddress leecher,
    OverlayId[] torrentIds) {
    tc = tc.unordered();
    for (OverlayId torrentId : torrentIds) {
      tc.expect(ConnLeecherEvents.Disconnect.class, localDiscP(torrentId, leecher.getId()), connP, Direction.OUT);
    }
    return tc.end();
  }

  private TestContext<R2MngrWrapperComp> localConnAcc(TestContext<R2MngrWrapperComp> tc,
    OverlayId torrentId, KAddress leecher) {

    return tc
      .inspect(state(leecher.getId(), ConnLeecher.States.CONNECTED))
      .trigger(localConnReq(torrentId, leecher), connP)
      .expect(ConnLeecherEvents.ConnectAcc.class, localConnAccP(torrentId, leecher.getId()), connP, Direction.OUT)
      .inspect(state(leecher.getId(), ConnLeecher.States.CONNECTED));
  }

  private TestContext<R2MngrWrapperComp> localDisc(TestContext<R2MngrWrapperComp> tc,
    OverlayId torrentId, KAddress leecher) {

    return tc
      .inspect(state(leecher.getId(), ConnLeecher.States.CONNECTED))
      .trigger(localDisc(torrentId, leecher), connP)
      .inspect(state(leecher.getId(), ConnLeecher.States.CONNECTED));
  }

  Predicate<ConnLeecherEvents.ConnectAcc> localConnAccP(OverlayId torrentId, Identifier leecherId) {
    return (ConnLeecherEvents.ConnectAcc m) -> {
      return m.torrentId.equals(torrentId) && m.leecherId.equals(leecherId);
    };
  }

  Predicate<ConnLeecherEvents.Disconnect> localDiscP(OverlayId torrentId, Identifier leecherId) {
    return (ConnLeecherEvents.Disconnect m) -> {
      return m.torrentId.equals(torrentId) && m.leecherId.equals(leecherId);
    };
  }

  private ConnMsgs.ConnectReq netConnReq(KAddress leecher) {
    return new ConnMsgs.ConnectReq(leecher.getId(), selfAdr.getId());
  }

  private ConnMsgs.Disconnect netDisc(KAddress leecher) {
    return new ConnMsgs.Disconnect(leecher.getId(), selfAdr.getId());
  }

  private ConnMsgs.Ping netPing(KAddress leecher) {
    return new ConnMsgs.Ping(leecher.getId(), selfAdr.getId());
  }

  private ConnLeecherEvents.ConnectReq localConnReq(OverlayId torrent, KAddress leecher) {
    return new ConnLeecherEvents.ConnectReq(torrent, leecher.getId());
  }

  private ConnLeecherEvents.Disconnect localDisc(OverlayId torrent, KAddress leecher) {
    return new ConnLeecherEvents.Disconnect(torrent, leecher.getId());
  }

  private TestContext<R2MngrWrapperComp> netWrongMsgAtStart(TestContext<R2MngrWrapperComp> tc, BasicContentMsg msg) {
    return tc
      .inspect(inactiveLeecherFSM(msg.getSource().getId()))
      .trigger(msg, networkP)
      .inspect(inactiveLeecherFSM(msg.getSource().getId()));
  }

  private TestContext<R2MngrWrapperComp> connWrongEventAtStart(TestContext<R2MngrWrapperComp> tc,
    ConnLeecher.Event event) {
    return tc
      .inspect(inactiveLeecherFSM(event.getConnLeecherFSMId()))
      .trigger(event, connP)
      .expect(FSMWrongState.class, connP, Direction.OUT)
      .inspect(inactiveLeecherFSM(event.getConnLeecherFSMId()));
  }

  Predicate<R2MngrWrapperComp> inactiveLeecherFSM(Identifier leecherId) {
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

  private <C extends Object> BasicContentMsg msg(KAddress src, KAddress dst, C content) {
    BasicHeader header = new BasicHeader(src, dst, Transport.UDP);
    BasicContentMsg msg = new BasicContentMsg(header, content);
    return msg;
  }
}
