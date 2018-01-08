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
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.kompics.Component;
import se.sics.kompics.Port;
import se.sics.kompics.fsm.FSMException;
import se.sics.kompics.fsm.FSMStateName;
import se.sics.kompics.testing.Direction;
import se.sics.kompics.testing.TestContext;
import se.sics.ktoolbox.util.identifiable.BasicBuilders;
import se.sics.ktoolbox.util.identifiable.overlay.OverlayId;
import se.sics.ktoolbox.util.identifiable.overlay.OverlayIdFactory;
import se.sics.ktoolbox.util.network.KAddress;
import se.sics.silk.SystemHelper;
import se.sics.silk.SystemSetup;
import se.sics.silk.mocktimer.MockTimerComp;
import se.sics.silk.r2torrent.R2Torrent.States;
import se.sics.silk.r2torrent.R2TorrentCtrlPort;
import se.sics.silk.r2torrent.R2TorrentProxy;
import se.sics.silk.r2torrent.event.R2TorrentCtrlEvents;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class UpldDwnlTest {
  private static final Logger LOG = LoggerFactory.getLogger(UpldDwnlTest.class);
  
  private TestContext<R2MngrWrapperComp> tc;
  private Component testComp;
  private R2MngrWrapperComp auxComp;
  private Port<MockTorrentCtrlPort> inTorrentP;
  private Port<R2TorrentCtrlPort> outTorrentP;
  private Port<MockTimerComp.Port> mockTimer;
  private OverlayIdFactory torrentIdFactory;
  private KAddress adr1;
  private KAddress adr2;

  @Before
  public void testSetup() throws FSMException {
    torrentIdFactory = SystemSetup.systemSetup("src/test/resources/application.conf");
    tc = getContext();
    testComp = tc.getComponentUnderTest();
    auxComp = (R2MngrWrapperComp)testComp.getComponent();
    inTorrentP = testComp.getPositive(MockTorrentCtrlPort.class);
    outTorrentP = testComp.getPositive(R2TorrentCtrlPort.class);
    mockTimer = testComp.getPositive(MockTimerComp.Port.class);
  }

  private TestContext<R2MngrWrapperComp> getContext() {
    adr1 = SystemHelper.getAddress(1);
    adr2 = SystemHelper.getAddress(2);
    R2MngrWrapperComp.Init init = new R2MngrWrapperComp.Init(adr1, adr2);
    TestContext<R2MngrWrapperComp> context = TestContext.newInstance(R2MngrWrapperComp.class, init);
    return context;
  }

  @After
  public void clean() {
  }

//  @Test
  public void testEmpty() {
    tc = tc.body();
    tc.repeat(1).body().end();
    assertTrue(tc.check());
  }
  
  @Test
  public void testTransfer() {
    OverlayId torrent1 = torrentIdFactory.id(new BasicBuilders.IntBuilder(1));
    tc = tc.body();
    tc = tc
      .inspect(inactiveFSM1(torrent1))  //1
      .inspect(inactiveFSM2(torrent1)); //2
    tc = uploadTorrent1(tc, torrent1); //3-6
    tc = downloadTorrent2(tc, torrent1); //7-12
    tc.repeat(1).body().end();
    assertTrue(tc.check());
  }
  
  private TestContext uploadTorrent1(TestContext tc, OverlayId torrentId) {
    return tc
      .trigger(uploadTorrent(adr1, torrentId), inTorrentP)
      .expect(R2TorrentCtrlEvents.TorrentBaseInfo.class, outTorrentP, Direction.OUT)
//      .inspect(state1(torrentId, States.HASH))
      .expect(R2TorrentCtrlEvents.TorrentBaseInfo.class, outTorrentP, Direction.OUT)
      .inspect(state1(torrentId, States.UPLOAD));
  }
  
  private TestContext downloadTorrent2(TestContext tc, OverlayId torrentId) {
    return tc
      .trigger(metadataGet(adr2, torrentId), inTorrentP)
      .expect(R2TorrentCtrlEvents.MetaGetSucc.class, outTorrentP, Direction.OUT)
      .inspect(state2(torrentId, States.DATA_STORAGE))
      .trigger(downloadTorrent(adr2, torrentId), inTorrentP)
      .expect(R2TorrentCtrlEvents.TorrentBaseInfo.class, outTorrentP, Direction.OUT)
      .inspect(state2(torrentId, States.TRANSFER));
  }
  
  public MockTorrentCtrlEvent uploadTorrent(KAddress adr, OverlayId torrentId) {
    return new MockTorrentCtrlEvent(adr, new R2TorrentCtrlEvents.Upload(torrentId));
  }
  
  public MockTorrentCtrlEvent metadataGet(KAddress adr, OverlayId torrentId) {
    return new MockTorrentCtrlEvent(adr, new R2TorrentCtrlEvents.MetaGetReq(torrentId));
  }
  
  public MockTorrentCtrlEvent downloadTorrent(KAddress adr, OverlayId torrentId) {
    return new MockTorrentCtrlEvent(adr, new R2TorrentCtrlEvents.Download(torrentId));
  }
  
  Predicate<R2MngrWrapperComp> inactiveFSM1(OverlayId torrentId) {
    return (R2MngrWrapperComp t) -> {
      return R2TorrentProxy.inactive(auxComp.mngrComp1.torrentMngr, torrentId.baseId);
    };
  }
  
  Predicate<R2MngrWrapperComp> inactiveFSM2(OverlayId torrentId) {
    return (R2MngrWrapperComp t) -> {
      return R2TorrentProxy.inactive(auxComp.mngrComp2.torrentMngr, torrentId.baseId);
    };
  }

  Predicate<R2MngrWrapperComp> state1(OverlayId torrentId, FSMStateName expectedState) {
    return (R2MngrWrapperComp t) -> {
      FSMStateName currentState = R2TorrentProxy.state(auxComp.mngrComp1.torrentMngr, torrentId.baseId);
      LOG.info("N1:{}", currentState);
      return currentState.equals(expectedState);
    };
  }
  
  Predicate<R2MngrWrapperComp> state2(OverlayId torrentId, FSMStateName expectedState) {
    return (R2MngrWrapperComp t) -> {
      FSMStateName currentState = R2TorrentProxy.state(auxComp.mngrComp2.torrentMngr, torrentId.baseId);
      return currentState.equals(expectedState);
    };
  }
}
