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
package se.sics.silk.r2torrent.torrent;

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
import se.sics.kompics.testing.TestContext;
import se.sics.kompics.util.Identifier;
import se.sics.ktoolbox.util.identifiable.BasicBuilders;
import se.sics.ktoolbox.util.identifiable.basic.IntIdFactory;
import se.sics.ktoolbox.util.identifiable.overlay.OverlayId;
import se.sics.ktoolbox.util.identifiable.overlay.OverlayIdFactory;
import se.sics.ktoolbox.util.network.KAddress;
import se.sics.nstream.StreamId;
import se.sics.nstream.storage.durable.DStreamControlPort;
import se.sics.nstream.storage.durable.util.MyStream;
import se.sics.silk.SystemHelper;
import se.sics.silk.SystemSetup;
import se.sics.silk.TorrentIdHelper;
import static se.sics.silk.TorrentTestHelper.eStreamCloseAck;
import static se.sics.silk.TorrentTestHelper.eStreamCloseReq;
import static se.sics.silk.TorrentTestHelper.eStreamOpenReq;
import static se.sics.silk.TorrentTestHelper.eStreamOpenSucc;
import static se.sics.silk.TorrentTestHelper.streamCloseSucc;
import static se.sics.silk.TorrentTestHelper.streamOpenSucc;
import static se.sics.silk.TorrentTestHelper.tStreamClose;
import static se.sics.silk.TorrentTestHelper.tStreamOpen;
import se.sics.silk.TorrentWrapperComp;
import se.sics.silk.r2torrent.R2TorrentComp;
import se.sics.silk.r2torrent.R2TorrentPort;
import se.sics.silk.r2torrent.torrent.R2StreamMngr.States;
import se.sics.silk.r2torrent.torrent.event.R2StreamMngrEvents;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class R2StreamMngrTest {
  private TestContext<TorrentWrapperComp> tc;
  private Component comp;
  private TorrentWrapperComp compState;
  private Port<DStreamControlPort> streamCtrlP;
  private Port<R2TorrentPort> triggerP;
  private Port<R2TorrentPort> expectP;
  private static OverlayIdFactory torrentIdFactory;
  private IntIdFactory intIdFactory;
  private KAddress selfAdr;
  private Identifier endpointId;
  OverlayId torrent;
  Identifier file;
  StreamId streamId;

  @BeforeClass
  public static void setup() throws FSMException {
    torrentIdFactory = SystemSetup.systemSetup("src/test/resources/application.conf");
  }

  @Before
  public void testSetup() {
    tc = getContext();
    comp = tc.getComponentUnderTest();
    compState = (TorrentWrapperComp) comp.getComponent();
    streamCtrlP = comp.getNegative(DStreamControlPort.class);
    triggerP = comp.getNegative(R2TorrentPort.class);
    expectP = comp.getPositive(R2TorrentPort.class);
    intIdFactory = new IntIdFactory(new Random());
    endpointId = intIdFactory.id(new BasicBuilders.IntBuilder(0));
    torrent = torrentIdFactory.id(new BasicBuilders.IntBuilder(1));
    file = intIdFactory.id(new BasicBuilders.IntBuilder(1));
    streamId = TorrentIdHelper.streamId(endpointId, torrent, file);
  }

  private TestContext<TorrentWrapperComp> getContext() {
    selfAdr = SystemHelper.getAddress(0);
    TorrentWrapperComp.Setup setup = new TorrentWrapperComp.Setup() {
      @Override
      public MultiFSM setupFSM(ComponentProxy proxy, Config config, R2TorrentComp.Ports ports) {
        try {
          R2StreamMngr.ES fsmEs = new R2StreamMngr.ES();
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
          return R2StreamMngr.FSM.multifsm(fsmIdFactory, fsmEs, oexa);
        } catch (FSMException ex) {
          throw new RuntimeException(ex);
        }
      }
    };
    TorrentWrapperComp.Init init = new TorrentWrapperComp.Init(selfAdr, setup);
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

  //*****************************************************START TO OPEN**************************************************
  @Test
  public void testStartToOpen() {
    tc = tc.body();
    tc = startToOpen(tc, torrent, file, streamId);
    tc.repeat(1).body().end();
    assertTrue(tc.check());
    Identifier fsmBaseId = R2StreamMngr.fsmBaseId(torrent, file);
    assertEquals(States.OPEN, compState.fsm.getFSMState(fsmBaseId));
  }
  
  private TestContext startToOpen(TestContext tc, OverlayId torrent, Identifier file, StreamId streamId) {
    tc = tStreamOpen(tc, triggerP, open(torrent, file, streamId, null));
    tc = eStreamOpenReq(tc, streamCtrlP);
    return tc;
  }
  //*****************************************************START TO ACTIVE************************************************
  @Test
  public void testStartToActive() {
    tc = tc.body();
    tc = startToActive(tc, torrent, file, streamId);
    tc.repeat(1).body().end();
    assertTrue(tc.check());
    Identifier fsmBaseId = R2StreamMngr.fsmBaseId(torrent, file);
    assertEquals(States.ACTIVE, compState.fsm.getFSMState(fsmBaseId));
  }
  private TestContext startToActive(TestContext tc, OverlayId torrent, Identifier file, StreamId streamId) {
    tc = tStreamOpen(tc, triggerP, open(torrent, file, streamId, null));
    tc = streamOpenSucc(tc, streamCtrlP, 0);
    tc = eStreamOpenSucc(tc, expectP);
    return tc;
  }
  
  //*****************************************************START TO CLOSE*************************************************
  @Test
  public void testStartToClose() {
    tc = tc.body();
    tc = startToClose(tc, torrent, file, streamId);
    tc.repeat(1).body().end();
    assertTrue(tc.check());
    Identifier fsmBaseId = R2StreamMngr.fsmBaseId(torrent, file);
    assertEquals(States.CLOSE, compState.fsm.getFSMState(fsmBaseId));
  }
  private TestContext startToClose(TestContext tc, OverlayId torrent, Identifier file, StreamId streamId) {
    tc = tStreamOpen(tc, triggerP, open(torrent, file, streamId, null));
    tc = streamOpenSucc(tc, streamCtrlP, 0);
    tc = eStreamOpenSucc(tc, expectP);
    tc = tStreamClose(tc, triggerP, close(torrent, file, streamId));
    tc = eStreamCloseReq(tc, streamCtrlP);
    return tc;
  }
  //******************************************************OPEN TO STOP**************************************************
  @Test
  public void testOpenToStop() {
    tc = tc.body();
    tc = startToOpen(tc, torrent, file, streamId);
    tc = stop(tc, torrent, file, streamId);
    tc.repeat(1).body().end();
    assertTrue(tc.check());
    Identifier fsmBaseId = R2StreamMngr.fsmBaseId(torrent, file);
    assertFalse(compState.fsm.activeFSM(fsmBaseId));
  }
  
  private TestContext stop(TestContext tc, OverlayId torrent, Identifier file, StreamId streamId) {
    tc = tStreamClose(tc, triggerP, close(torrent, file, streamId));
    tc = streamCloseSucc(tc, streamCtrlP);
    tc = eStreamCloseAck(tc, expectP);
    return tc;
  }
  //*****************************************************ACTIVE TO STOP*************************************************
  @Test
  public void testActiveToStop() {
    tc = tc.body();
    tc = startToActive(tc, torrent, file, streamId);
    tc = stop(tc, torrent, file, streamId);
    tc.repeat(1).body().end();
    assertTrue(tc.check());
    Identifier fsmBaseId = R2StreamMngr.fsmBaseId(torrent, file);
    assertFalse(compState.fsm.activeFSM(fsmBaseId));
  }
  
  public R2StreamMngrEvents.Open open(OverlayId torrentId, Identifier fileId, StreamId streamId, MyStream stream) {
    return new R2StreamMngrEvents.Open(torrentId, fileId, streamId, stream);
  }
  
  public R2StreamMngrEvents.Close close(OverlayId torrentId, Identifier fileId, StreamId streamId) {
    return new R2StreamMngrEvents.Close(torrentId, fileId, streamId);
  }
}
