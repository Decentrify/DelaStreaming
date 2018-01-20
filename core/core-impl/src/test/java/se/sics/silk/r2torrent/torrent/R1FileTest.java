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
import se.sics.silk.SelfPort;
import se.sics.silk.SystemHelper;
import se.sics.silk.SystemSetup;
import se.sics.silk.TorrentIdHelper;
import static se.sics.silk.TorrentTestHelper.eFileIndication;
import static se.sics.silk.TorrentTestHelper.eStreamCloseReq;
import static se.sics.silk.TorrentTestHelper.eStreamOpenReq;
import static se.sics.silk.TorrentTestHelper.streamCloseSucc;
import static se.sics.silk.TorrentTestHelper.streamOpenSucc;
import static se.sics.silk.TorrentTestHelper.tFileClose;
import static se.sics.silk.TorrentTestHelper.tFileConnect;
import static se.sics.silk.TorrentTestHelper.tFileOpen;
import se.sics.silk.TorrentWrapperComp;
import se.sics.silk.r2torrent.R2TorrentComp;
import se.sics.silk.r2torrent.torrent.R1FileGet.States;
import se.sics.silk.r2torrent.torrent.event.R1FileGetEvents;
import se.sics.silk.r2torrent.torrent.state.FileStatus;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class R1FileTest {
  private TestContext<TorrentWrapperComp> tc;
  private Component comp;
  private TorrentWrapperComp compState;
  private Port<DStreamControlPort> streamCtrlP;
  private Port<SelfPort> triggerP;
  private Port<SelfPort> expectP;
  private static OverlayIdFactory torrentIdFactory;
  private IntIdFactory intIdFactory;
  private KAddress selfAdr;
  private Identifier endpointId;
  private OverlayId torrent;
  private Identifier file;
  private StreamId streamId;
  private KAddress seeder;

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
    triggerP = comp.getNegative(SelfPort.class);
    expectP = comp.getPositive(SelfPort.class);
    intIdFactory = new IntIdFactory(new Random());
    endpointId = intIdFactory.id(new BasicBuilders.IntBuilder(0));
    torrent = torrentIdFactory.id(new BasicBuilders.IntBuilder(1));
    file = intIdFactory.id(new BasicBuilders.IntBuilder(1));
    streamId = TorrentIdHelper.streamId(endpointId, torrent, file);
    seeder = SystemHelper.getAddress(1);
  }

  private TestContext<TorrentWrapperComp> getContext() {
    selfAdr = SystemHelper.getAddress(0);
    TorrentWrapperComp.Setup setup = new TorrentWrapperComp.Setup() {
      @Override
      public MultiFSM setupFSM(ComponentProxy proxy, Config config, R2TorrentComp.Ports ports) {
        try {
          R1FileGet.ES fsmEs = new R1FileGet.ES(selfAdr);
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
          return R1FileGet.FSM.multifsm(fsmIdFactory, fsmEs, oexa);
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

  //****************************************************START TO STORAGE************************************************
  @Test
  public void testStartToOpen() {
    tc = tc.body();
    tc = startToOpen(tc, torrent, file, streamId);
    tc.repeat(1).body().end();
    assertTrue(tc.check());
    Identifier fsmBaseId = R1FileGet.fsmBaseId(torrent, file);
    assertEquals(States.STORAGE, compState.fsm.getFSMState(fsmBaseId));
  }
  
  private TestContext startToOpen(TestContext tc, OverlayId torrent, Identifier file, StreamId streamId) {
    tc = tFileOpen(tc, triggerP, open(torrent, file, streamId, null));
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
    Identifier fsmBaseId = R1FileGet.fsmBaseId(torrent, file);
    assertEquals(States.ACTIVE, compState.fsm.getFSMState(fsmBaseId));
  }
  private TestContext startToActive(TestContext tc, OverlayId torrent, Identifier file, StreamId streamId) {
    tc = tFileOpen(tc, triggerP, open(torrent, file, streamId, null));
    tc = streamOpenSucc(tc, streamCtrlP, 0);
    tc = eFileIndication(tc, expectP, FileStatus.ACTIVE);
    return tc;
  }
  
  //****************************************************START TO COMPLETE***********************************************
  @Test
  public void testStartToComplete() {
    tc = tc.body();
    tc = startToComplete(tc, torrent, file, streamId);
    tc.repeat(1).body().end();
    assertTrue(tc.check());
    Identifier fsmBaseId = R1FileGet.fsmBaseId(torrent, file);
    assertEquals(States.ACTIVE, compState.fsm.getFSMState(fsmBaseId));
  }
  private TestContext startToComplete(TestContext tc, OverlayId torrent, Identifier file, StreamId streamId) {
    tc = tFileOpen(tc, triggerP, open(torrent, file, streamId, null));
    tc = streamOpenSucc(tc, streamCtrlP, 0);
    tc = eFileIndication(tc, expectP, FileStatus.ACTIVE);
    tc = tFileConnect(tc, triggerP, connect(torrent, file, seeder));
    return tc;
  }
  
  //*****************************************************START TO CLOSE*************************************************
  @Test
  public void testStartToClose() {
    tc = tc.body();
    tc = startToClose(tc, torrent, file, streamId);
    tc.repeat(1).body().end();
    assertTrue(tc.check());
    Identifier fsmBaseId = R1FileGet.fsmBaseId(torrent, file);
    assertEquals(States.CLOSE, compState.fsm.getFSMState(fsmBaseId));
  }
  private TestContext startToClose(TestContext tc, OverlayId torrent, Identifier file, StreamId streamId) {
    tc = tFileOpen(tc, triggerP, open(torrent, file, streamId, null));
    tc = streamOpenSucc(tc, streamCtrlP, 0);
    tc = tFileClose(tc, triggerP, close(torrent, file));
    tc = eStreamCloseReq(tc, streamCtrlP);
    return tc;
  }
  //******************************************************OPEN TO STOP**************************************************
  @Test
  public void testOpenToStop() {
    tc = tc.body();
    tc = startToOpen(tc, torrent, file, streamId);
    tc = stop(tc, torrent, file);
    tc.repeat(1).body().end();
    assertTrue(tc.check());
    Identifier fsmBaseId = R1FileGet.fsmBaseId(torrent, file);
    assertFalse(compState.fsm.activeFSM(fsmBaseId));
  }
  
  private TestContext stop(TestContext tc, OverlayId torrent, Identifier file) {
    tc = tFileClose(tc, triggerP, close(torrent, file));
    tc = streamCloseSucc(tc, streamCtrlP);
    return tc;
  }
  //*****************************************************ACTIVE TO STOP*************************************************
  @Test
  public void testActiveToStop() {
    tc = tc.body();
    tc = startToActive(tc, torrent, file, streamId);
    tc = stop(tc, torrent, file);
    tc.repeat(1).body().end();
    assertTrue(tc.check());
    Identifier fsmBaseId = R1FileGet.fsmBaseId(torrent, file);
    assertFalse(compState.fsm.activeFSM(fsmBaseId));
  }
  
  public R1FileGetEvents.Start open(OverlayId torrentId, Identifier fileId, StreamId streamId, MyStream stream) {
    return new R1FileGetEvents.Start(torrentId, fileId, streamId, stream);
  }
  
  public R1FileGetEvents.Close close(OverlayId torrentId, Identifier fileId) {
    return new R1FileGetEvents.Close(torrentId, fileId);
  }
  
  public R1FileGetEvents.Connect connect(OverlayId torrentId, Identifier fileId, KAddress seeder) {
    return new R1FileGetEvents.Connect(torrentId, fileId, seeder);
  }
}
