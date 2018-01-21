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

import com.google.common.base.Predicate;
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
import se.sics.kompics.testing.Direction;
import se.sics.kompics.testing.TestContext;
import se.sics.kompics.timer.Timer;
import se.sics.kompics.util.Identifier;
import se.sics.ktoolbox.util.identifiable.BasicBuilders;
import se.sics.ktoolbox.util.identifiable.basic.IntIdFactory;
import se.sics.ktoolbox.util.identifiable.overlay.OverlayId;
import se.sics.ktoolbox.util.identifiable.overlay.OverlayIdFactory;
import se.sics.ktoolbox.util.network.KAddress;
import se.sics.nstream.StreamId;
import se.sics.nstream.storage.durable.DStreamControlPort;
import se.sics.nstream.storage.durable.events.DStreamConnect;
import se.sics.nstream.storage.durable.util.MyStream;
import se.sics.nstream.util.BlockDetails;
import se.sics.silk.SelfPort;
import se.sics.silk.SystemHelper;
import se.sics.silk.SystemSetup;
import se.sics.silk.TorrentIdHelper;
import static se.sics.silk.TorrentTestHelper.eCancelPeriodicTimer;
import static se.sics.silk.TorrentTestHelper.eSchedulePeriodicTimer;
import static se.sics.silk.TorrentTestHelper.storageStreamConnected;
import static se.sics.silk.TorrentTestHelper.storageStreamDisconnected;
import static se.sics.silk.TorrentTestHelper.tFileOpen;
import static se.sics.silk.TorrentTestHelper.transferSeederConnectSucc;
import se.sics.silk.TorrentWrapperComp;
import se.sics.silk.r2torrent.R2TorrentComp;
import se.sics.silk.r2torrent.torrent.R1FileDownload.States;
import se.sics.silk.r2torrent.torrent.event.R1FileDownloadEvents;
import se.sics.silk.r2torrent.torrent.util.R1FileMngr;
import se.sics.silk.r2torrent.transfer.events.R1DownloadTimeout;
import se.sics.silk.r2torrent.transfer.events.R1TransferSeederEvents;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class R1FileDownloadTest {

  private TestContext<TorrentWrapperComp> tc;
  private Component comp;
  private TorrentWrapperComp compState;
  private Port<DStreamControlPort> streamCtrlP;
  private Port<SelfPort> triggerP;
  private Port<SelfPort> expectP;
  private Port<Timer> timerP;
  private static OverlayIdFactory torrentIdFactory;
  private IntIdFactory intIdFactory;
  private KAddress selfAdr;
  private Identifier endpointId;
  private OverlayId torrent;
  private Identifier file;
  private StreamId streamId;
  private KAddress seeder1;
  private KAddress seeder2;
  private KAddress seeder3;
  private BlockDetails defaultBlock;

  @BeforeClass
  public static void setup() throws FSMException {
    torrentIdFactory = SystemSetup.systemSetup("src/test/resources/application.conf");
  }

  @Before
  public void testSetup() {
    intIdFactory = new IntIdFactory(new Random());
    selfAdr = SystemHelper.getAddress(0);
    seeder1 = SystemHelper.getAddress(1);
    seeder2 = SystemHelper.getAddress(2);
    seeder3 = SystemHelper.getAddress(3);
    endpointId = intIdFactory.id(new BasicBuilders.IntBuilder(0));
    torrent = torrentIdFactory.id(new BasicBuilders.IntBuilder(1));
    file = intIdFactory.id(new BasicBuilders.IntBuilder(1));
    streamId = TorrentIdHelper.streamId(endpointId, torrent, file);
    defaultBlock = new BlockDetails(1024 * 100 + 10, 100, 1024, 10);

    tc = getContext();
    comp = tc.getComponentUnderTest();
    compState = (TorrentWrapperComp) comp.getComponent();
    streamCtrlP = comp.getNegative(DStreamControlPort.class);
    triggerP = comp.getNegative(SelfPort.class);
    expectP = comp.getPositive(SelfPort.class);
    timerP = comp.getNegative(Timer.class);
  }

  private TestContext<TorrentWrapperComp> getContext() {
    TorrentWrapperComp.Setup setup = new TorrentWrapperComp.Setup() {
      @Override
      public MultiFSM setupFSM(ComponentProxy proxy, Config config, R2TorrentComp.Ports ports) {
        try {
          R1FileDownload.ES fsmEs = new R1FileDownload.ES(selfAdr, 10, defaultBlock, new R1FileMngr());
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
          return R1FileDownload.FSM.multifsm(fsmIdFactory, fsmEs, oexa);
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

  //****************************************************START TO STORAGE_PENDING****************************************
  @Test
  public void testStartToStoragePending() {
    tc = tc.body();
    tc = startToStoragePending(tc, torrent, file, streamId);
    tc.repeat(1).body().end();
    assertTrue(tc.check());
    Identifier fsmBaseId = R1FileDownload.fsmBaseId(torrent, file);
    assertEquals(States.STORAGE_PENDING, compState.fsm.getFSMState(fsmBaseId));
  }

  private TestContext startToStoragePending(TestContext tc, OverlayId torrent, Identifier file, StreamId streamId) {
    tc = tFileOpen(tc, triggerP, open(torrent, file, streamId, null));
    tc = tc.expect(DStreamConnect.Request.class, streamCtrlP, Direction.OUT);
    return tc;
  }

  //***************************************************START TO STORAGE_SUCC********************************************
  @Test
  public void testStartToStorageSucc() {
    tc = tc.body();
    tc = startToStorageSucc(tc, torrent, file, streamId);
    tc.repeat(1).body().end();
    assertTrue(tc.check());
    Identifier fsmBaseId = R1FileDownload.fsmBaseId(torrent, file);
    assertEquals(States.STORAGE_SUCC, compState.fsm.getFSMState(fsmBaseId));
  }

  private TestContext startToStorageSucc(TestContext tc, OverlayId torrent, Identifier file, StreamId streamId) {
    tc = tFileOpen(tc, triggerP, open(torrent, file, streamId, null)); //1
    tc = myStorageStreamConnected(tc);//2-4
    return tc;
  }

  //***************************************************START TO ACTIVE**************************************************
  @Test
  public void testStartToActive() {
    tc = tc.body();
    tc = startToActive(tc, torrent, file, streamId, seeder1);
    tc.repeat(1).body().end();
    assertTrue(tc.check());
    Identifier fsmBaseId = R1FileDownload.fsmBaseId(torrent, file);
    assertEquals(States.ACTIVE, compState.fsm.getFSMState(fsmBaseId));
  }

  private TestContext startToActive(TestContext tc, OverlayId torrent, Identifier file, StreamId streamId,
    KAddress seeder) {
    tc = tFileOpen(tc, triggerP, open(torrent, file, streamId, null)); //1
    tc = myStorageStreamConnected(tc); //2-4
    tc = transferConnected(tc, torrent, file, seeder); //5-8
    return tc;
  }

  //***********************************************SEEDER CONNECT*******************************************************
  @Test
  public void testMultiSeederConnect() {
    tc = tc.body();
    tc = tFileOpen(tc, triggerP, open(torrent, file, streamId, null)); //1
    tc = myStorageStreamConnected(tc); //2-4
    tc = transferConnected(tc, torrent, file, seeder1); //5-8
    tc = transferConnected(tc, torrent, file, seeder2); //9-12
    tc = transferConnected(tc, torrent, file, seeder3); //13-16
    tc.repeat(1).body().end();
    assertTrue(tc.check());
    Identifier fsmBaseId = R1FileDownload.fsmBaseId(torrent, file);
    assertEquals(States.ACTIVE, compState.fsm.getFSMState(fsmBaseId));
  }
  
  //**********************************************SEEDER DISCONNECT*****************************************************
  @Test
  public void testSeederConnectDisc() {
    tc = tc.body();
    tc = tFileOpen(tc, triggerP, open(torrent, file, streamId, null)); //1
    tc = myStorageStreamConnected(tc); //2-4
    tc = transferConnected(tc, torrent, file, seeder1); //5-8
    tc = transferConnected(tc, torrent, file, seeder2); //9-12
    tc = transferConnected(tc, torrent, file, seeder3); //13-16
    tc = disconnectedTransfer(tc, torrent, file, seeder1); //17-20
    tc = disconnectedTransfer(tc, torrent, file, seeder2); //20-23
    tc = disconnectedTransfer(tc, torrent, file, seeder3); //24-27
    tc = eFileIndication(tc, States.IDLE); //28
    tc = transferConnected(tc, torrent, file, seeder1); //5-8
    tc.repeat(1).body().end();
    assertTrue(tc.check());
    Identifier fsmBaseId = R1FileDownload.fsmBaseId(torrent, file);
    assertEquals(States.ACTIVE, compState.fsm.getFSMState(fsmBaseId));
  }
  //*********************************************STORAGE_PENDING TO CLOSE***********************************************
  @Test
  public void testStoragePendingClose() {
    tc = tc.body();
    tc = startToStoragePending(tc, torrent, file, streamId);
    tc = tc.trigger(close(torrent, file), triggerP);
    tc = storageStreamDisconnected(tc, streamCtrlP, streamCtrlP);
    tc.repeat(1).body().end();
    assertTrue(tc.check());
    Identifier fsmBaseId = R1FileDownload.fsmBaseId(torrent, file);
    assertFalse(compState.fsm.activeFSM(fsmBaseId));
  }

  //*********************************************STORAGE_SUCCESS TO CLOSE***********************************************
  @Test
  public void testStorageSuccessClose() {
    tc = tc.body();
    tc = startToStorageSucc(tc, torrent, file, streamId);
    tc = tc.trigger(close(torrent, file), triggerP);
    tc = storageStreamDisconnected(tc, streamCtrlP, streamCtrlP);
    tc.repeat(1).body().end();
    assertTrue(tc.check());
    Identifier fsmBaseId = R1FileDownload.fsmBaseId(torrent, file);
    assertFalse(compState.fsm.activeFSM(fsmBaseId));
  }

  //***************************************************ACTIVE TO CLOSE**************************************************
  @Test
  public void testActiveClose() {
    tc = tc.body();
    tc = tFileOpen(tc, triggerP, open(torrent, file, streamId, null)); //1
    tc = myStorageStreamConnected(tc); //2-4
    tc = transferConnected(tc, torrent, file, seeder1); //5-8
    tc = disconnectActive(tc, torrent, file, new KAddress[]{seeder1});
    tc = eFileIndication(tc, States.CLOSE);
    tc.repeat(1).body().end();
    assertTrue(tc.check());
    Identifier fsmBaseId = R1FileDownload.fsmBaseId(torrent, file);
    assertFalse(compState.fsm.activeFSM(fsmBaseId));
  }

  //***************************************************IDLE TO CLOSE**************************************************
  @Test
  public void testIdleClose() {
    tc = tc.body();
    tc = tFileOpen(tc, triggerP, open(torrent, file, streamId, null)); //1
    tc = myStorageStreamConnected(tc); //2-4
    tc = transferConnected(tc, torrent, file, seeder1); //5-8
    tc = disconnectedTransfer(tc, torrent, file, seeder1); //9-12
    tc = eFileIndication(tc, States.IDLE); //13
    tc = disconnectActive(tc, torrent, file, new KAddress[]{});
    tc = eFileIndication(tc, States.CLOSE);
    tc.repeat(1).body().end();
    assertTrue(tc.check());
    Identifier fsmBaseId = R1FileDownload.fsmBaseId(torrent, file);
    assertFalse(compState.fsm.activeFSM(fsmBaseId));
  }

  //********************************************************************************************************************

  public TestContext disconnectActive(TestContext tc, OverlayId torrentId, Identifier fileId, KAddress[] seeders) {
    tc = tc.trigger(close(torrent, file), triggerP); //1
    for (KAddress seeder : seeders) {
      tc = disconnectTransfer(tc, torrentId, fileId, seeder); //2-4 - x times
    }
    tc = storageStreamDisconnected(tc, streamCtrlP, streamCtrlP);//2+3x
    return tc;
  }
  
  public TestContext disconnectedTransfer(TestContext tc, OverlayId torrentId, Identifier fileId, KAddress seeder) {
    tc = tc.trigger(disconnected(torrent, file, seeder), triggerP); //1
    tc = disconnectTransfer(tc, torrentId, fileId, seeder); //2-4
    return tc;
  }

  public TestContext disconnectTransfer(TestContext tc, OverlayId torrentId, Identifier fileId, KAddress seeder) {
    tc = eCancelPeriodicTimer(tc, timerP); //1
    tc = tc.expect(R1TransferSeederEvents.Disconnect.class, expectP, Direction.OUT); //2
    tc = tc.expect(R1FileDownloadEvents.Disconnected.class, expectP, Direction.OUT); //3
    return tc;
  }

  public TestContext myStorageStreamConnected(TestContext tc) {
    tc = storageStreamConnected(tc, streamCtrlP, streamCtrlP);//1-2
    tc = eFileIndication(tc, R1FileDownload.States.STORAGE_SUCC); //3
    return tc;
  }
  
  public TestContext transferConnected(TestContext tc, OverlayId torrentId, Identifier fileId, KAddress seeder) {
    tc = tc.trigger(connect(torrentId, fileId, seeder), triggerP); //1
    tc = transferSeederConnectSucc(tc, expectP, triggerP); //2-3
    tc = eSchedulePeriodicTimer(tc, R1DownloadTimeout.class, timerP); //4
    return tc;
  }
  
  public TestContext transferDisconnect(TestContext tc) {
    tc = tc.trigger(disconnected(torrent, file, seeder1), triggerP);
    return tc;
  }

  public TestContext eFileIndication(TestContext tc, States state) {
    Predicate p = fileIndication(state);
    return tc.expect(R1FileDownloadEvents.Indication.class, p, expectP, Direction.OUT);
  }

  public Predicate<R1FileDownloadEvents.Indication> fileIndication(States state) {
    return (R1FileDownloadEvents.Indication t) -> t.state.equals(state);
  }
  
  public R1FileDownloadEvents.Start open(OverlayId torrentId, Identifier fileId, StreamId streamId, MyStream stream) {
    return new R1FileDownloadEvents.Start(torrentId, fileId, streamId, stream);
  }

  public R1FileDownloadEvents.Close close(OverlayId torrentId, Identifier fileId) {
    return new R1FileDownloadEvents.Close(torrentId, fileId);
  }

  public R1FileDownloadEvents.Connect connect(OverlayId torrentId, Identifier fileId, KAddress seeder) {
    return new R1FileDownloadEvents.Connect(torrentId, fileId, seeder);
  }
  
  public R1TransferSeederEvents.Disconnected disconnected(OverlayId torrentId, Identifier fileId, KAddress seeder) {
    return new R1TransferSeederEvents.Disconnected(torrentId, fileId, seeder);
  }
}
