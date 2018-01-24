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
import se.sics.ktoolbox.util.managedStore.core.util.HashUtil;
import se.sics.ktoolbox.util.network.KAddress;
import se.sics.nstream.StreamId;
import se.sics.nstream.storage.durable.DStoragePort;
import se.sics.nstream.storage.durable.DStreamControlPort;
import se.sics.nstream.storage.durable.events.DStreamConnect;
import se.sics.nstream.util.BlockDetails;
import se.sics.silk.SelfPort;
import se.sics.silk.SystemHelper;
import se.sics.silk.SystemSetup;
import se.sics.silk.TorrentIdHelper;
import static se.sics.silk.TorrentTestHelper.eSchedulePeriodicTimer;
import static se.sics.silk.TorrentTestHelper.storageStreamConnected;
import static se.sics.silk.TorrentTestHelper.tFileOpen;
import static se.sics.silk.TorrentTestHelper.transferSeederConnectSucc;
import se.sics.silk.TorrentWrapperComp;
import se.sics.silk.r2torrent.R2TorrentComp;
import se.sics.silk.r2torrent.torrent.R1FileDownload.States;
import se.sics.silk.r2torrent.torrent.event.R1FileDownloadEvents;
import se.sics.silk.r2torrent.torrent.util.R1FileMetadata;
import se.sics.silk.r2torrent.torrent.util.R1TorrentDetails;
import se.sics.silk.r2torrent.transfer.R1DownloadPort;
import se.sics.silk.r2torrent.transfer.events.R1DownloadEvents;
import se.sics.silk.r2torrent.transfer.events.R1DownloadTimeout;
import se.sics.silk.r2torrent.transfer.events.R1TransferSeederEvents;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class R1FileDownloadTest {

  private TestContext<TorrentWrapperComp> tc;
  private Component comp;
  private TorrentWrapperComp compState;
  private Port<SelfPort> triggerP;
  private Port<SelfPort> expectP;
  private Port<Timer> timerP;
  private Port<DStreamControlPort> streamCtrlP;
  private Port<DStoragePort> storagePort;
  private Port<R1DownloadPort> downloadP;
  private static OverlayIdFactory torrentIdFactory;
  private IntIdFactory intIdFactory;
  private KAddress selfAdr;
  private Identifier endpointId;
  private OverlayId torrent;
  private Identifier file1;
  private Identifier file2;
  private Identifier file3;
  private StreamId file1StreamId;
  private StreamId file2StreamId;
  private StreamId file3StreamId;
  private KAddress seeder1;
  private KAddress seeder2;
  private KAddress seeder3;
  private R1TorrentDetails torrentDetails;

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
    file1 = intIdFactory.id(new BasicBuilders.IntBuilder(1));
    file2 = intIdFactory.id(new BasicBuilders.IntBuilder(2));
    file3 = intIdFactory.id(new BasicBuilders.IntBuilder(3));
    file1StreamId = TorrentIdHelper.streamId(endpointId, torrent, file1);
    file2StreamId = TorrentIdHelper.streamId(endpointId, torrent, file2);
    file3StreamId = TorrentIdHelper.streamId(endpointId, torrent, file3);
    BlockDetails defaultBlock = new BlockDetails(1024 * 100, 100, 1024, 1024);
    R1FileMetadata fileMetadata = R1FileMetadata.instance(1024 * 100 * 2, defaultBlock);
    torrentDetails = new R1TorrentDetails(HashUtil.getAlgName(HashUtil.SHA));
    torrentDetails.addMetadata(file1, fileMetadata);
    torrentDetails.addMetadata(file2, fileMetadata);
    torrentDetails.addMetadata(file3, fileMetadata);
    torrentDetails.addStorage(file1, file1StreamId, null);
    torrentDetails.addStorage(file2, file2StreamId, null);
    torrentDetails.addStorage(file3, file3StreamId, null);
    Random rand = new Random();

    tc = getContext();
    comp = tc.getComponentUnderTest();
    compState = (TorrentWrapperComp) comp.getComponent();
    triggerP = comp.getNegative(SelfPort.class);
    expectP = comp.getPositive(SelfPort.class);
    timerP = comp.getNegative(Timer.class);
    streamCtrlP = comp.getNegative(DStreamControlPort.class);
    storagePort = comp.getNegative(DStoragePort.class);
    downloadP = comp.getNegative(R1DownloadPort.class);
  }

  private TestContext<TorrentWrapperComp> getContext() {
    TorrentWrapperComp.Setup setup = new TorrentWrapperComp.Setup() {
      @Override
      public MultiFSM setupFSM(ComponentProxy proxy, Config config, R2TorrentComp.Ports ports) {
        try {
          R1FileDownload.ES fsmEs = new R1FileDownload.ES(selfAdr, torrentDetails);
          fsmEs.setProxy(proxy);
          fsmEs.setPorts(ports);

          OnFSMExceptionAction oexa = (FSMException ex) -> {
            throw new RuntimeException(ex);
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
    tc = startToStoragePending(tc, torrent, file1, file1StreamId);
    tc.repeat(1).body().end();
    assertTrue(tc.check());
    Identifier fsmBaseId = R1FileDownload.fsmBaseId(torrent, file1);
    assertEquals(States.STORAGE_PENDING, compState.fsm.getFSMState(fsmBaseId));
  }

  private TestContext startToStoragePending(TestContext tc, OverlayId torrent, Identifier file, StreamId streamId) {
    tc = tFileOpen(tc, triggerP, open(torrent, file));
    tc = tc.expect(DStreamConnect.Request.class, streamCtrlP, Direction.OUT);
    return tc;
  }

  //***************************************************START TO STORAGE_SUCC********************************************
  @Test
  public void testStartToStorageSucc() {
    tc = tc.body();
    tc = startToStorageSucc(tc, torrent, file1, file1StreamId);
    tc.repeat(1).body().end();
    assertTrue(tc.check());
    Identifier fsmBaseId = R1FileDownload.fsmBaseId(torrent, file1);
    assertEquals(States.STORAGE_SUCC, compState.fsm.getFSMState(fsmBaseId));
  }

  private TestContext startToStorageSucc(TestContext tc, OverlayId torrent, Identifier file, StreamId streamId) {
    tc = tFileOpen(tc, triggerP, open(torrent, file)); //1
    tc = myStorageStreamConnected(tc);//2-4
    return tc;
  }

  //***************************************************START TO ACTIVE**************************************************
  @Test
  public void testStartToActive() {
    tc = tc.body();
    tc = startToActive(tc, torrent, file1, file1StreamId, seeder1);
    tc.repeat(1).body().end();
    assertTrue(tc.check());
    Identifier fsmBaseId = R1FileDownload.fsmBaseId(torrent, file1);
    assertEquals(States.ACTIVE, compState.fsm.getFSMState(fsmBaseId));
  }

  private TestContext startToActive(TestContext tc, OverlayId torrent, Identifier file, StreamId streamId,
    KAddress seeder) {
    tc = tFileOpen(tc, triggerP, open(torrent, file)); //1
    tc = myStorageStreamConnected(tc); //2-4
    tc = transferConnected(tc, torrent, file, seeder); //5-8
    tc = tc.expect(R1DownloadEvents.GetBlocks.class, downloadP, Direction.OUT); //9
    return tc;
  }
  
  //***************************************************DOWNLOAD_FILE****************************************************
  @Test
  public void testDownloadFile() {
    tc = tc.body();
    tc = tFileOpen(tc, triggerP, open(torrent, file1)); //1
    tc = myStorageStreamConnected(tc); //2-4
    tc = transferConnected(tc, torrent, file1, seeder1); //5-8
    tc.repeat(1).body().end();
    assertTrue(tc.check());
    Identifier fsmBaseId = R1FileDownload.fsmBaseId(torrent, file1);
    assertEquals(States.ACTIVE, compState.fsm.getFSMState(fsmBaseId));
  }
  
  TestContext getAllBlocksInOrder(TestContext tc, BlockDetails blockDetails, int nrBlocks) {
    tc = tc.expect(R1DownloadEvents.GetBlocks.class, downloadP, Direction.OUT);
    Random rand = new Random();
    for(int i= 0; i < nrBlocks; i++) {
      byte[] block = new byte[blockDetails.blockSize];
      rand.nextBytes(block);
      byte[] hash = HashUtil.makeHash(block, HashUtil.getAlgName(HashUtil.SHA));
      tr.trigger(R1DownloadEvents.Completed)
    }
    return tc;
  }
//  //**********************************************SEEDER DISCONNECT*****************************************************
//  @Test
//  public void testSeederConnectDisc() {
//    tc = tc.body();
//    tc = tFileOpen(tc, triggerP, open(torrent, file1)); //1
//    tc = myStorageStreamConnected(tc); //2-4
//    tc = transferConnected(tc, torrent, file1, seeder1); //5-8
//    tc = tc.expect(R1DownloadEvents.GetBlocks.class, downloadP, Direction.OUT); //10
//    tc = disconnectedTransfer(tc, torrent, file1, seeder1); //11-14
//    tc = eFileIndication(tc, States.IDLE); //15
//    tc = transferConnected(tc, torrent, file1, seeder1); //16-19
//    tc = tc.expect(R1DownloadEvents.GetBlocks.class, downloadP, Direction.OUT); //
//    tc.repeat(1).body().end();
//    assertTrue(tc.check());
//    Identifier fsmBaseId = R1FileDownload.fsmBaseId(torrent, file1);
//    assertEquals(States.ACTIVE, compState.fsm.getFSMState(fsmBaseId));
//  }
//
//  //*********************************************STORAGE_PENDING TO CLOSE***********************************************
//  @Test
//  public void testStoragePendingClose() {
//    tc = tc.body();
//    tc = startToStoragePending(tc, torrent, file1, file1StreamId);
//    tc = tc.trigger(close(torrent, file1), triggerP);
//    tc = storageStreamDisconnected(tc, streamCtrlP, streamCtrlP);
//    tc.repeat(1).body().end();
//    assertTrue(tc.check());
//    Identifier fsmBaseId = R1FileDownload.fsmBaseId(torrent, file1);
//    assertFalse(compState.fsm.activeFSM(fsmBaseId));
//  }
//
//  //*********************************************STORAGE_SUCCESS TO CLOSE***********************************************
//  @Test
//  public void testStorageSuccessClose() {
//    tc = tc.body();
//    tc = startToStorageSucc(tc, torrent, file1, file1StreamId);
//    tc = tc.trigger(close(torrent, file1), triggerP);
//    tc = storageStreamDisconnected(tc, streamCtrlP, streamCtrlP);
//    tc.repeat(1).body().end();
//    assertTrue(tc.check());
//    Identifier fsmBaseId = R1FileDownload.fsmBaseId(torrent, file1);
//    assertFalse(compState.fsm.activeFSM(fsmBaseId));
//  }
//
//  //***************************************************ACTIVE TO CLOSE**************************************************
//  @Test
//  public void testActiveClose() {
//    tc = tc.body();
//    tc = tFileOpen(tc, triggerP, open(torrent, file1)); //1
//    tc = myStorageStreamConnected(tc); //2-4
//    tc = transferConnected(tc, torrent, file1, seeder1); //5-8
//    tc = disconnectActive(tc, torrent, file1, new KAddress[]{seeder1});
//    tc = eFileIndication(tc, States.CLOSE);
//    tc.repeat(1).body().end();
//    assertTrue(tc.check());
//    Identifier fsmBaseId = R1FileDownload.fsmBaseId(torrent, file1);
//    assertFalse(compState.fsm.activeFSM(fsmBaseId));
//  }
//
//  //***************************************************IDLE TO CLOSE**************************************************
//  @Test
//  public void testIdleClose() {
//    tc = tc.body();
//    tc = tFileOpen(tc, triggerP, open(torrent, file1)); //1
//    tc = myStorageStreamConnected(tc); //2-4
//    tc = transferConnected(tc, torrent, file1, seeder1); //5-8
//    tc = disconnectedTransfer(tc, torrent, file1, seeder1); //9-12
//    tc = eFileIndication(tc, States.IDLE); //13
//    tc = disconnectActive(tc, torrent, file1, new KAddress[]{});
//    tc = eFileIndication(tc, States.CLOSE);
//    tc.repeat(1).body().end();
//    assertTrue(tc.check());
//    Identifier fsmBaseId = R1FileDownload.fsmBaseId(torrent, file1);
//    assertFalse(compState.fsm.activeFSM(fsmBaseId));
//  }

  //********************************************************************************************************************
//  public TestContext disconnectActive(TestContext tc, OverlayId torrentId, Identifier fileId, KAddress[] seeders) {
//    tc = tc.trigger(close(torrent, file1), triggerP); //1
//    for (KAddress seeder : seeders) {
//      tc = disconnectTransfer(tc, torrentId, fileId, seeder); //2-4 - x times
//    }
//    tc = storageStreamDisconnected(tc, streamCtrlP, streamCtrlP);//2+3x
//    return tc;
//  }
//
//  public TestContext disconnectedTransfer(TestContext tc, OverlayId torrentId, Identifier fileId, KAddress seeder) {
//    tc = tc.trigger(disconnected(torrent, file1, seeder), triggerP); //1
//    tc = eCancelPeriodicTimer(tc, timerP); //1
//    tc = tc.expect(R1TransferSeederEvents.Disconnect.class, expectP, Direction.OUT); //2
//    return tc;
//  }
//
//  public TestContext disconnectTransfer(TestContext tc, OverlayId torrentId, Identifier fileId, KAddress seeder) {
//    tc = tc.trigger(disconnect(torrent, file1, seeder), triggerP); //1
//    tc = eCancelPeriodicTimer(tc, timerP); //1
//    tc = tc.expect(R1TransferSeederEvents.Disconnect.class, expectP, Direction.OUT); //2
//    return tc;
//  }

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

//  public TestContext transferDisconnect(TestContext tc) {
//    tc = tc.trigger(disconnected(torrent, file1, seeder1), triggerP);
//    return tc;
//  }

  public TestContext eFileIndication(TestContext tc, States state) {
    Predicate p = fileIndication(state);
    return tc.expect(R1FileDownloadEvents.Indication.class, p, expectP, Direction.OUT);
  }

  public Predicate<R1FileDownloadEvents.Indication> fileIndication(States state) {
    return (R1FileDownloadEvents.Indication t) -> t.state.equals(state);
  }

  public R1FileDownloadEvents.Start open(OverlayId torrentId, Identifier fileId) {
    return new R1FileDownloadEvents.Start(torrentId, fileId);
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
  
  public R1FileDownloadEvents.Disconnect disconnect(OverlayId torrentId, Identifier fileId, KAddress seeder) {
    return new R1FileDownloadEvents.Disconnect(torrentId, fileId, seeder.getId());
  }
  
  public R1DownloadEvents.Completed completed(OverlayId torrentId, Identifier fileId, KAddress seeder) {
    
  }
}
