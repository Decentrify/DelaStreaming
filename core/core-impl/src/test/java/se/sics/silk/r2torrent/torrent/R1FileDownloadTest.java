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
import java.util.Set;
import java.util.TreeSet;
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
import se.sics.kompics.network.Network;
import se.sics.kompics.testing.Direction;
import se.sics.kompics.testing.Future;
import se.sics.kompics.testing.TestContext;
import se.sics.kompics.timer.Timer;
import se.sics.kompics.util.Identifier;
import se.sics.ktoolbox.util.identifiable.BasicBuilders;
import se.sics.ktoolbox.util.identifiable.basic.IntIdFactory;
import se.sics.ktoolbox.util.identifiable.overlay.OverlayId;
import se.sics.ktoolbox.util.identifiable.overlay.OverlayIdFactory;
import se.sics.ktoolbox.util.managedStore.core.util.HashUtil;
import se.sics.ktoolbox.util.network.KAddress;
import se.sics.ktoolbox.util.result.Result;
import se.sics.nstream.StreamId;
import se.sics.nstream.storage.durable.DStoragePort;
import se.sics.nstream.storage.durable.DStreamControlPort;
import se.sics.nstream.storage.durable.DurableStorageProvider;
import se.sics.nstream.storage.durable.events.DStorageWrite;
import se.sics.nstream.storage.durable.events.DStreamConnect;
import se.sics.nstream.util.BlockDetails;
import se.sics.silk.FutureHelper;
import se.sics.silk.FutureHelper.BasicFuture;
import se.sics.silk.SelfPort;
import se.sics.silk.SystemHelper;
import se.sics.silk.SystemSetup;
import se.sics.silk.TorrentIdHelper;
import se.sics.silk.TorrentTestHelper.CancelPeriodicTimerPredicate;
import static se.sics.silk.TorrentTestHelper.storageStreamConnected;
import static se.sics.silk.TorrentTestHelper.storageStreamDisconnected;
import static se.sics.silk.TorrentTestHelper.tFileOpen;
import static se.sics.silk.TorrentTestHelper.transferSeederConnectSucc;
import se.sics.silk.TorrentWrapperComp;
import se.sics.silk.r2torrent.R2TorrentComp;
import se.sics.silk.r2torrent.torrent.R1FileDownload.States;
import se.sics.silk.r2torrent.torrent.event.R1FileDownloadEvents;
import se.sics.silk.r2torrent.torrent.util.R1FileMetadata;
import se.sics.silk.r2torrent.torrent.util.R1TorrentDetails;
import se.sics.silk.r2torrent.transfer.R1DownloadPort;
import se.sics.silk.r2torrent.transfer.R1TransferSeederCtrl;
import se.sics.silk.r2torrent.transfer.events.R1DownloadEvents;
import se.sics.silk.r2torrent.transfer.events.R1TransferSeederEvents;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class R1FileDownloadTest {

  private static OverlayIdFactory torrentIdFactory;

  private TestContext<TorrentWrapperComp> tc;
  private Component comp;
  private TorrentWrapperComp compState;
  private Port<SelfPort> expectP;
  private Port<Timer> timerP;
  private Port<Network> networkP;
  private Port<R1FileDownloadCtrl> fileDownloadCtrl;
  private Port<DStreamControlPort> streamCtrlP;
  private Port<DStoragePort> storageP;
  private Port<R1DownloadPort> fileDownloadP;
  private Port<R1TransferSeederCtrl> transferSeederP;
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
  private final R1TorrentDetails.Mngr torrentDetailsMngr = new R1TorrentDetails.Mngr();
  private CancelPeriodicTimerPredicate pcp;

  @BeforeClass
  public static void setup() throws FSMException {
    R1FileDownload.HardCodedConfig.DOWNLOAD_COMP_BUFFER_SIZE = 5;
    R1FileDownload.HardCodedConfig.BLOCK_BATCH_REQUEST = 2;
    torrentIdFactory = SystemSetup.systemSetup("src/test/resources/application.conf");
  }

  @Before
  public void testSetup() {
    intIdFactory = new IntIdFactory(new Random());
    selfAdr = SystemHelper.getAddress(0);
    seeder1 = SystemHelper.getAddress(1);
    seeder2 = SystemHelper.getAddress(2);
    seeder3 = SystemHelper.getAddress(3);
    torrentDetails();

    tc = getContext();
    comp = tc.getComponentUnderTest();
    compState = (TorrentWrapperComp) comp.getComponent();
    fileDownloadCtrl = comp.getPositive(R1FileDownloadCtrl.class);
    expectP = comp.getPositive(SelfPort.class);
    timerP = comp.getNegative(Timer.class);
    networkP = comp.getNegative(Network.class);
    streamCtrlP = comp.getNegative(DStreamControlPort.class);
    storageP = comp.getNegative(DStoragePort.class);
    fileDownloadP = comp.getNegative(R1DownloadPort.class);
    transferSeederP = comp.getNegative(R1TransferSeederCtrl.class);
  }
  
  private void torrentDetails() {
    endpointId = intIdFactory.id(new BasicBuilders.IntBuilder(0));
    DurableStorageProvider endpoint = null;
    torrent = torrentIdFactory.id(new BasicBuilders.IntBuilder(1));
    file1 = intIdFactory.id(new BasicBuilders.IntBuilder(1));
    file2 = intIdFactory.id(new BasicBuilders.IntBuilder(2));
    file3 = intIdFactory.id(new BasicBuilders.IntBuilder(3));
    file1StreamId = TorrentIdHelper.streamId(endpointId, torrent, file1);
    file2StreamId = TorrentIdHelper.streamId(endpointId, torrent, file2);
    file3StreamId = TorrentIdHelper.streamId(endpointId, torrent, file3);
    int pieceSize = 1024;
    int nrPieces = 10;
    int nrBlocks = 5;
    BlockDetails defaultBlock = new BlockDetails(pieceSize * nrPieces, nrPieces, pieceSize, pieceSize);
    R1FileMetadata fileMetadata = R1FileMetadata.instance(pieceSize * nrPieces * nrBlocks, defaultBlock);
    torrentDetails = new R1TorrentDetails(HashUtil.getAlgName(HashUtil.SHA), endpointId, endpoint);
    torrentDetails.addMetadata(file1, fileMetadata);
    torrentDetails.addMetadata(file2, fileMetadata);
    torrentDetails.addMetadata(file3, fileMetadata);
    torrentDetails.addStorage(file1, file1StreamId, null);
    torrentDetails.addStorage(file2, file2StreamId, null);
    torrentDetails.addStorage(file3, file3StreamId, null);
    torrentDetailsMngr.addTorrent(torrent, torrentDetails);
  }

  private TestContext<TorrentWrapperComp> getContext() {
    TorrentWrapperComp.Setup setup = new TorrentWrapperComp.Setup() {
      @Override
      public MultiFSM setupFSM(ComponentProxy proxy, Config config, R2TorrentComp.Ports ports) {
        try {
          R1FileDownload.ES fsmEs = new R1FileDownload.ES(selfAdr, torrentDetailsMngr);
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
    tc = tFileOpen(tc, fileDownloadCtrl, download(torrent, file));
    tc = tc.expect(DStreamConnect.Request.class, streamCtrlP, Direction.OUT);
    return tc;
  }

  //********************************************************START TO IDLE***********************************************
  @Test
  public void testStartToIdle() {
    tc = tc.body();
    tc = startToStorageSucc(tc, torrent, file1, file1StreamId);
    tc.repeat(1).body().end();
    assertTrue(tc.check());
    Identifier fsmBaseId = R1FileDownload.fsmBaseId(torrent, file1);
    assertEquals(States.IDLE, compState.fsm.getFSMState(fsmBaseId));
  }

  private TestContext startToStorageSucc(TestContext tc, OverlayId torrent, Identifier file, StreamId streamId) {
    tc = tFileOpen(tc, fileDownloadCtrl, download(torrent, file)); //1
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
    tc = tFileOpen(tc, fileDownloadCtrl, download(torrent, file)); //1
    tc = myStorageStreamConnected(tc); //2-4
    tc = transferConnected(tc, torrent, file, seeder); //5-8
    tc = tc.expect(R1DownloadEvents.GetBlocks.class, fileDownloadP, Direction.OUT); //9
    return tc;
  }

  //***************************************************DOWNLOAD_FILE****************************************************
//  @Test
  public void testDownloadFileInOrder() {
    R1FileMetadata fm = torrentDetails.getMetadata(file1).get();

    Random rand = new Random();
    byte[] block = new byte[fm.defaultBlock.blockSize];
    rand.nextBytes(block);
    byte[] hash = HashUtil.makeHash(block, HashUtil.getAlgName(HashUtil.SHA));

//    tc = tc.setTimeout(1000 * 60 * 10);
    tc = tc.body();
    tc = tFileOpen(tc, fileDownloadCtrl, download(torrent, file1)); //1
    tc = myStorageStreamConnected(tc); //3
    tc = transferConnected(tc, torrent, file1, seeder1); //4
    tc = downloadInOrder(tc, torrent, file1, seeder1, block, hash); //18
    tc = transferCompleted(tc, torrent, file1, seeder1); //4
//    tc = storageStreamDisconnected(tc, streamCtrlP, streamCtrlP);//2
    tc.repeat(1).body().end();
    assertTrue(tc.check());
    Identifier fsmBaseId = R1FileDownload.fsmBaseId(torrent, file1);
    assertFalse(compState.fsm.activeFSM(fsmBaseId));
  }
  
//  @Test
  public void testDownloadFileOutOfOrder() {
    R1FileMetadata fm = torrentDetails.getMetadata(file1).get();

    Random rand = new Random();
    byte[] block = new byte[fm.defaultBlock.blockSize];
    rand.nextBytes(block);
    byte[] hash = HashUtil.makeHash(block, HashUtil.getAlgName(HashUtil.SHA));

//    tc = tc.setTimeout(1000 * 60 * 10);
    tc = tc.body();
    tc = tFileOpen(tc, fileDownloadCtrl, download(torrent, file1)); //1
    tc = myStorageStreamConnected(tc); //3
    tc = transferConnected(tc, torrent, file1, seeder1); //4
    tc = downloadOutOfOrder(tc, torrent, file1, seeder1, block, hash); //18
    tc = transferCompleted(tc, torrent, file1, seeder1); //4
    tc = storageStreamDisconnected(tc, streamCtrlP, streamCtrlP);//2
    tc.repeat(1).body().end();
    assertTrue(tc.check());
    Identifier fsmBaseId = R1FileDownload.fsmBaseId(torrent, file1);
    assertFalse(compState.fsm.activeFSM(fsmBaseId));
  }

  private TestContext downloadInOrder(TestContext tc, OverlayId torrentId, Identifier fileId, KAddress seeder,
    byte[] block, byte[] hash) {
    Set<Integer> blocks1 = new TreeSet<>();
    blocks1.add(0);
    blocks1.add(1);
    Set<Integer> blocks2 = new TreeSet<>();
    blocks2.add(2);
    blocks2.add(3);
    Set<Integer> blocks3 = new TreeSet<>();
    blocks3.add(4);
    tc = tc.expect(R1DownloadEvents.GetBlocks.class, getBlock(blocks1), fileDownloadP, Direction.OUT);
    tc = tc.trigger(completed(torrentId, fileId, seeder, 0, block, hash), fileDownloadP);
    tc = blockWriteToStorage(tc); //2
    tc = tc.expect(R1DownloadEvents.GetBlocks.class, getBlock(blocks2), fileDownloadP, Direction.OUT);
    tc = tc.trigger(completed(torrentId, fileId, seeder, 1, block, hash), fileDownloadP);
    tc = blockWriteToStorage(tc);//2
    tc = tc.expect(R1DownloadEvents.GetBlocks.class, getBlock(blocks3), fileDownloadP, Direction.OUT);
    tc = tc.trigger(completed(torrentId, fileId, seeder, 2, block, hash), fileDownloadP);
    tc = blockWriteToStorage(tc);//2
    tc = tc.trigger(completed(torrentId, fileId, seeder, 3, block, hash), fileDownloadP);
    tc = blockWriteToStorage(tc);//2
    tc = tc.trigger(completed(torrentId, fileId, seeder, 4, block, hash), fileDownloadP);
    tc = blockWriteToStorage(tc);//2
    return tc;
  }
  
  private TestContext downloadOutOfOrder(TestContext tc, OverlayId torrentId, Identifier fileId, KAddress seeder,
    byte[] block, byte[] hash) {
    Set<Integer> blocks1 = new TreeSet<>();
    blocks1.add(0);
    blocks1.add(1);
    Set<Integer> blocks2 = new TreeSet<>();
    blocks2.add(2);
    blocks2.add(3);
    Set<Integer> blocks3 = new TreeSet<>();
    blocks3.add(4);
    tc = tc.expect(R1DownloadEvents.GetBlocks.class, getBlock(blocks1), fileDownloadP, Direction.OUT);
    tc = tc.trigger(completed(torrentId, fileId, seeder, 1, block, hash), fileDownloadP);
    tc = blockWriteToStorage(tc);//2
    tc = tc.expect(R1DownloadEvents.GetBlocks.class, getBlock(blocks2), fileDownloadP, Direction.OUT);
    tc = tc.trigger(completed(torrentId, fileId, seeder, 2, block, hash), fileDownloadP);
    tc = blockWriteToStorage(tc);//2
    tc = tc.expect(R1DownloadEvents.GetBlocks.class, getBlock(blocks3), fileDownloadP, Direction.OUT);
    tc = tc.trigger(completed(torrentId, fileId, seeder, 3, block, hash), fileDownloadP);
    tc = blockWriteToStorage(tc);//2
    tc = tc.trigger(completed(torrentId, fileId, seeder, 4, block, hash), fileDownloadP);
    tc = blockWriteToStorage(tc);//2
    tc = tc.trigger(completed(torrentId, fileId, seeder, 0, block, hash), fileDownloadP);
    tc = blockWriteToStorage(tc);//2
    return tc;
  }

  public Predicate getBlock(Set<Integer> blocks) {
    return (Predicate<R1DownloadEvents.GetBlocks>) (R1DownloadEvents.GetBlocks t) -> t.blocks.equals(blocks);
  }

  private TestContext blockWriteToStorage(TestContext tc) {
    Future blockWriteF = blockWriteF();
    tc = tc.answerRequest(DStorageWrite.Request.class, storageP, blockWriteF);
    tc = tc.trigger(blockWriteF, storageP);
    return tc;
  }

  private Future blockWriteF() {
    return new FutureHelper.BasicFuture<DStorageWrite.Request, DStorageWrite.Response>() {
      @Override
      public DStorageWrite.Response get() {
        return event.respond(Result.success(true));
      }
    };
  }

  public TestContext myStorageStreamConnected(TestContext tc) {
    tc = storageStreamConnected(tc, streamCtrlP, streamCtrlP);//1-2
    tc = eFileIndication(tc, R1FileDownload.States.IDLE); //3
    return tc;
  }
  
  public TestContext transferConnected(TestContext tc, OverlayId torrentId, Identifier fileId, KAddress seeder) {
    tc = tc.trigger(connect(torrentId, fileId, seeder), fileDownloadCtrl); //1
    tc = transferSeederConnectSucc(tc, transferSeederP, transferSeederP); //2-3
    return tc;
  }
  
  public TestContext transferCompleted(TestContext tc, OverlayId torrentId, Identifier fileId, KAddress seeder) {
    Future f = new BasicFuture<R1TransferSeederEvents.Disconnect, R1TransferSeederEvents.Disconnected>() {

      @Override
      public R1TransferSeederEvents.Disconnected get() {
        return event.ack();
      }
    };
    tc = tc.answerRequest(R1TransferSeederEvents.Disconnect.class, transferSeederP, f);
    tc = tc.expect(R1FileDownloadEvents.Disconnected.class, fileDownloadCtrl, Direction.OUT);
    tc = eFileIndication(tc, States.COMPLETED);
    tc = tc.trigger(f, transferSeederP);
    return tc;
  }

  public TestContext eFileIndication(TestContext tc, States state) {
    Predicate p = fileIndication(state);
    return tc.expect(R1FileDownloadEvents.Indication.class, p, fileDownloadCtrl, Direction.OUT);
  }

  public Predicate<R1FileDownloadEvents.Indication> fileIndication(States state) {
    return (R1FileDownloadEvents.Indication t) -> t.state.equals(state);
  }

  public R1FileDownloadEvents.Start download(OverlayId torrentId, Identifier fileId) {
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

  public R1DownloadEvents.Completed completed(OverlayId torrentId, Identifier fileId, KAddress seeder,
    int blockNr, byte[] block, byte[] hash) {
    return new R1DownloadEvents.Completed(torrentId, fileId, seeder.getId(), blockNr, block, hash);
  }
}
