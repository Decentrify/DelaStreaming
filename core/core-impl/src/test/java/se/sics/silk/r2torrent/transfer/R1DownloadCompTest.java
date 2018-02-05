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
package se.sics.silk.r2torrent.transfer;

import com.google.common.base.Predicate;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;
import org.junit.After;
import static org.junit.Assert.assertTrue;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import se.sics.kompics.Component;
import se.sics.kompics.Port;
import se.sics.kompics.fsm.FSMException;
import se.sics.kompics.network.Msg;
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
import se.sics.nstream.util.BlockDetails;
import se.sics.silk.FutureHelper;
import se.sics.silk.SystemHelper;
import se.sics.silk.SystemSetup;
import se.sics.silk.TorrentTestHelper.CancelPeriodicTimerPredicate;
import static se.sics.silk.TorrentTestHelper.eSchedulePeriodicTimer;
import se.sics.silk.r2torrent.torrent.util.R1FileMetadata;
import se.sics.silk.r2torrent.transfer.events.R1DownloadEvents;
import se.sics.silk.r2torrent.transfer.events.R1DownloadTimeout;
import se.sics.silk.r2torrent.transfer.msgs.R1TransferMsgs;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class R1DownloadCompTest {
  
  private static OverlayIdFactory torrentIdFactory;
  
  private TestContext<R1DownloadComp> tc;
  private Component comp;
  private R1DownloadComp compState;
  private Port<Timer> timerP;
  private Port<Network> networkP;
  private Port<R1DownloadPort> downloadP;
  
  private IntIdFactory intIdFactory;
  private KAddress self;
  private KAddress seeder;
  private OverlayId torrent;
  private Identifier file;
  private R1FileMetadata fileMetadata;
  
  private CancelPeriodicTimerPredicate dcp;
  
  @BeforeClass
  public static void setup() throws FSMException {
    torrentIdFactory = SystemSetup.systemSetup("src/test/resources/application.conf");
    R1DownloadComp.HardCodedConfig.CWND_SIZE = 20;
  }

  @Before
  public void testSetup() {
    self = SystemHelper.getAddress(0);
    seeder = SystemHelper.getAddress(1);
    intIdFactory = new IntIdFactory(new Random());
    torrent = torrentIdFactory.id(new BasicBuilders.IntBuilder(1));
    file = intIdFactory.id(new BasicBuilders.IntBuilder(1));
    int pieceSize = 1024;
    int nrPieces = 10;
    int nrBlocks = 5;
    BlockDetails defaultBlock = new BlockDetails(pieceSize * nrPieces, nrPieces, pieceSize, pieceSize);
    fileMetadata = R1FileMetadata.instance(pieceSize * nrPieces * nrBlocks, defaultBlock);

    tc = getContext();
    comp = tc.getComponentUnderTest();
    compState = (R1DownloadComp) comp.getComponent();
    timerP = comp.getNegative(Timer.class);
    networkP = comp.getNegative(Network.class);
    downloadP = comp.getPositive(R1DownloadPort.class);
    
    dcp = new CancelPeriodicTimerPredicate();
  }
  
  private TestContext<R1DownloadComp> getContext() {
    R1DownloadComp.Init init = new R1DownloadComp.Init(self, torrent, file, seeder, fileMetadata);
    TestContext<R1DownloadComp> context = TestContext.newInstance(R1DownloadComp.class, init);
    return context;
  }

  @After
  public void clean() {
  }

  @Test
  public void testStart() {
    tc = tc.body();
    tc = eSchedulePeriodicTimer(tc, R1DownloadTimeout.class, timerP, dcp);
    tc.repeat(1).body().end();
    assertTrue(tc.check());
  }
  
  @Test
  public void downloadOneBlockInOrder() {
    Set<Integer> blocks = new TreeSet<>();
    blocks.add(0);
    
    Random rand = new Random();
    byte[] block = new byte[fileMetadata.defaultBlock.blockSize];
    rand.nextBytes(block);
    byte[] hash = HashUtil.makeHash(block, HashUtil.getAlgName(HashUtil.SHA));
    
    tc = tc.body();
    tc = eSchedulePeriodicTimer(tc, R1DownloadTimeout.class, timerP, dcp); //1
    tc = tc.trigger(getBlocks(torrent, file, seeder, blocks), downloadP); //1
    tc = cacheHintAccept(tc); //2 - e,t
    tc = hashSuccess(tc, hash); //2 - e,t
    tc = downloadBlock(tc, 0, block, hash);//5
    tc.repeat(1).body().end();
    assertTrue(tc.check());
  }
  
  @Test
  public void downloadFiveBlocksInOrder() {
    Set<Integer> blocks = new TreeSet<>();
    blocks.add(0);
    blocks.add(1);
    blocks.add(2);
    blocks.add(3);
    blocks.add(4);
    
    Random rand = new Random();
    byte[] block = new byte[fileMetadata.defaultBlock.blockSize];
    rand.nextBytes(block);
    byte[] hash = HashUtil.makeHash(block, HashUtil.getAlgName(HashUtil.SHA));
    tc = tc.body();
    tc = eSchedulePeriodicTimer(tc, R1DownloadTimeout.class, timerP, dcp); //1
    tc = tc.trigger(getBlocks(torrent, file, seeder, blocks), downloadP); //1
    tc = cacheHintAccept(tc); //2 - e,t
    tc = hashSuccess(tc, hash); //2 - e,t
    tc = downloadFiveBlock(tc, fileMetadata.defaultBlock, block, hash); //5
    tc.repeat(1).body().end();
    assertTrue(tc.check());
  }
  
  private TestContext downloadFiveBlock(TestContext tc, BlockDetails blockDetails, byte[] block, byte[] hash) {
    Future blockTF0 = blockTransferF(blockDetails, block);
    Future blockTF1 = blockTransferF(blockDetails, block);
    Future blockTF2 = blockTransferF(blockDetails, block);
    Future blockTF3 = blockTransferF(blockDetails, block);
    Future blockTF4 = blockTransferF(blockDetails, block);
    
    tc = blockReq(tc, blockTF0);
    tc = pieceResp(tc, blockTF0, 1);
    tc = blockReq(tc, blockTF1);
    tc = pieceResp(tc, blockTF0, 1);
    tc = blockReq(tc, blockTF2);
    tc = pieceResp(tc, blockTF0, blockDetails.nrPieces-2); //finish block1
    tc = completeBlock(tc, 0);
    tc = cacheHintAccept(tc);
    tc = pieceResp(tc, blockTF1, 1);
    tc = blockReq(tc, blockTF3);
    tc = pieceResp(tc, blockTF1, blockDetails.nrPieces-1); //finish block2
    tc = completeBlock(tc, 1);
    tc = cacheHintAccept(tc);
    tc = pieceResp(tc, blockTF2, 1);
    tc = blockReq(tc, blockTF4);
    tc = pieceResp(tc, blockTF2, blockDetails.nrPieces-1); //finish block2
    tc = completeBlock(tc, 2);
    tc = cacheHintAccept(tc);
    tc = pieceResp(tc, blockTF3, blockDetails.nrPieces);
    tc = completeBlock(tc, 3);
    tc = cacheHintAccept(tc);
    tc = pieceResp(tc, blockTF4, blockDetails.nrPieces);
    tc = completeBlock(tc, 4);
    tc = cacheHintAccept(tc);
    return tc;
  }
  
  private TestContext blockReq(TestContext tc, Future block) {
    tc.answerRequest(Msg.class, networkP, block);
    return tc;
  }
  
  private TestContext pieceResp(TestContext tc, Future piece, int nrPieces) {
    tc = tc.repeat(nrPieces).body();
    tc = tc.trigger(piece, networkP);
    tc = tc.end();
    return tc;
  }
  
  private TestContext completeBlock(TestContext tc, int block) {
    tc = tc.expect(R1DownloadEvents.Completed.class, blockP(block), downloadP, Direction.OUT); //1
    return tc;
  }
  private TestContext downloadBlock(TestContext tc, int blockNr, byte[] block, byte[] hash) {
    tc = blockDownload(tc, fileMetadata.defaultBlock, block);//2 - e,10t(repeated)
    tc = tc.expect(R1DownloadEvents.Completed.class, blockP(blockNr), downloadP, Direction.OUT); //1
    tc = cacheHintAccept(tc); //2 - e,t
    return tc;
  }
  
  private Predicate blockP(int blockNr) {
    return new Predicate<R1DownloadEvents.Completed>() {
      @Override
      public boolean apply(R1DownloadEvents.Completed t) {
        if(!HashUtil.checkHash(HashUtil.getAlgName(HashUtil.SHA), t.value, t.hash)) {
          return false;
        }
        return t.block == blockNr;
      }
    };
  }
  
  private TestContext cacheHintAccept(TestContext tc) {
    Future cacheF = new FutureHelper.NetBEFuture<R1TransferMsgs.CacheHintReq>(R1TransferMsgs.CacheHintReq.class) {
      @Override
      public Msg get() {
        return msg.answer(content.accept());
      }
    };
    tc = tc.answerRequest(Msg.class, networkP, cacheF);
    tc = tc.trigger(cacheF, networkP);
    return tc;
  }

  private TestContext hashSuccess(TestContext tc, byte[] hash) {
    Future hashF = new FutureHelper.NetBEFuture<R1TransferMsgs.HashReq>(R1TransferMsgs.HashReq.class) {

      @Override
      public Msg get() {
        Map<Integer, byte[]> hashes = new HashMap<>();
        content.hashes.stream().forEach((block) -> hashes.put(block, hash));
        return msg.answer(content.accept(hashes));
      }
    };
    tc = tc.answerRequest(Msg.class, networkP, hashF);
    tc = tc.trigger(hashF, networkP);
    return tc;
  }
  
  private TestContext blockDownload(TestContext tc, BlockDetails blockDetails, byte[] block) {
    Future blockTransferF = blockTransferF(blockDetails, block);
    tc = tc.answerRequest(Msg.class, networkP, blockTransferF);
    tc = tc.repeat(blockDetails.nrPieces).body();
    tc = tc.trigger(blockTransferF, networkP);
    tc = tc.end();
    return tc;
  }
  
  private Future blockTransferF(BlockDetails blockDetails, byte[] block) {
    return new FutureHelper.NetBEFuture<R1TransferMsgs.BlockReq>(R1TransferMsgs.BlockReq.class) {
      int i = 0;

      @Override
      public Msg get() {
        byte[] piece = Arrays.copyOfRange(block, i * blockDetails.defaultPieceSize, (i + 1)
          * blockDetails.defaultPieceSize);
        return msg.answer(content.piece(i++, piece));
      }
    };
  }

  public static R1DownloadEvents.GetBlocks getBlocks(OverlayId torrentId, Identifier fileId, KAddress seeder, 
    Set<Integer> blocks) {
    return new R1DownloadEvents.GetBlocks(torrentId, fileId, seeder.getId(), blocks);
  }
}
