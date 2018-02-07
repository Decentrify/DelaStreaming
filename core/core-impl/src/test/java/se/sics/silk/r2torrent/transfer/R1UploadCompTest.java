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
import java.util.LinkedList;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;
import org.junit.After;
import static org.junit.Assert.assertTrue;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import se.sics.kompics.Component;
import se.sics.kompics.KompicsEvent;
import se.sics.kompics.Port;
import se.sics.kompics.fsm.FSMException;
import se.sics.kompics.network.Msg;
import se.sics.kompics.network.Network;
import se.sics.kompics.network.Transport;
import se.sics.kompics.testing.Direction;
import se.sics.kompics.testing.Future;
import se.sics.kompics.testing.TestContext;
import se.sics.kompics.timer.SchedulePeriodicTimeout;
import se.sics.kompics.timer.Timer;
import se.sics.kompics.util.Identifier;
import se.sics.ktoolbox.util.identifiable.BasicBuilders;
import se.sics.ktoolbox.util.identifiable.basic.IntIdFactory;
import se.sics.ktoolbox.util.identifiable.overlay.OverlayId;
import se.sics.ktoolbox.util.identifiable.overlay.OverlayIdFactory;
import se.sics.ktoolbox.util.managedStore.core.util.HashUtil;
import se.sics.ktoolbox.util.network.KAddress;
import se.sics.ktoolbox.util.network.KHeader;
import se.sics.ktoolbox.util.network.basic.BasicContentMsg;
import se.sics.ktoolbox.util.network.basic.BasicHeader;
import se.sics.ktoolbox.util.reference.KReference;
import se.sics.ktoolbox.util.reference.KReferenceFactory;
import se.sics.nstream.storage.cache.KHint;
import se.sics.nstream.util.BlockDetails;
import se.sics.silk.PredicateHelper;
import se.sics.silk.PredicateHelper.ContentPredicate;
import se.sics.silk.PredicateHelper.MsgPredicate;
import se.sics.silk.SystemHelper;
import se.sics.silk.SystemSetup;
import se.sics.silk.TorrentTestHelper;
import static se.sics.silk.TorrentTestHelper.eSchedulePeriodicTimer;
import se.sics.silk.r2torrent.torrent.util.R1FileMetadata;
import se.sics.silk.r2torrent.transfer.R1UploadComp.HardCodedConfig;
import se.sics.silk.r2torrent.transfer.events.R1UploadEvents;
import se.sics.silk.r2torrent.transfer.events.R1UploadTimeout;
import se.sics.silk.r2torrent.transfer.msgs.R1TransferMsgs;

/**
 *
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class R1UploadCompTest {

  private static OverlayIdFactory torrentIdFactory;

  private TestContext<R1UploadComp> tc;
  private Component comp;
  private R1UploadComp compState;
  private Port<Timer> timerP;
  private Port<Network> networkP;
  private Port<R1UploadPort> uploadP;

  private Random rand = new Random(1234);

  private IntIdFactory intIdFactory;
  private KAddress self;
  private KAddress leecher;
  private OverlayId torrent;
  private Identifier file;
  private R1FileMetadata fileMetadata;

  private TorrentTestHelper.CancelPeriodicTimerPredicate dcp;

  @BeforeClass
  public static void setup() throws FSMException {
    R1UploadComp.HardCodedConfig.cwndSize = 5;
    torrentIdFactory = SystemSetup.systemSetup("src/test/resources/application.conf");
  }

  @Before
  public void testSetup() {
    self = SystemHelper.getAddress(0);
    leecher = SystemHelper.getAddress(1);
    intIdFactory = new IntIdFactory(new Random());
    torrent = torrentIdFactory.id(new BasicBuilders.IntBuilder(1));
    file = intIdFactory.id(new BasicBuilders.IntBuilder(1));
    int pieceSize = 1024;
    int nrPieces = 10;
    int nrBlocks = 3;
    BlockDetails defaultBlock = new BlockDetails(pieceSize * nrPieces, nrPieces, pieceSize, pieceSize);
    fileMetadata = R1FileMetadata.instance(pieceSize * nrPieces * nrBlocks, defaultBlock);

    tc = getContext();
    comp = tc.getComponentUnderTest();
    compState = (R1UploadComp) comp.getComponent();
    timerP = comp.getNegative(Timer.class);
    networkP = comp.getNegative(Network.class);
    uploadP = comp.getNegative(R1UploadPort.class);

    dcp = new TorrentTestHelper.CancelPeriodicTimerPredicate();
  }

  private TestContext<R1UploadComp> getContext() {
    R1UploadComp.Init init = new R1UploadComp.Init(self, torrent, file, leecher, fileMetadata);
    TestContext<R1UploadComp> context = TestContext.newInstance(R1UploadComp.class, init);
    return context;
  }

  @After
  public void clean() {
  }

  @Test
  public void testStart() {
    tc = tc.body();
    tc = eSchedulePeriodicTimer(tc, R1UploadTimeout.class, timerP, dcp);
    tc.repeat(1).body().end();
    assertTrue(tc.check());
  }

  @Test
  public void testUpload() {
    tc = tc.body();
    tc = eSchedulePeriodicTimer(tc, R1UploadTimeout.class, timerP, dcp);
    tc = uploadFile(tc);
    tc.repeat(1).body().end();
    assertTrue(tc.check());
  }

  private TestContext uploadFile(TestContext tc) {
    Set<Integer> blocks1 = new TreeSet<>();
    blocks1.add(0);
    blocks1.add(1);
    blocks1.add(2);
    Set<Integer> blocks2 = new TreeSet<>();
    blocks2.add(1);
    blocks2.add(2);
    Set<Integer> blocks3 = new TreeSet<>();
    blocks3.add(2);
    Set<Integer> blocks4 = new TreeSet<>();
    tc = cacheHint(tc, torrent, file, fileMetadata.defaultBlock, self, leecher, new KHint.Summary(0, blocks1), 3);
    tc = hashReq(tc, torrent, file, self, self, blocks1);
    tc = blockReq(tc, torrent, file, fileMetadata.defaultBlock, 0, self, leecher);
    tc = clearCacheHint(tc, torrent, file, self, leecher, new KHint.Summary(1, blocks2));
    tc = blockReq(tc, torrent, file, fileMetadata.defaultBlock, 1, self, leecher);
    tc = clearCacheHint(tc, torrent, file, self, leecher, new KHint.Summary(2, blocks3));
    tc = blockReq(tc, torrent, file, fileMetadata.defaultBlock, 2, self, leecher);
    tc = clearCacheHint(tc, torrent, file, self, leecher, new KHint.Summary(3, blocks4));
    return tc;
  }

  public TestContext blockReq(TestContext tc, OverlayId torrentId, Identifier fileI, BlockDetails blockDetails,
    int blockNr, KAddress src, KAddress dst) {
    Predicate p = new MsgPredicate(PredicateHelper.TRUE_P, new ContentPredicate(R1TransferMsgs.PieceResp.class));
    tc = tc.trigger(blockReq(torrentId, fileI, src, dst, blockDetails, blockNr), networkP);
    tc = tc.repeat(2).body();
    tc = tc.trigger(uploadTimeout(), timerP);
    tc = tc.repeat(HardCodedConfig.cwndSize).body();
    tc = tc.expect(Msg.class, p, networkP, Direction.OUT);
    tc = tc.end();
    tc = tc.end();
    return tc;
  }

  public TestContext hashReq(TestContext tc, OverlayId torrentId, Identifier fileI, KAddress src, KAddress dst,
    Set<Integer> hashes) {
    Predicate p = new MsgPredicate(PredicateHelper.TRUE_P, new ContentPredicate(R1TransferMsgs.HashResp.class));
    tc = tc.trigger(hashReq(torrentId, fileI, src, dst, hashes), networkP);
    tc = tc.expect(Msg.class, p, networkP, Direction.OUT);
    return tc;
  }

  public TestContext cacheHint(TestContext tc, OverlayId torrentId, Identifier fileI, BlockDetails defaultDetails,
    KAddress src, KAddress dst, KHint.Summary cacheHint, int nrBlocks) {
    Predicate p = new MsgPredicate(PredicateHelper.TRUE_P, new ContentPredicate(R1TransferMsgs.CacheHintAcc.class));
    tc = tc.trigger(cacheHintReq(torrentId, fileI, src, dst, cacheHint), networkP);
    tc = serveBlocks(tc, defaultDetails, nrBlocks);
    tc = tc.expect(Msg.class, p, networkP, Direction.OUT);
    return tc;
  }

  public TestContext clearCacheHint(TestContext tc, OverlayId torrentId, Identifier fileI,
    KAddress src, KAddress dst, KHint.Summary cacheHint) {
    Predicate p = new MsgPredicate(PredicateHelper.TRUE_P, new ContentPredicate(R1TransferMsgs.CacheHintAcc.class));
    tc = tc.trigger(cacheHintReq(torrentId, fileI, src, dst, cacheHint), networkP);
    tc = tc.expect(Msg.class, p, networkP, Direction.OUT);
    return tc;
  }

  private TestContext serveBlocks(TestContext tc, BlockDetails blockDetails, int nrBlocks) {
    
    Future f = new Future<R1UploadEvents.BlocksReq, R1UploadEvents.BlockResp>() {
      R1UploadEvents.BlocksReq event;
      LinkedList<Integer> aux = new LinkedList<>();
      
      @Override
      public R1UploadEvents.BlockResp get() {
        int blockNr = aux.pollFirst();
        byte[] blockVal = new byte[blockDetails.blockSize];
        rand.nextBytes(blockVal);
        KReference<byte[]> block = KReferenceFactory.getReference(blockVal);
        byte[] hashVal = HashUtil.makeHash(blockVal, HashUtil.getAlgName(HashUtil.SHA));
        return new R1UploadEvents.BlockResp(event.torrentId, event.fileId, event.nodeId, blockNr, block,
          hashVal, Optional.empty());
      }

      @Override
      public boolean set(R1UploadEvents.BlocksReq request) {
        this.event = request;
        this.aux.addAll(request.blocks);
        return true;
      }
    };
    tc = tc.answerRequest(R1UploadEvents.BlocksReq.class, uploadP, f);
    tc = tc.repeat(nrBlocks).body();
    tc = tc.trigger(f, uploadP);
    tc = tc.end();
    return tc;
  }

  public Msg cacheHintReq(OverlayId torrentId, Identifier fileId, KAddress src, KAddress dst, KHint.Summary cacheHint) {
    return msg(src, dst, new R1TransferMsgs.CacheHintReq(torrentId, fileId, cacheHint));
  }

  public Msg hashReq(OverlayId torrentId, Identifier fileId, KAddress src, KAddress dst, Set<Integer> hashes) {
    return msg(src, dst, new R1TransferMsgs.HashReq(torrentId, fileId, hashes));
  }

  public Msg blockReq(OverlayId torrentId, Identifier fileId, KAddress src, KAddress dst, BlockDetails blockDetails,
    int blockNr) {
    return msg(src, dst, new R1TransferMsgs.BlockReq(torrentId, fileId, blockNr, blockDetails.nrPieces));
  }

  public Msg msg(KAddress src, KAddress dst, KompicsEvent payload) {
    KHeader header = new BasicHeader(src, dst, Transport.UDP);
    return new BasicContentMsg(header, payload);
  }

  public R1UploadTimeout uploadTimeout() {
    SchedulePeriodicTimeout spt = new SchedulePeriodicTimeout(HardCodedConfig.cwndSize, HardCodedConfig.cwndSize);
    return new R1UploadTimeout(spt);
  }
}
