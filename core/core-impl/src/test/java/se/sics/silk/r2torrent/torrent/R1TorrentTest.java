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

import java.util.Arrays;
import java.util.HashSet;
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
import se.sics.kompics.testing.Future;
import se.sics.kompics.testing.TestContext;
import se.sics.kompics.util.Identifier;
import se.sics.ktoolbox.util.identifiable.BasicBuilders;
import se.sics.ktoolbox.util.identifiable.basic.IntIdFactory;
import se.sics.ktoolbox.util.identifiable.overlay.OverlayId;
import se.sics.ktoolbox.util.identifiable.overlay.OverlayIdFactory;
import se.sics.ktoolbox.util.managedStore.core.util.HashUtil;
import se.sics.ktoolbox.util.network.KAddress;
import se.sics.nstream.StreamId;
import se.sics.nstream.util.BlockDetails;
import se.sics.silk.FutureHelper.BasicFuture;
import se.sics.silk.SystemHelper;
import se.sics.silk.SystemSetup;
import se.sics.silk.TorrentIdHelper;
import se.sics.silk.TorrentWrapperComp;
import se.sics.silk.r2torrent.R2TorrentComp;
import se.sics.silk.r2torrent.torrent.event.R1FileDownloadEvents;
import se.sics.silk.r2torrent.torrent.event.R1TorrentConnEvents;
import se.sics.silk.r2torrent.torrent.event.R1TorrentCtrlEvents;
import se.sics.silk.r2torrent.torrent.util.R1FileMetadata;
import se.sics.silk.r2torrent.torrent.util.R1TorrentDetails;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class R1TorrentTest {

  private static OverlayIdFactory torrentIdFactory;
  private IntIdFactory intIdFactory;
  
  private TestContext<TorrentWrapperComp> tc;
  private Component comp;
  private TorrentWrapperComp compState;
  private Port<R1TorrentCtrlPort> torrentCtrl;
  private Port<R1TorrentConnPort> torrentConn;
  private Port<R1FileDownloadCtrl> fileDownloadCtrl;
  private Port<R1FileUploadCtrl> fileUploadCtrl;
  private KAddress selfAdr;
  private KAddress seeder1;
  private KAddress seeder2;
  
  private OverlayId torrent;
  private Identifier file1;
  private Identifier file2; 
  private Identifier file3; 
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
    torrentDetails();
    
    tc = getContext();
    comp = tc.getComponentUnderTest();
    compState = (TorrentWrapperComp) comp.getComponent();
    torrentCtrl = comp.getPositive(R1TorrentCtrlPort.class);
    torrentConn = comp.getNegative(R1TorrentConnPort.class);
    fileDownloadCtrl = comp.getNegative(R1FileDownloadCtrl.class);
    fileUploadCtrl = comp.getNegative(R1FileUploadCtrl.class);
  }

  private void torrentDetails() {
    torrent = torrentIdFactory.id(new BasicBuilders.IntBuilder(1));
    torrentDetails = new R1TorrentDetails(HashUtil.getAlgName(HashUtil.SHA));

    Identifier endpointId = intIdFactory.id(new BasicBuilders.IntBuilder(0));
    file1 = intIdFactory.id(new BasicBuilders.IntBuilder(1));
    StreamId file1StreamId = TorrentIdHelper.streamId(endpointId, torrent, file1);
    file2 = intIdFactory.id(new BasicBuilders.IntBuilder(2));
    StreamId file2StreamId = TorrentIdHelper.streamId(endpointId, torrent, file2);
    file3 = intIdFactory.id(new BasicBuilders.IntBuilder(3));
    StreamId file3StreamId = TorrentIdHelper.streamId(endpointId, torrent, file3);

    int pieceSize = 1024;
    int nrPieces = 10;
    int nrBlocks = 5;
    BlockDetails defaultBlock = new BlockDetails(pieceSize * nrPieces, nrPieces, pieceSize, pieceSize);
    R1FileMetadata file1Metadata = R1FileMetadata.instance(pieceSize * nrPieces * nrBlocks, defaultBlock);
    torrentDetails.addMetadata(file1, file1Metadata);
    torrentDetails.addMetadata(file2, file1Metadata);
    torrentDetails.addMetadata(file3, file1Metadata);
    torrentDetails.addStorage(file1, file1StreamId, null);
    torrentDetails.addStorage(file2, file2StreamId, null);
    torrentDetails.addStorage(file3, file3StreamId, null);
  }

  private TestContext<TorrentWrapperComp> getContext() {
    TorrentWrapperComp.Setup setup = new TorrentWrapperComp.Setup() {
      @Override
      public MultiFSM setupFSM(ComponentProxy proxy, Config config, R2TorrentComp.Ports ports) {
        try {
          R1Torrent.ES fsmEs = new R1Torrent.ES(new R1TorrentDetails.Mngr());
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
          return R1Torrent.FSM.multifsm(fsmIdFactory, fsmEs, oexa);
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

  @Test
  public void testUpload() {
    tc = tc.body();
    tc = tc.trigger(upload(torrent, torrentDetails), torrentCtrl);
    tc.repeat(1).body().end();
    assertTrue(tc.check());
    Identifier fsmBaseId = R1Torrent.fsmBaseId(torrent);
    assertEquals(R1Torrent.States.UPLOAD, compState.fsm.getFSMState(fsmBaseId));
  }

  @Test
  public void testDownload() {
    tc = tc.body();
    tc = tc.trigger(download(torrent, torrentDetails), torrentCtrl);
    tc = tc.expect(R1TorrentConnEvents.Bootstrap.class, torrentConn, Direction.OUT);
    tc.repeat(1).body().end();
    assertTrue(tc.check());
    Identifier fsmBaseId = R1Torrent.fsmBaseId(torrent);
    assertEquals(R1Torrent.States.DOWNLOAD, compState.fsm.getFSMState(fsmBaseId));
  }

  @Test
  public void testDownloadWithSeeders() {
    tc = tc.body();
    tc = tc.trigger(download(torrent, torrentDetails), torrentCtrl); //1
    tc = tc.expect(R1TorrentConnEvents.Bootstrap.class, torrentConn, Direction.OUT);///1
    tc = connect2(tc, torrent, file1, file2, seeder1, seeder2);//13
    tc.repeat(1).body().end();
    assertTrue(tc.check());
    Identifier fsmBaseId = R1Torrent.fsmBaseId(torrent);
    assertEquals(R1Torrent.States.UPLOAD, compState.fsm.getFSMState(fsmBaseId));
  }
  
  TestContext connect2(TestContext tc, OverlayId torrent, Identifier file1, Identifier file2, 
    KAddress seeder1, KAddress seeder2) {
    Future f1 = startFileF();
    Future f2 = startFileF();
    Future f3 = startFileF();
    tc = tc.trigger(seeders(torrent, seeder1, seeder2), torrentConn);//1
    tc = tc.answerRequest(R1FileDownloadEvents.Start.class, fileDownloadCtrl, f1);//1
    tc = tc.answerRequest(R1FileDownloadEvents.Start.class, fileDownloadCtrl, f2);//1
    tc = tc.trigger(f1, fileDownloadCtrl);//1
    tc = tc.trigger(f2, fileDownloadCtrl);//1
    tc = tc.expect(R1FileDownloadEvents.Connect.class, fileDownloadCtrl, Direction.OUT);//1
    tc = tc.expect(R1FileDownloadEvents.Connect.class, fileDownloadCtrl, Direction.OUT);//1
    //finish one file and we should use the seeder for a new file
    tc = tc.trigger(completed(torrent, file1), fileDownloadCtrl);//1
    tc = tc.answerRequest(R1FileDownloadEvents.Start.class, fileDownloadCtrl, f3);//1
    tc = tc.trigger(f3, fileDownloadCtrl);//1
    tc = tc.expect(R1FileDownloadEvents.Connect.class, fileDownloadCtrl, Direction.OUT);//1
    tc = tc.trigger(completed(torrent, file2), fileDownloadCtrl);//1
    tc = tc.trigger(completed(torrent, file3), fileDownloadCtrl);//1
    return tc;
  }
  
  public Future<R1FileDownloadEvents.Start, R1FileDownloadEvents.Indication> startFileF() {
    return new BasicFuture<R1FileDownloadEvents.Start, R1FileDownloadEvents.Indication>() {

      @Override
      public R1FileDownloadEvents.Indication get() {
        return new R1FileDownloadEvents.Indication(event.torrentId, event.fileId, R1FileDownload.States.IDLE);
      }
    };
  }

  public R1TorrentConnEvents.Seeders seeders(OverlayId torrentId, KAddress... seeders) {
    return new R1TorrentConnEvents.Seeders(torrentId, new HashSet<>(Arrays.asList(seeders)));
  }
  
  public R1TorrentCtrlEvents.Upload upload(OverlayId torrentId, R1TorrentDetails torrentDetails) {
    return new R1TorrentCtrlEvents.Upload(torrentId, torrentDetails);
  }

  public R1TorrentCtrlEvents.Download download(OverlayId torrentId, R1TorrentDetails torrentDetails, 
    KAddress... seeders) {
    return new R1TorrentCtrlEvents.Download(torrentId, torrentDetails, new HashSet<>(Arrays.asList(seeders)));
  }
  
  public R1FileDownloadEvents.Indication completed(OverlayId torrentId, Identifier fileId) {
    return new R1FileDownloadEvents.Indication(torrentId, fileId, R1FileDownload.States.COMPLETED);
  }
}
