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
import se.sics.kompics.testing.Future;
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
import se.sics.nstream.util.BlockDetails;
import se.sics.silk.FutureHelper;
import se.sics.silk.SelfPort;
import se.sics.silk.SystemHelper;
import se.sics.silk.SystemSetup;
import se.sics.silk.TorrentIdHelper;
import se.sics.silk.TorrentTestHelper;
import static se.sics.silk.TorrentTestHelper.storageStreamConnected;
import static se.sics.silk.TorrentTestHelper.storageStreamDisconnected;
import se.sics.silk.TorrentWrapperComp;
import se.sics.silk.r2torrent.R2TorrentComp;
import se.sics.silk.r2torrent.torrent.R1FileUpload.R1FileMngr;
import se.sics.silk.r2torrent.torrent.R1FileUpload.States;
import se.sics.silk.r2torrent.torrent.event.R1FileUploadEvents;
import se.sics.silk.r2torrent.transfer.events.R1TransferLeecherEvents;
import se.sics.silk.r2torrent.transfer.events.R1UploadTimeout;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class R1FileUploadTest {

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
  private KAddress leecher1;
  private KAddress leecher2;
  private KAddress leecher3;
  private R1FileMngr fileMngr;
  private BlockDetails defaultBlock;

  @BeforeClass
  public static void setup() throws FSMException {
    torrentIdFactory = SystemSetup.systemSetup("src/test/resources/application.conf");
  }

  @Before
  public void testSetup() {
    intIdFactory = new IntIdFactory(new Random());
    selfAdr = SystemHelper.getAddress(0);
    leecher1 = SystemHelper.getAddress(1);
    leecher2 = SystemHelper.getAddress(2);
    leecher3 = SystemHelper.getAddress(3);
    endpointId = intIdFactory.id(new BasicBuilders.IntBuilder(0));
    torrent = torrentIdFactory.id(new BasicBuilders.IntBuilder(1));
    file = intIdFactory.id(new BasicBuilders.IntBuilder(1));
    streamId = TorrentIdHelper.streamId(endpointId, torrent, file);
    defaultBlock = new BlockDetails(1024 * 100 + 10, 100, 1024, 10);
    fileMngr = new R1FileMngr(endpointId, torrent);

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
          R1FileUpload.ES fsmEs = new R1FileUpload.ES(selfAdr, fileMngr, defaultBlock);
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
          return R1FileUpload.FSM.multifsm(fsmIdFactory, fsmEs, oexa);
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
  public void testStartToStoragePending1() {
    tc = tc.body();
    tc = startToStoragePending1(tc, torrent, file, leecher1);//1-2
    tc.repeat(1).body().end();
    assertTrue(tc.check());
    Identifier fsmBaseId = R1FileUpload.fsmBaseId(torrent, file);
    assertEquals(States.STORAGE_PENDING, compState.fsm.getFSMState(fsmBaseId));
  }

  private TestContext startToStoragePending1(TestContext tc, OverlayId torrent, Identifier file, KAddress leecher) {
    tc = tc.trigger(connect(torrent, file, leecher), triggerP); //1
    tc = tc.expect(DStreamConnect.Request.class, streamCtrlP, Direction.OUT); //2
    return tc;
  }

  @Test
  public void testStartToStoragePending2() {
    tc = tc.body();
    tc = startToStoragePending2(tc, torrent, file, leecher1); //1-3
    tc.repeat(1).body().end();
    assertTrue(tc.check());
    Identifier fsmBaseId = R1FileUpload.fsmBaseId(torrent, file);
    assertEquals(States.STORAGE_PENDING, compState.fsm.getFSMState(fsmBaseId));
  }

  private TestContext startToStoragePending2(TestContext tc, OverlayId torrent, Identifier file, KAddress leecher) {
    tc = tc.trigger(connect(torrent, file, leecher), triggerP); //1
    tc = tc.expect(DStreamConnect.Request.class, streamCtrlP, Direction.OUT); //2
    tc = tc.trigger(connect(torrent, file, leecher2), triggerP); //3
    return tc;
  }

  //****************************************************START TO ACTIVE*************************************************
  @Test
  public void testStartToStorageSucc1() {
    tc = tc.body();
    tc = startToStorageSucc1(tc, torrent, file, leecher1);//1-5
    tc.repeat(1).body().end();
    assertTrue(tc.check());
    Identifier fsmBaseId = R1FileUpload.fsmBaseId(torrent, file);
    assertEquals(States.ACTIVE, compState.fsm.getFSMState(fsmBaseId));
  }

  private TestContext startToStorageSucc1(TestContext tc, OverlayId torrent, Identifier file, KAddress leecher) {
    tc = tc.trigger(connect(torrent, file, leecher), triggerP); //1
    tc = storageStreamConnected(tc, streamCtrlP, streamCtrlP);//2-3
    tc = eConnectSucc(tc);
    tc = eFileIndication(tc, States.ACTIVE); //4
    return tc;
  }

  @Test
  public void testStartToStorageSucc2() {
    tc = tc.body();
    tc = startToStorageSucc2(tc, torrent, file, leecher1, leecher2);//1-6
    tc.repeat(1).body().end();
    assertTrue(tc.check());
    Identifier fsmBaseId = R1FileUpload.fsmBaseId(torrent, file);
    assertEquals(States.ACTIVE, compState.fsm.getFSMState(fsmBaseId));
  }

  private TestContext startToStorageSucc2(TestContext tc, OverlayId torrent, Identifier file,
    KAddress leecher1, KAddress leecher2) {
    Future f = new FutureHelper.BasicFuture<DStreamConnect.Request, DStreamConnect.Success>() {

      @Override
      public DStreamConnect.Success get() {
        return event.success(0);
      }
    };
    tc = tc.trigger(connect(torrent, file, leecher1), triggerP); //1
    tc = tc.answerRequest(DStreamConnect.Request.class, streamCtrlP, f); //2
    tc = tc.trigger(connect(torrent, file, leecher2), triggerP); //3
    tc = tc.trigger(f, streamCtrlP); //4
    tc = tc.repeat(2).body();
    tc = eConnectSucc(tc); //5-6
    tc = tc.end();
    tc = eFileIndication(tc, States.ACTIVE); //7
    return tc;
  }

  //*******************************************************DISCONNECT***************************************************
  @Test
  public void testTransferDisconnected1() {
    tc = tc.body();
    tc = tc.trigger(connect(torrent, file, leecher1), triggerP); //1
    tc = storageStreamConnected(tc, streamCtrlP, streamCtrlP);//2-3
    tc = eConnectSucc(tc); //4-5
    tc = eFileIndication(tc, States.ACTIVE); //6
    tc = connect(tc, torrent, file, leecher2); //7-9
    tc = disconnected(tc, torrent, file, leecher2); //10-11
    tc.repeat(1).body().end();
    assertTrue(tc.check());
    Identifier fsmBaseId = R1FileUpload.fsmBaseId(torrent, file);
    assertEquals(States.ACTIVE, compState.fsm.getFSMState(fsmBaseId));
  }
  
  @Test
  public void testTransferDisconnected2() {
    tc = tc.body();
    tc = tc.trigger(connect(torrent, file, leecher1), triggerP); //1
    tc = storageStreamConnected(tc, streamCtrlP, streamCtrlP);//2-3
    tc = eConnectSucc(tc); //4-5
    tc = eFileIndication(tc, States.ACTIVE); //6
    tc = connect(tc, torrent, file, leecher2); //7-9
    tc = disconnected(tc, torrent, file, leecher2); //10-11
    tc = disconnected(tc, torrent, file, leecher1); //12-13
    tc = storageStreamDisconnected(tc, streamCtrlP, streamCtrlP); //14-15
    tc.repeat(1).body().end();
    assertTrue(tc.check());
    Identifier fsmBaseId = R1FileUpload.fsmBaseId(torrent, file);
    assertFalse(compState.fsm.activeFSM(fsmBaseId));
  }

  public TestContext connect(TestContext tc, OverlayId torrentId, Identifier fileId, KAddress leecher) {
    tc = tc.trigger(connect(torrent, file, leecher), triggerP); //1
    tc = eConnectSucc(tc); //2-3
    return tc;
  }
  
   public TestContext eConnectSucc(TestContext tc) {
    tc = TorrentTestHelper.eSchedulePeriodicTimer(tc, R1UploadTimeout.class, timerP); //1
    tc = tc.expect(R1TransferLeecherEvents.ConnectAcc.class, expectP, Direction.OUT); //2
    return tc;
  }

  public TestContext disconnected(TestContext tc, OverlayId torrentId, Identifier fileId, KAddress leecher) {
    tc = tc.trigger(disconnected(torrent, file, leecher), triggerP);//1
    tc = TorrentTestHelper.eCancelPeriodicTimer(tc, timerP); //2
    return tc;
  }

  public TestContext eFileIndication(TestContext tc, States state) {
    Predicate p = fileIndication(state);
    return tc.expect(R1FileUploadEvents.Indication.class, p, expectP, Direction.OUT);
  }

  public Predicate<R1FileUploadEvents.Indication> fileIndication(States state) {
    return (R1FileUploadEvents.Indication t) -> {
      boolean res = t.state.equals(state);
      return res;
    };
  }

  public R1TransferLeecherEvents.ConnectReq connect(OverlayId torrentId, Identifier fileId, KAddress leecher) {
    return new R1TransferLeecherEvents.ConnectReq(torrentId, fileId, leecher);
  }

  public R1TransferLeecherEvents.Disconnected disconnected(OverlayId torrentId, Identifier fileId, KAddress leecher) {
    return new R1TransferLeecherEvents.Disconnected(torrentId, fileId, leecher.getId());
  }
}