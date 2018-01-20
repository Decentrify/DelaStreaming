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
import se.sics.silk.SystemHelper;
import se.sics.silk.SystemSetup;
import static se.sics.silk.TorrentTestHelper.eCtrlBaseInfoInd;
import static se.sics.silk.TorrentTestHelper.eCtrlMetaGetSucc;
import static se.sics.silk.TorrentTestHelper.eCtrlStopAck;
import static se.sics.silk.TorrentTestHelper.eHashReq;
import static se.sics.silk.TorrentTestHelper.eMetadataGetReq;
import static se.sics.silk.TorrentTestHelper.eMetadataServeReq;
import static se.sics.silk.TorrentTestHelper.hashStop;
import static se.sics.silk.TorrentTestHelper.hashSucc;
import static se.sics.silk.TorrentTestHelper.metadataGetStop;
import static se.sics.silk.TorrentTestHelper.metadataGetSucc;
import static se.sics.silk.TorrentTestHelper.metadataServeStop;
import static se.sics.silk.TorrentTestHelper.metadataServeSucc;
import static se.sics.silk.TorrentTestHelper.tCtrlDownloadReq;
import static se.sics.silk.TorrentTestHelper.tCtrlMetaGetReq;
import static se.sics.silk.TorrentTestHelper.tCtrlStopReq;
import static se.sics.silk.TorrentTestHelper.tCtrlUploadReq;
import se.sics.silk.TorrentWrapperComp;
import se.sics.silk.r2torrent.R2TorrentComp;
import se.sics.silk.r2torrent.R2TorrentCtrlPort;
import se.sics.silk.SelfPort;
import se.sics.silk.r2torrent.torrent.R2Torrent.States;
import se.sics.silk.r2torrent.util.R2TorrentStatus;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class R2TorrentTest {

  private TestContext<TorrentWrapperComp> tc;
  private Component comp;
  private TorrentWrapperComp compState;
  private Port<R2TorrentCtrlPort> ctrlP;
  private Port<SelfPort> triggerP;
  private Port<SelfPort> expectP;
  private static OverlayIdFactory torrentIdFactory;
  private IntIdFactory intIdFactory;
  private KAddress selfAdr;

  @BeforeClass
  public static void setup() throws FSMException {
    torrentIdFactory = SystemSetup.systemSetup("src/test/resources/application.conf");
  }

  @Before
  public void testSetup() {
    tc = getContext();
    comp = tc.getComponentUnderTest();
    compState = (TorrentWrapperComp) comp.getComponent();
    ctrlP = comp.getPositive(R2TorrentCtrlPort.class);
    triggerP = comp.getNegative(SelfPort.class);
    expectP = comp.getPositive(SelfPort.class);
    intIdFactory = new IntIdFactory(new Random());
  }

  private TestContext<TorrentWrapperComp> getContext() {
    selfAdr = SystemHelper.getAddress(0);
    TorrentWrapperComp.Setup setup = new TorrentWrapperComp.Setup() {
      @Override
      public MultiFSM setupFSM(ComponentProxy proxy, Config config, R2TorrentComp.Ports ports) {
        try {
          R2Torrent.ES fsmEs = new R2Torrent.ES();
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
          return R2Torrent.FSM.multifsm(fsmIdFactory, fsmEs, oexa);
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

  //******************************************************DOWNLOAD******************************************************
  private TestContext metadataGet(TestContext tc) {
    tc = metadataGetSucc(tc, expectP, triggerP); //1-2
    tc = eCtrlMetaGetSucc(tc, ctrlP); //3
    return tc;
  }

  private TestContext metadataServe(TestContext tc) {
    tc = metadataServeSucc(tc, expectP, triggerP); //1-2
    tc = eCtrlBaseInfoInd(tc, ctrlP, R2TorrentStatus.META_SERVE);//3
    return tc;
  }

  private TestContext hash(TestContext tc) {
    tc = hashSucc(tc, expectP, triggerP); //1-2
    tc = eCtrlBaseInfoInd(tc, ctrlP, R2TorrentStatus.HASH);//3
    return tc;
  }
  
  //*************************************************START TO METADATA_GET**********************************************
  @Test
  public void testStartDownloadD() {
    OverlayId torrent = torrentIdFactory.id(new BasicBuilders.IntBuilder(1));
    KAddress seeder = SystemHelper.getAddress(1);
    tc = tc.body();
    tc = startToMetadataGetD(tc, torrent, seeder);
    tc.repeat(1).body().end();
    assertTrue(tc.check());
    Identifier fsmBaseId = R2Torrent.fsmBaseId(torrent);
    assertEquals(States.META_GET, compState.fsm.getFSMState(fsmBaseId));
  }

  private TestContext startToMetadataGetD(TestContext tc, OverlayId torrent, KAddress seeder) {
    tc = tCtrlMetaGetReq(tc, ctrlP, torrent, seeder);
    tc = eMetadataGetReq(tc, expectP);
    return tc;
  }

  //********************************************METADATA_GET TO METADATA_SERVE******************************************
  @Test
  public void testMetadataGetToMetadataServeD() {
    OverlayId torrent = torrentIdFactory.id(new BasicBuilders.IntBuilder(1));
    KAddress seeder = SystemHelper.getAddress(1);
    tc = tc.body();
    tc = startToMetadataServeD(tc, torrent, seeder); //1-5
    tc.repeat(1).body().end();
    assertTrue(tc.check());
    Identifier fsmBaseId = R2Torrent.fsmBaseId(torrent);
    assertEquals(States.META_SERVE, compState.fsm.getFSMState(fsmBaseId));
  }

  private TestContext startToMetadataServeD(TestContext tc, OverlayId torrent, KAddress seeder) {
    tc = tCtrlMetaGetReq(tc, ctrlP, torrent, seeder); //1
    tc = metadataGet(tc); //2-4
    tc = eMetadataServeReq(tc, expectP); //5
    return tc;
  }
  //********************************************METADATA_SERVE TO DATA_STORAGE******************************************

  @Test
  public void testMetadataServeToDataStorageD() {
    OverlayId torrent = torrentIdFactory.id(new BasicBuilders.IntBuilder(1));
    KAddress seeder = SystemHelper.getAddress(1);
    tc = tc.body();
    tc = startToDataStorageD(tc, torrent, seeder); //1-7
    tc.repeat(1).body().end();
    assertTrue(tc.check());
    Identifier fsmBaseId = R2Torrent.fsmBaseId(torrent);
    assertEquals(States.DATA_STORAGE, compState.fsm.getFSMState(fsmBaseId));
  }

  private TestContext startToDataStorageD(TestContext tc, OverlayId torrent, KAddress seeder) {
    tc = tCtrlMetaGetReq(tc, ctrlP, torrent, seeder); //1
    tc = metadataGet(tc); //2-4
    tc = metadataServe(tc); //5-7
    return tc;
  }

  //**************************************************DATA_STORAGE TO HASH**********************************************
  @Test
  public void testDataStorageToHashD() {
    OverlayId torrent = torrentIdFactory.id(new BasicBuilders.IntBuilder(1));
    KAddress seeder = SystemHelper.getAddress(1);
    tc = tc.body();
    tc = startToHashD(tc, torrent, seeder); //1-9
    tc.repeat(1).body().end();
    assertTrue(tc.check());
    Identifier fsmBaseId = R2Torrent.fsmBaseId(torrent);
    assertEquals(States.HASH, compState.fsm.getFSMState(fsmBaseId));
  }

  private TestContext startToHashD(TestContext tc, OverlayId torrent, KAddress seeder) {
    tc = tCtrlMetaGetReq(tc, ctrlP, torrent, seeder); //1
    tc = metadataGet(tc); //2-4
    tc = metadataServe(tc); //5-7
    tc = tCtrlDownloadReq(tc, ctrlP, torrent); //8
    tc = eHashReq(tc, expectP); //9
    return tc;
  }
  //****************************************************HASH TO TRANSFER************************************************

  @Test
  public void testHashToTransferD() {
    OverlayId torrent = torrentIdFactory.id(new BasicBuilders.IntBuilder(1));
    KAddress seeder = SystemHelper.getAddress(1);
    tc = tc.body();
    tc = startToTransferD(tc, torrent, seeder); //1-11
    tc.repeat(1).body().end();
    assertTrue(tc.check());
    Identifier fsmBaseId = R2Torrent.fsmBaseId(torrent);
    assertEquals(States.TRANSFER, compState.fsm.getFSMState(fsmBaseId));
  }

  private TestContext startToTransferD(TestContext tc, OverlayId torrent, KAddress seeder) {
    tc = tCtrlMetaGetReq(tc, ctrlP, torrent, seeder); //1
    tc = metadataGet(tc); //2-4
    tc = metadataServe(tc); //5-7
    tc = tCtrlDownloadReq(tc, ctrlP, torrent); //8
    tc = hash(tc); //9-11
    return tc;
  }

  //************************************************METADATA_GET TO STOP************************************************

  @Test
  public void testMetadataGetToStopD() {
    OverlayId torrent = torrentIdFactory.id(new BasicBuilders.IntBuilder(1));
    KAddress seeder = SystemHelper.getAddress(1);
    tc = tc.body();
    tc = startToMetadataGetD(tc, torrent, seeder); //1-2
    tc = tCtrlStopReq(tc, ctrlP, torrent); //3
    tc = metadataGetStop(tc, expectP, triggerP); //4-5
    tc = eCtrlStopAck(tc, ctrlP); //6
    tc.repeat(1).body().end();
    assertTrue(tc.check());
    Identifier fsmBaseId = R2Torrent.fsmBaseId(torrent);
    assertFalse(compState.fsm.activeFSM(fsmBaseId));
  }

  //************************************************METADATA_SERVE TO STOP**********************************************
  @Test
  public void testMetadataServeToStopD() {
    OverlayId torrent = torrentIdFactory.id(new BasicBuilders.IntBuilder(1));
    KAddress seeder = SystemHelper.getAddress(1);
    tc = tc.body();
    tc = startToMetadataServeD(tc, torrent, seeder); //1-5
    tc = tCtrlStopReq(tc, ctrlP, torrent); //6
    tc = metadataServeStop(tc, expectP, triggerP); //7-8
    tc = eCtrlStopAck(tc, ctrlP); //9
    tc.repeat(1).body().end();
    assertTrue(tc.check());
    Identifier fsmBaseId = R2Torrent.fsmBaseId(torrent);
    assertFalse(compState.fsm.activeFSM(fsmBaseId));
  }

  //*************************************************DATA_STORAGE TO STOP***********************************************
  @Test
  public void testDataStorageToStopD() {
    OverlayId torrent = torrentIdFactory.id(new BasicBuilders.IntBuilder(1));
    KAddress seeder = SystemHelper.getAddress(1);
    tc = tc.body();
    tc = startToDataStorageD(tc, torrent, seeder); //1-7
    tc = tCtrlStopReq(tc, ctrlP, torrent); //8
    tc = metadataServeStop(tc, expectP, triggerP); //9-10
    tc = eCtrlStopAck(tc, ctrlP); //11
    tc.repeat(1).body().end();
    assertTrue(tc.check());
    Identifier fsmBaseId = R2Torrent.fsmBaseId(torrent);
    assertFalse(compState.fsm.activeFSM(fsmBaseId));
  }

  //*******************************************************HASH TO STOP*************************************************

  @Test
  public void testHashToStopD() {
    OverlayId torrent = torrentIdFactory.id(new BasicBuilders.IntBuilder(1));
    KAddress seeder = SystemHelper.getAddress(1);
    tc = tc.body();
    tc = startToHashD(tc, torrent, seeder); //1-9
    tc = tCtrlStopReq(tc, ctrlP, torrent); //10
    tc = hashStop(tc, expectP, triggerP); //11-12
    tc = metadataServeStop(tc, expectP, triggerP); //13-14
    tc = eCtrlStopAck(tc, ctrlP); //15
    tc.repeat(1).body().end();
    assertTrue(tc.check());
    Identifier fsmBaseId = R2Torrent.fsmBaseId(torrent);
    assertFalse(compState.fsm.activeFSM(fsmBaseId));
  }
  //*****************************************************TRANSFER TO STOP***********************************************
  @Test
  public void testTransferToStopD() {
    OverlayId torrent = torrentIdFactory.id(new BasicBuilders.IntBuilder(1));
    KAddress seeder = SystemHelper.getAddress(1);
    tc = tc.body();
    tc = startToTransferD(tc, torrent, seeder); //1-9
    tc = tCtrlStopReq(tc, ctrlP, torrent); //10
    tc = metadataServeStop(tc, expectP, triggerP); //11-12
    tc = eCtrlStopAck(tc, ctrlP); //13
    tc.repeat(1).body().end();
    assertTrue(tc.check());
    Identifier fsmBaseId = R2Torrent.fsmBaseId(torrent);
    assertFalse(compState.fsm.activeFSM(fsmBaseId));
  }
  //***********************************************************UPLOAD***************************************************
  //*************************************************START_METADATA_SERVE***********************************************
  @Test
  public void testStartUploadU() {
    OverlayId torrent = torrentIdFactory.id(new BasicBuilders.IntBuilder(1));
    tc = tc.body();
    tc = startToMetadataServeU(tc, torrent); //1-2
    tc.repeat(1).body().end();
    assertTrue(tc.check());
    Identifier fsmBaseId = R2Torrent.fsmBaseId(torrent);
    assertEquals(States.META_SERVE, compState.fsm.getFSMState(fsmBaseId));
  }

  private TestContext startToMetadataServeU(TestContext tc, OverlayId torrent) {
    tc = tCtrlUploadReq(tc, ctrlP, torrent); //1
    tc = eMetadataServeReq(tc, expectP); //2
    return tc;
  }

  //************************************************METADATA_SERVE TO HASH**********************************************
  @Test
  public void testMetadataServeToHashU() {
    OverlayId torrent = torrentIdFactory.id(new BasicBuilders.IntBuilder(1));
    tc = tc.body();
    tc = startToHashU(tc, torrent); //1-5
    tc.repeat(1).body().end();
    assertTrue(tc.check());
    Identifier fsmBaseId = R2Torrent.fsmBaseId(torrent);
    assertEquals(States.HASH, compState.fsm.getFSMState(fsmBaseId));
  }

  private TestContext startToHashU(TestContext tc, OverlayId torrent) {
    tc = tCtrlUploadReq(tc, ctrlP, torrent); //1
    tc = metadataServe(tc); //2-4
    tc = eHashReq(tc, expectP); //5
    return tc;
  }

  //**************************************************HASH TO TRANSFER**************************************************
  @Test
  public void testHashToTransferU() {
    OverlayId torrent = torrentIdFactory.id(new BasicBuilders.IntBuilder(1));
    tc = tc.body();
    tc = startToTransferU(tc, torrent); //1-7
    tc.repeat(1).body().end();
    assertTrue(tc.check());
    Identifier fsmBaseId = R2Torrent.fsmBaseId(torrent);
    assertEquals(States.TRANSFER, compState.fsm.getFSMState(fsmBaseId));
  }

  private TestContext startToTransferU(TestContext tc, OverlayId torrent) {
    tc = tCtrlUploadReq(tc, ctrlP, torrent); //1
    tc = metadataServe(tc); //2-4
    tc = hash(tc); //5-7
    return tc;
  }
  //************************************************METADATA_SERVE TO STOP**********************************************

  @Test
  public void testMetadataServeToStopU() {
    OverlayId torrent = torrentIdFactory.id(new BasicBuilders.IntBuilder(1));
    tc = tc.body();
    tc = startToMetadataServeU(tc, torrent); //1-2
    tc = tCtrlStopReq(tc, ctrlP, torrent); //3
    tc = metadataServeStop(tc, expectP, triggerP); //4-5
    tc = eCtrlStopAck(tc, ctrlP); //6
    tc.repeat(1).body().end();
    assertTrue(tc.check());
    Identifier fsmBaseId = R2Torrent.fsmBaseId(torrent);
    assertFalse(compState.fsm.activeFSM(fsmBaseId));
  }

  //****************************************************HASH TO STOP****************************************************
  @Test
  public void testHashToStopU() {
    OverlayId torrent = torrentIdFactory.id(new BasicBuilders.IntBuilder(1));
    tc = tc.body();
    tc = startToHashU(tc, torrent); //1-2
    tc = tCtrlStopReq(tc, ctrlP, torrent); //3
    tc = hashStop(tc, expectP, triggerP); //4-5
    tc = metadataServeStop(tc, expectP, triggerP); //5-6
    tc = eCtrlStopAck(tc, ctrlP); //7
    tc.repeat(1).body().end();
    assertTrue(tc.check());
    Identifier fsmBaseId = R2Torrent.fsmBaseId(torrent);
    assertFalse(compState.fsm.activeFSM(fsmBaseId));
  }

  //**************************************************TRANSFER TO STOP**************************************************
  @Test
  public void testTransferToStopU() {
    OverlayId torrent = torrentIdFactory.id(new BasicBuilders.IntBuilder(1));
    tc = tc.body();
    tc = startToTransferU(tc, torrent); //1-7
    tc = tCtrlStopReq(tc, ctrlP, torrent); //8
    tc = metadataServeStop(tc, expectP, triggerP); //9-10
    tc = eCtrlStopAck(tc, ctrlP); //11
    tc.repeat(1).body().end();
    assertTrue(tc.check());
    Identifier fsmBaseId = R2Torrent.fsmBaseId(torrent);
    assertFalse(compState.fsm.activeFSM(fsmBaseId));
  }
}
