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
package se.sics.silk.r2torrent;

import com.google.common.base.Predicate;
import java.util.LinkedList;
import java.util.List;
import org.junit.After;
import static org.junit.Assert.assertTrue;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import se.sics.kompics.Component;
import se.sics.kompics.LoopbackPort;
import se.sics.kompics.Port;
import se.sics.kompics.fsm.FSMException;
import se.sics.kompics.fsm.FSMStateName;
import se.sics.kompics.testing.Direction;
import se.sics.kompics.testing.Future;
import se.sics.kompics.testing.TestContext;
import se.sics.kompics.util.Identifier;
import se.sics.ktoolbox.util.identifiable.BasicBuilders;
import se.sics.ktoolbox.util.identifiable.overlay.OverlayId;
import se.sics.ktoolbox.util.identifiable.overlay.OverlayIdFactory;
import se.sics.ktoolbox.util.network.KAddress;
import se.sics.silk.SystemHelper;
import se.sics.silk.SystemSetup;
import se.sics.silk.r2torrent.R2Torrent.States;
import se.sics.silk.r2torrent.event.R1HashEvents;
import se.sics.silk.r2torrent.event.R1MetadataEvents;
import se.sics.silk.r2torrent.event.R2TorrentCtrlEvents;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class R2TorrentTestOld {

  private TestContext<R2TorrentComp> tc;
  private Component r2MngrComp;
  private Port<R2TorrentCtrlPort> ctrlP;
  private Port<LoopbackPort> loopbackP;
  private static OverlayIdFactory torrentIdFactory;
  private KAddress selfAdr;

  @BeforeClass
  public static void setup() throws FSMException {
    torrentIdFactory = SystemSetup.systemSetup("src/test/resources/application.conf");
  }

  @Before
  public void testSetup() {
    tc = getContext();
    r2MngrComp = tc.getComponentUnderTest();
    ctrlP = r2MngrComp.getPositive(R2TorrentCtrlPort.class);
    loopbackP = r2MngrComp.getNegative(LoopbackPort.class);
  }

  private TestContext<R2TorrentComp> getContext() {
    selfAdr = SystemHelper.getAddress(0);
    R2TorrentComp.Init init = new R2TorrentComp.Init(selfAdr);
    TestContext<R2TorrentComp> context = TestContext.newInstance(R2TorrentComp.class, init);
    return context;
  }

  @After
  public void clean() {
  }

  @Test
  public void testDownload() {
    OverlayId torrent1 = torrentIdFactory.id(new BasicBuilders.IntBuilder(1));
    KAddress seeder = SystemHelper.getAddress(1);
    tc = tc.body();
    tc = tc.inspect(inactiveFSM(torrent1)); //1
    tc = tc.trigger(ctrlMetaGetReq(seeder, torrent1), ctrlP); //2
    tc = transferMetaGetSucc(tc, torrent1); //3-7
    tc = ctrlDownload(tc, torrent1); //8-10
    tc = transferHashSucc(tc, torrent1); //11-13
    tc = tc.expect(R2TorrentCtrlEvents.TorrentBaseInfo.class, ctrlP, Direction.OUT);//14
    tc = tc.inspect(state(torrent1.baseId, States.TRANSFER)); //15
    tc = compStop2(tc, torrent1); //16-26
    tc.repeat(1).body().end();
    assertTrue(tc.check());
  }

  @Test
  public void testUpload() {
    OverlayId torrent1 = torrentIdFactory.id(new BasicBuilders.IntBuilder(1));

    tc = tc.body();
    tc = tc.inspect(inactiveFSM(torrent1)); //1
    tc = tc.trigger(ctrlUpload(torrent1), ctrlP); //2
    tc = transferMetaServeSucc(tc, torrent1); //3-7
    tc = tc.expect(R2TorrentCtrlEvents.TorrentBaseInfo.class, ctrlP, Direction.OUT);//8
    tc = transferHashSucc(tc, torrent1); //9-11
    tc = tc.expect(R2TorrentCtrlEvents.TorrentBaseInfo.class, ctrlP, Direction.OUT); //12
    tc = tc.inspect(state(torrent1.baseId, States.UPLOAD)); //13
    tc = compStop2(tc, torrent1); //14-24
    tc.repeat(1).body().end();
    assertTrue(tc.check());
  }

//  @Test
  public void testFailMeta() {
    OverlayId torrent1 = torrentIdFactory.id(new BasicBuilders.IntBuilder(1));
    KAddress seeder = SystemHelper.getAddress(1);
    
    tc = tc.body();
    tc = tc.inspect(inactiveFSM(torrent1)); //1
    tc = tc.trigger(ctrlMetaGetReq(seeder, torrent1), ctrlP); //2
    tc = transferMetaGetFail(tc, torrent1); //3-6
    tc = tc.expect(R2TorrentCtrlEvents.MetaGetFail.class, ctrlP, Direction.OUT);//7
    tc = tc.inspect(inactiveFSM(torrent1)); //8
    tc.repeat(1).body().end();
    assertTrue(tc.check());
  }

//  @Test
  public void testFailHash() {
    OverlayId torrent1 = torrentIdFactory.id(new BasicBuilders.IntBuilder(1));
    KAddress seeder = SystemHelper.getAddress(1);
    
    tc = tc.body();
    tc = tc.inspect(inactiveFSM(torrent1)); //1
    tc = tc.trigger(ctrlMetaGetReq(seeder, torrent1), ctrlP); //2
    tc = transferMetaGetSucc(tc, torrent1); //3-7
    tc = ctrlDownload(tc, torrent1); //8-10
    tc = transferHashFail(tc, torrent1); //11-15
    tc = transferMetaClean(tc, torrent1); //16-19
    tc = tc.inspect(inactiveFSM(torrent1)); //20
    tc.repeat(1).body().end();
    assertTrue(tc.check());
  }

//  @Test
  public void testStopMeta() {
    OverlayId torrent1 = torrentIdFactory.id(new BasicBuilders.IntBuilder(1));
    KAddress seeder = SystemHelper.getAddress(1);
    
    tc = tc.body();
    tc = compMetaStop(tc, seeder, torrent1); //1-11
    tc.repeat(1).body().end();
    assertTrue(tc.check());
  }

//  @Test
  public void testStopInDataStorage() {
    OverlayId torrent1 = torrentIdFactory.id(new BasicBuilders.IntBuilder(1));
    KAddress seeder = SystemHelper.getAddress(1);
    
    tc = tc.body();
    tc = tc.inspect(inactiveFSM(torrent1)); //1
    tc = tc.trigger(ctrlMetaGetReq(seeder, torrent1), ctrlP); //2
    tc = transferMetaGetSucc(tc, torrent1); //3-7
    tc = tc.expect(R2TorrentCtrlEvents.MetaGetSucc.class, ctrlP, Direction.OUT);//8
    tc = tc.inspect(state(torrent1.baseId, States.DATA_STORAGE));
    tc = compStop2(tc, torrent1);
    tc.repeat(1).body().end();
    assertTrue(tc.check());
  }

//  @Test
  public void testStopInHash() {
    OverlayId torrent1 = torrentIdFactory.id(new BasicBuilders.IntBuilder(1));
    KAddress seeder = SystemHelper.getAddress(1);
    
    tc = tc.body();
    tc = tc.inspect(inactiveFSM(torrent1)); //1
    tc = tc.trigger(ctrlMetaGetReq(seeder, torrent1), ctrlP); //2
    tc = transferMetaGetSucc(tc, torrent1); //3-7
    tc = ctrlDownload(tc, torrent1); //8-10
    tc = tc
      .inspect(state(torrent1.baseId, States.HASH))
      .expect(R1HashEvents.HashReq.class, loopbackP, Direction.OUT);
    tc = compStop2(tc, torrent1);
    tc.repeat(1).body().end();
    assertTrue(tc.check());
  }

//  @Test
  public void testStopInTransfer() {
    OverlayId torrent1 = torrentIdFactory.id(new BasicBuilders.IntBuilder(1));
    KAddress seeder = SystemHelper.getAddress(1);
    
    tc = tc.body();
    tc = tc.inspect(inactiveFSM(torrent1)); //1
    tc = tc.trigger(ctrlMetaGetReq(seeder, torrent1), ctrlP); //2
    tc = transferMetaGetSucc(tc, torrent1); //3-7
    tc = ctrlDownload(tc, torrent1); //8-10
    tc = transferHashSucc(tc, torrent1); //11-14
    tc = tc.expect(R2TorrentCtrlEvents.TorrentBaseInfo.class, ctrlP, Direction.OUT); //15
    tc = compStop2(tc, torrent1);
    tc.repeat(1).body().end();
    assertTrue(tc.check());
  }

  @Test
  public void testSimple() {
    tc = tc.body();
    tc.repeat(1).body().end();
    assertTrue(tc.check());
  }

  private TestContext compStop2(TestContext tc, OverlayId torrentId) {
    tc = tc
      .trigger(controlStop(torrentId), ctrlP); //1
    tc = transferHashClean(tc, torrentId); //2-5
    tc = transferMetaClean(tc, torrentId); //6-9
    tc = tc
      .expect(R2TorrentCtrlEvents.StopAck.class, ctrlP, Direction.OUT) //10
      .inspect(inactiveFSM(torrentId.baseId)); //11
    return tc;
  }

  private TestContext compMetaStop(TestContext tc, KAddress seeder, OverlayId torrentId) {
    tc = tc
      .inspect(inactiveFSM(torrentId.baseId))
      .trigger(ctrlMetaGetReq(seeder, torrentId), ctrlP)
      .inspect(state(torrentId.baseId, States.META_GET))
      .expect(R1MetadataEvents.MetaGetReq.class, loopbackP, Direction.OUT)
      .trigger(controlStop(torrentId), ctrlP);
    tc = transferMetaClean(tc, torrentId);
    tc = tc.expect(R2TorrentCtrlEvents.StopAck.class, ctrlP, Direction.OUT)
      .inspect(inactiveFSM(torrentId.baseId));
    return tc;
  }

  private TestContext transferMetaGetFail(TestContext tc, OverlayId torrentId) {
    Future f = R1MetadataHelper.transferMetaGetFail();
    return tc
      .answerRequest(R1MetadataEvents.MetaGetReq.class, loopbackP, f)
      .inspect(state(torrentId.baseId, States.META_GET))
      .trigger(f, loopbackP);
  }
  
  private TestContext transferMetaGetSucc(TestContext tc, OverlayId torrentId) {
    Future f = R1MetadataHelper.transferMetaGetSucc();
    return tc
      .answerRequest(R1MetadataEvents.MetaGetReq.class, loopbackP, f)
      .inspect(state(torrentId.baseId, States.META_GET))
      .trigger(f, loopbackP)
      .inspect(state(torrentId.baseId, States.DATA_STORAGE));
  }
  
  private TestContext transferMetaServeSucc(TestContext tc, OverlayId torrentId) {
    Future f = R1MetadataHelper.transferMetaServeSucc();
    return tc
      .answerRequest(R1MetadataEvents.MetaServeReq.class, loopbackP, f)
      .inspect(state(torrentId.baseId, States.META_SERVE))
      .trigger(f, loopbackP)
      .inspect(state(torrentId.baseId, States.HASH));
  }
  
  private TestContext transferMetaClean(TestContext tc, OverlayId torrentId) {
    Future f = R1MetadataHelper.transferMetaStop();
    return tc
      .inspect(state(torrentId.baseId, States.CLEAN_META))
      .answerRequest(R1MetadataEvents.MetaStop.class, loopbackP, f)
      .trigger(f, loopbackP)
      .inspect(inactiveFSM(torrentId.baseId));
  }

  private TestContext transferHashSucc(TestContext tc, OverlayId torrentId) {
    Future f = R1HashHelper.transferHashSucc();
    return tc
      .inspect(state(torrentId.baseId, States.HASH))
      .answerRequest(R1HashEvents.HashReq.class, loopbackP, f)
      .trigger(f, loopbackP);
  }

  private TestContext transferHashFail(TestContext tc, OverlayId torrentId) {
    Future f = R1HashHelper.transferHashFail();
    return tc
      .inspect(state(torrentId.baseId, States.HASH))
      .answerRequest(R1HashEvents.HashReq.class, loopbackP, f)
      .trigger(f, loopbackP)
      .inspect(state(torrentId.baseId, States.CLEAN_META))
      .expect(R2TorrentCtrlEvents.TorrentBaseInfo.class, ctrlP, Direction.OUT);
  }

  private TestContext transferHashClean(TestContext tc, OverlayId torrentId) {
    Future f = R1HashHelper.transferHashClean();
    return tc
      .inspect(state(torrentId.baseId, States.CLEAN_HASH))
      .answerRequest(R1HashEvents.HashStop.class, loopbackP, f)
      .trigger(f, loopbackP)
      .inspect(state(torrentId.baseId, States.CLEAN_META));
  }

  private TestContext ctrlDownload(TestContext tc, OverlayId torrentId) {
    return tc
      .expect(R2TorrentCtrlEvents.MetaGetSucc.class, ctrlP, Direction.OUT)
      .inspect(state(torrentId.baseId, States.DATA_STORAGE))
      .trigger(ctrlDownload(torrentId), ctrlP)
      .inspect(state(torrentId.baseId, States.HASH));
  }

  private TestContext ctrlTo(TestContext tc, R2Torrent.CtrlEvent event, FSMStateName to) {
    return tc
      .trigger(event, ctrlP)
      .inspect(state(event.getR2TorrentFSMId(), to));
  }

  Predicate<R2TorrentComp> inactiveFSM(Identifier baseTorrentId) {
    return (R2TorrentComp t) -> !t.activeTorrentFSM(baseTorrentId);
  }

  Predicate<R2TorrentComp> state(Identifier baseTorrentId, FSMStateName expectedState) {
    return (R2TorrentComp t) -> {
      FSMStateName currentState = t.getTorrentState(baseTorrentId);
      return currentState.equals(expectedState);
    };
  }

  private R2TorrentCtrlEvents.MetaGetReq ctrlMetaGetReq(KAddress partner, OverlayId torrentId) {
    List<KAddress> partners = new LinkedList<>();
    partners.add(partner);
    return new R2TorrentCtrlEvents.MetaGetReq(torrentId, partners);
  }

  private R2TorrentCtrlEvents.Download ctrlDownload(OverlayId torrentId) {
    return new R2TorrentCtrlEvents.Download(torrentId);
  }

  private R2TorrentCtrlEvents.Upload ctrlUpload(OverlayId torrentId) {
    return new R2TorrentCtrlEvents.Upload(torrentId);
  }

  private R2TorrentCtrlEvents.Stop controlStop(OverlayId torrentId) {
    return new R2TorrentCtrlEvents.Stop(torrentId);
  }
}
