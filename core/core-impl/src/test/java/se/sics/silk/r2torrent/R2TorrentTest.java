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
import org.junit.After;
import static org.junit.Assert.assertTrue;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import se.sics.kompics.Component;
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
import se.sics.silk.r2torrent.event.R2TorrentCtrlEvents;
import se.sics.silk.r2torrent.event.R2TorrentTransferEvents;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class R2TorrentTest {

  private TestContext<R2TorrentComp> tc;
  private Component r2MngrComp;
  private Port<R2TorrentCtrlPort> ctrlP;
  private Port<R2TorrentTransferPort> transferP;
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
    transferP = r2MngrComp.getNegative(R2TorrentTransferPort.class);
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
  public void testDownload1() {
    OverlayId torrent1 = torrentIdFactory.id(new BasicBuilders.IntBuilder(1));

    tc = tc.body();
    tc = compMetaGetSucc(tc, torrent1); //1-7
    tc = ctrlDataStorage(tc, torrent1); //8-10
    tc = transferHashSucc(tc, torrent1); //11-14
    tc = tc.inspect(state(torrent1.baseId, States.TRANSFER)); //15
    tc = compStop2(tc, torrent1); //16-26
    tc.repeat(1).body().end();
    assertTrue(tc.check());
  }
  
  @Test
  public void testFailMeta() {
    OverlayId torrent1 = torrentIdFactory.id(new BasicBuilders.IntBuilder(1));

    tc = tc.body();
    tc = compMetaGetSucc(tc, torrent1); //1-7
    tc.repeat(1).body().end();
    assertTrue(tc.check());
  }
  
  @Test
  public void testFailHash() {
    OverlayId torrent1 = torrentIdFactory.id(new BasicBuilders.IntBuilder(1));

    tc = tc.body();
    tc = compMetaGetSucc(tc, torrent1); //1-7
    tc = ctrlDataStorage(tc, torrent1); //8-10
    tc = transferHashFail(tc, torrent1); //11-15
    tc = transferMetaClean(tc, torrent1); //16-19
    tc = tc.inspect(inactiveFSM(torrent1)); //20
    tc.repeat(1).body().end(); 
    assertTrue(tc.check());
  }
  
  @Test
  public void testStopMeta() {
    OverlayId torrent1 = torrentIdFactory.id(new BasicBuilders.IntBuilder(1));

    tc = tc.body();
    tc = compMetaStop(tc, torrent1); //1-11
    tc.repeat(1).body().end();
    assertTrue(tc.check());
  }
  
  @Test
  public void testStopInDataStorage() {
    OverlayId torrent1 = torrentIdFactory.id(new BasicBuilders.IntBuilder(1));

    tc = tc.body();
    tc = compMetaGetSucc(tc, torrent1); //1-7
    tc = tc.inspect(state(torrent1.baseId, States.DATA_STORAGE));
    tc = compStop2(tc, torrent1);
    tc.repeat(1).body().end();
    assertTrue(tc.check());
  }
  
  @Test
  public void testStopInHash() {
    OverlayId torrent1 = torrentIdFactory.id(new BasicBuilders.IntBuilder(1));

    tc = tc.body();
    tc = compMetaGetSucc(tc, torrent1); //1-7
    tc = ctrlDataStorage(tc, torrent1); //8-10
    tc = tc
      .inspect(state(torrent1.baseId, States.HASH))
      .expect(R2TorrentTransferEvents.HashReq.class, transferP, Direction.OUT);
    tc = compStop2(tc, torrent1);
    tc.repeat(1).body().end();
    assertTrue(tc.check());
  }
  
  @Test
  public void testStopInTransfer() {
    OverlayId torrent1 = torrentIdFactory.id(new BasicBuilders.IntBuilder(1));

    tc = tc.body();
    tc = compMetaGetSucc(tc, torrent1); //1-7
    tc = ctrlDataStorage(tc, torrent1); //8-10
    tc = transferHashSucc(tc, torrent1); //11-15
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
    tc =  tc
      .trigger(controlStop(torrentId), ctrlP); //1
    tc = transferHashClean(tc, torrentId); //2-5
    tc = transferMetaClean(tc, torrentId); //6-9
    tc = tc
      .expect(R2TorrentCtrlEvents.StopAck.class, ctrlP, Direction.OUT) //10
      .inspect(inactiveFSM(torrentId.baseId)); //11
    return tc;
  }
  
  private TestContext compMetaStop(TestContext tc, OverlayId torrentId) {
    tc =  tc
      .inspect(inactiveFSM(torrentId.baseId)) 
      .trigger(ctrlMetaGetReq(torrentId), ctrlP)
      .inspect(state(torrentId.baseId, States.META_GET))
      .expect(R2TorrentTransferEvents.MetaGetReq.class, transferP, Direction.OUT)
      .trigger(controlStop(torrentId), ctrlP);
    tc = transferMetaClean(tc, torrentId);
    tc = tc.expect(R2TorrentCtrlEvents.StopAck.class, ctrlP, Direction.OUT)
      .inspect(inactiveFSM(torrentId.baseId));
    return tc;
  }
  
  private TestContext compMetaGetSucc(TestContext tc, OverlayId torrentId) {
    Future f = transferMetaGetSucc(torrentId);
    return tc
      .inspect(inactiveFSM(torrentId.baseId)) 
      .trigger(ctrlMetaGetReq(torrentId), ctrlP)
      .inspect(state(torrentId.baseId, States.META_GET))
      .answerRequest(R2TorrentTransferEvents.MetaGetReq.class, transferP, f)
      .trigger(f, transferP)
      .expect(R2TorrentCtrlEvents.MetaGetSucc.class, ctrlP, Direction.OUT)
      .inspect(state(torrentId.baseId, States.DATA_STORAGE));
  }
  
  private TestContext compMetaGetFail(TestContext tc, OverlayId torrentId) {
    Future f = transferMetaGetFail(torrentId);
    return tc
      .inspect(inactiveFSM(torrentId.baseId)) 
      .trigger(ctrlMetaGetReq(torrentId), ctrlP)
      .inspect(state(torrentId.baseId, States.META_GET))
      .answerRequest(R2TorrentTransferEvents.MetaGetReq.class, transferP, f)
      .trigger(f, transferP)
      .expect(R2TorrentCtrlEvents.MetaGetFail.class, ctrlP, Direction.OUT)
      .inspect(inactiveFSM(torrentId.baseId));
  }

  private TestContext transferMetaClean(TestContext tc, OverlayId torrentId) {
    Future f = transferMetaStop(torrentId);
    return tc
      .inspect(state(torrentId.baseId, States.CLEAN_META))
      .answerRequest(R2TorrentTransferEvents.MetaStop.class, transferP, f)
      .trigger(f, transferP)
      .inspect(inactiveFSM(torrentId.baseId));
  }
  
  private TestContext transferHashSucc(TestContext tc, OverlayId torrentId) {
    Future f = transferHashSucc(torrentId);
    return tc
      .inspect(state(torrentId.baseId, States.HASH))
      .answerRequest(R2TorrentTransferEvents.HashReq.class, transferP, f)
      .trigger(f, transferP)
      .inspect(state(torrentId.baseId, States.TRANSFER));
  }
  
  private TestContext transferHashFail(TestContext tc, OverlayId torrentId) {
    Future f = transferHashFail(torrentId);
    return tc
      .inspect(state(torrentId.baseId, States.HASH))
      .answerRequest(R2TorrentTransferEvents.HashReq.class, transferP, f)
      .trigger(f, transferP)
      .inspect(state(torrentId.baseId, States.CLEAN_META))
      .expect(R2TorrentCtrlEvents.TorrentBaseInfo.class, ctrlP, Direction.OUT);
  }
  
  private TestContext transferHashClean(TestContext tc, OverlayId torrentId) {
    Future f = transferHashClean(torrentId);
    return tc
      .inspect(state(torrentId.baseId, States.CLEAN_HASH))
      .answerRequest(R2TorrentTransferEvents.HashStop.class, transferP, f)
      .trigger(f, transferP)
      .inspect(state(torrentId.baseId, States.CLEAN_META));
  }
  
  private TestContext ctrlDataStorage(TestContext tc, OverlayId torrentId) {
    return tc
      .inspect(state(torrentId.baseId, States.DATA_STORAGE))
      .trigger(ctrlDataStorage(torrentId), ctrlP)
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
  
  Future<R2TorrentTransferEvents.MetaGetReq, R2TorrentTransferEvents.MetaGetSucc> transferMetaGetSucc(
    OverlayId torrentId) {
    return new Future<R2TorrentTransferEvents.MetaGetReq, R2TorrentTransferEvents.MetaGetSucc>() {
      R2TorrentTransferEvents.MetaGetReq request;

      @Override
      public boolean set(R2TorrentTransferEvents.MetaGetReq request) {
        if (request.torrentId.equals(torrentId)) {
          this.request = request;
          return true;
        }
        return false;
      }

      @Override
      public R2TorrentTransferEvents.MetaGetSucc get() {
        return request.success();
      }
    };
  }
  
  Future<R2TorrentTransferEvents.MetaGetReq, R2TorrentTransferEvents.MetaGetFail> transferMetaGetFail(
    OverlayId torrentId) {
    return new Future<R2TorrentTransferEvents.MetaGetReq, R2TorrentTransferEvents.MetaGetFail>() {
      R2TorrentTransferEvents.MetaGetReq request;

      @Override
      public boolean set(R2TorrentTransferEvents.MetaGetReq request) {
        if (request.torrentId.equals(torrentId)) {
          this.request = request;
          return true;
        }
        return false;
      }

      @Override
      public R2TorrentTransferEvents.MetaGetFail get() {
        return request.fail();
      }
    };
  }
  
  Future<R2TorrentTransferEvents.MetaStop, R2TorrentTransferEvents.MetaStopAck> transferMetaStop(
    OverlayId torrentId) {
    return new Future<R2TorrentTransferEvents.MetaStop, R2TorrentTransferEvents.MetaStopAck>() {
      R2TorrentTransferEvents.MetaStop request;

      @Override
      public boolean set(R2TorrentTransferEvents.MetaStop request) {
        if (request.torrentId.equals(torrentId)) {
          this.request = request;
          return true;
        }
        return false;
      }

      @Override
      public R2TorrentTransferEvents.MetaStopAck get() {
        return request.ack();
      }
    };
  }
  
  Future<R2TorrentTransferEvents.HashReq, R2TorrentTransferEvents.HashSucc> transferHashSucc(
    OverlayId torrentId) {
    return new Future<R2TorrentTransferEvents.HashReq, R2TorrentTransferEvents.HashSucc>() {
      R2TorrentTransferEvents.HashReq request;

      @Override
      public boolean set(R2TorrentTransferEvents.HashReq request) {
        if (request.torrentId.equals(torrentId)) {
          this.request = request;
          return true;
        }
        return false;
      }

      @Override
      public R2TorrentTransferEvents.HashSucc get() {
        return request.success();
      }
    };
  }
  
  Future<R2TorrentTransferEvents.HashReq, R2TorrentTransferEvents.HashFail> transferHashFail(
    OverlayId torrentId) {
    return new Future<R2TorrentTransferEvents.HashReq, R2TorrentTransferEvents.HashFail>() {
      R2TorrentTransferEvents.HashReq request;

      @Override
      public boolean set(R2TorrentTransferEvents.HashReq request) {
        if (request.torrentId.equals(torrentId)) {
          this.request = request;
          return true;
        }
        return false;
      }

      @Override
      public R2TorrentTransferEvents.HashFail get() {
        return request.fail();
      }
    };
  }
  
  Future<R2TorrentTransferEvents.HashStop, R2TorrentTransferEvents.HashStopAck> transferHashClean(
    OverlayId torrentId) {
    return new Future<R2TorrentTransferEvents.HashStop, R2TorrentTransferEvents.HashStopAck>() {
      R2TorrentTransferEvents.HashStop request;

      @Override
      public boolean set(R2TorrentTransferEvents.HashStop request) {
        if (request.torrentId.equals(torrentId)) {
          this.request = request;
          return true;
        }
        return false;
      }

      @Override
      public R2TorrentTransferEvents.HashStopAck get() {
        return request.ack();
      }
    };
  }
  
  private R2TorrentCtrlEvents.MetaGetReq ctrlMetaGetReq(OverlayId torrentId) {
    return new R2TorrentCtrlEvents.MetaGetReq(torrentId);
  }
  
  private R2TorrentCtrlEvents.DataStorage ctrlDataStorage(OverlayId torrentId) {
    return new R2TorrentCtrlEvents.DataStorage(torrentId);
  }
  
  private R2TorrentCtrlEvents.Stop controlStop(OverlayId torrentId) {
    return new R2TorrentCtrlEvents.Stop(torrentId);
  }

  private R2TorrentTransferEvents.MetaGetReq transferMetaGetReq(OverlayId torrentId) {
    return new R2TorrentTransferEvents.MetaGetReq(torrentId);
  }
}
