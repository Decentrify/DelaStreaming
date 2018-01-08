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
package se.sics.silk.r2transfer;

import com.google.common.base.Predicate;
import java.util.LinkedList;
import java.util.List;
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
import se.sics.kompics.testing.TestContext;
import se.sics.kompics.util.Identifier;
import se.sics.ktoolbox.util.identifiable.BasicBuilders;
import se.sics.ktoolbox.util.identifiable.overlay.OverlayId;
import se.sics.ktoolbox.util.identifiable.overlay.OverlayIdFactory;
import se.sics.ktoolbox.util.network.KAddress;
import se.sics.silk.SystemHelper;
import se.sics.silk.SystemSetup;
import se.sics.silk.r2torrent.R2TorrentTransferPort;
import se.sics.silk.r2torrent.event.R2TorrentTransferEvents;
import se.sics.silk.r2transfer.R1Metadata.States;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class R1MetadataTest {
  private TestContext<R2TransferComp> tc;
  private Component r2TransferComp;
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
    r2TransferComp = tc.getComponentUnderTest();
    transferP = r2TransferComp.getPositive(R2TorrentTransferPort.class);
  }

  private TestContext<R2TransferComp> getContext() {
    selfAdr = SystemHelper.getAddress(0);
    R2TransferComp.Init init = new R2TransferComp.Init(selfAdr);
    TestContext<R2TransferComp> context = TestContext.newInstance(R2TransferComp.class, init);
    return context;
  }

  @After
  public void clean() {
  }

//  @Test
  public void testEmpty() {
    tc = tc.body();
    tc.repeat(1).body().end();

    assertTrue(tc.check());
  }
  
  @Test
  public void testWrongOnStart() {
    tc = tc.body();
    tc.repeat(1).body().end();
    assertTrue(tc.check());
  }
  
  @Test
  public void testGet() {
    OverlayId torrent1 = torrentIdFactory.id(new BasicBuilders.IntBuilder(1));
    KAddress seeder = SystemHelper.getAddress(1);
    
    tc = tc.body();
    tc = tc.inspect(inactiveFSM(torrent1.baseId)); // 1
    tc = transferGetSucc(tc, seeder, torrent1); // 3-5
    tc = transferStop(tc, torrent1); // 5-7
    tc.repeat(1).body().end();
    assertTrue(tc.check());
  }
  
  @Test
  public void testServe() {
    OverlayId torrent1 = torrentIdFactory.id(new BasicBuilders.IntBuilder(1));
    
    tc = tc.body();
    tc = tc.inspect(inactiveFSM(torrent1.baseId)); // 1
    tc = transferServeSucc(tc, torrent1); // 3-5
    tc = transferStop(tc, torrent1); // 5-7
    tc.repeat(1).body().end();
    assertTrue(tc.check());
  }
  
  @Test
  public void testStopOnStart() {
    OverlayId torrent1 = torrentIdFactory.id(new BasicBuilders.IntBuilder(1));
    
    tc = tc.body();
    tc = tc.inspect(inactiveFSM(torrent1.baseId)); // 1
    tc = transferStop(tc, torrent1); // 3-5
    tc.repeat(1).body().end();
    assertTrue(tc.check());
  }
  
  @Test
  public void testStopOnServe() {
    OverlayId torrent1 = torrentIdFactory.id(new BasicBuilders.IntBuilder(1));
    
    tc = tc.body();
    tc = tc.inspect(inactiveFSM(torrent1.baseId)); // 1
    tc = transferServeSucc(tc, torrent1); //3-5
    tc = transferStop(tc, torrent1); // 5-7
    tc.repeat(1).body().end();
    assertTrue(tc.check());
  }
  
  private TestContext transferGetSucc(TestContext tc, KAddress seeder, OverlayId torrentId) {

    return tc
      .trigger(transferGet(torrentId, seeder), transferP)
      .expect(R2TorrentTransferEvents.MetaGetSucc.class, transferP, Direction.OUT)
      .inspect(state(torrentId.baseId, States.SERVE));
  }
  
  private TestContext transferServeSucc(TestContext tc, OverlayId torrentId) {

    return tc
      .trigger(transferServe(torrentId), transferP)
      .expect(R2TorrentTransferEvents.MetaServeSucc.class, transferP, Direction.OUT)
      .inspect(state(torrentId.baseId, States.SERVE));
  }
  
  private TestContext transferStop(TestContext tc, OverlayId torrentId) {

    return tc
      .trigger(transferStop(torrentId), transferP)
      .expect(R2TorrentTransferEvents.MetaStopAck.class, transferP, Direction.OUT)
      .inspect(inactiveFSM(torrentId.baseId));
  }
  
  Predicate<R2TransferComp> inactiveFSM(Identifier torrentBaseId) {
    return (R2TransferComp t) -> {
      return !t.activeMetadataFSM(torrentBaseId);
    };
  }

  Predicate<R2TransferComp> state(Identifier torrentBaseId, FSMStateName expectedState) {
    return (R2TransferComp t) -> {
      FSMStateName currentState = t.getMetadataState(torrentBaseId);
      return currentState.equals(expectedState);
    };
  }
  
  private R2TorrentTransferEvents.MetaGetReq transferGet(OverlayId torrent, KAddress partner) {
    List<KAddress> partners = new LinkedList<>();
    partners.add(partner);
    return new R2TorrentTransferEvents.MetaGetReq(torrent, partners);
  }
  
  private R2TorrentTransferEvents.MetaServeReq transferServe(OverlayId torrent) {
    return new R2TorrentTransferEvents.MetaServeReq(torrent);
  }
  
  private R2TorrentTransferEvents.MetaStop transferStop(OverlayId torrent) {
    return new R2TorrentTransferEvents.MetaStop(torrent);
  }
}
