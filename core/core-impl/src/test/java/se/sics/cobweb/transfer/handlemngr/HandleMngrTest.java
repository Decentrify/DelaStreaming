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
package se.sics.cobweb.transfer.handlemngr;

import com.google.common.base.Function;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Set;
import org.javatuples.Triplet;
import static org.junit.Assert.assertEquals;
import org.junit.Before;
import org.junit.Test;
import se.sics.cobweb.ReflectPort;
import se.sics.cobweb.ReflectPortFunc;
import se.sics.cobweb.ports.HandleMngrPortHelper;
import se.sics.cobweb.ports.LeecherHandlePortHelper;
import se.sics.cobweb.ports.NetworkPortHelper;
import se.sics.cobweb.transfer.handle.LeecherHandlePort;
import se.sics.cobweb.transfer.handle.event.LeecherHandleE;
import se.sics.cobweb.transfer.handlemngr.event.HandleMngrE;
import se.sics.cobweb.util.FileId;
import se.sics.cobweb.util.HandleId;
import se.sics.fsm.helper.FSMHelper;
import se.sics.kompics.Component;
import se.sics.kompics.Direct;
import se.sics.kompics.KompicsEvent;
import se.sics.kompics.Negative;
import se.sics.kompics.Port;
import se.sics.kompics.network.Network;
import se.sics.kompics.testkit.Direction;
import se.sics.kompics.testkit.TestContext;
import se.sics.ktoolbox.nutil.fsm.ids.FSMIdRegistry;
import se.sics.ktoolbox.util.Either;
import se.sics.ktoolbox.util.identifiable.BasicBuilders;
import se.sics.ktoolbox.util.identifiable.overlay.OverlayId;
import se.sics.ktoolbox.util.identifiable.overlay.OverlayIdFactory;
import se.sics.ktoolbox.util.identifiable.overlay.OverlayRegistry;
import se.sics.ktoolbox.util.network.KAddress;
import se.sics.ktoolbox.util.network.basic.BasicContentMsg;
import se.sics.nstream.util.BlockDetails;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class HandleMngrTest {

  private static final Direction incoming = Direction.INCOMING;
  private static final Direction outgoing = Direction.OUTGOING;

  private KAddress selfAdr;
  private KAddress peerAdr1;
  private KAddress peerAdr2;
  private OverlayId torrentId;
  private static OverlayIdFactory torrentIdFactory;
  private TestContext<HandleMngrComp> tc;
  private Component handleMngr;
  private Component driver;
  private final static int nrPings = 4;
  private FileId fileId1;
  private FileId fileId2;
  private HandleId leecherHId1;
  private HandleId leecherHId2;
  private HandleId leecherHId3;
  private HandleId seederHId1;
  private HandleId seederHId2;
  private HandleId seederHId3;
  private BlockDetails defaultBlock = new BlockDetails(1024*1024, 1024, 1024, 1024);
  private boolean withHashes = false;

  @Before
  public void setup() {
    FSMIdRegistry.reset();
    OverlayRegistry.reset();
    String[] fsmNames = new String[]{};
    torrentIdFactory = FSMHelper.systemSetup("src/test/resources/conn/mngr/application.conf", fsmNames);
    selfAdr = FSMHelper.getAddress(0);
    peerAdr1 = FSMHelper.getAddress(1);
    peerAdr2 = FSMHelper.getAddress(2);
    torrentId = torrentIdFactory.id(new BasicBuilders.IntBuilder(0));
    fileId1 = new FileId(torrentId.baseId, 1);
    fileId2 = new FileId(torrentId.baseId, 2);
    leecherHId1 = fileId1.leecher(peerAdr1.getId());
    leecherHId2 = fileId2.leecher(peerAdr1.getId());
    leecherHId3 = fileId1.leecher(peerAdr2.getId());
    seederHId1 = fileId1.seeder(peerAdr1.getId());
    seederHId2 = fileId2.seeder(peerAdr1.getId());
    seederHId3 = fileId1.seeder(peerAdr2.getId());
    
    ReflectPortFunc reflectPort = new ReflectPortFunc() {
      private Negative<ReflectPort> port;
      
      @Override
      public void setPort(Negative<ReflectPort> port) {
        this.port = port;
      }
      
      @Override
      public Negative<ReflectPort> apply(Boolean f) {
        return port;
      }
    };
    LeecherHandleCreator leecher = HandleMngrHelper.getMockLeecherHandle(reflectPort, leecherReflectEvents());
    SeederHandleCreator seeder = HandleMngrHelper.getMockSeederHandle(reflectPort, seederReflectEvents());

    tc = HandleMngrHelper.getContext(selfAdr, leecher, seeder);
    handleMngr = tc.getComponentUnderTest();
    driver = HandleMngrHelper.connectDriver(tc, handleMngr, reflectPort, driverOnStart(), driverScenario());
    HandleMngrHelper.setNetwork(tc, handleMngr, driver);
  }

  private Triplet driverOnStart() {
    return HandleMngrPortHelper.leecherConnect(torrentId, leecherHId1, peerAdr1);
  }

  private LinkedList driverScenario() {
    LinkedList<Either<Function<Direct.Request, Direct.Response>, Triplet<KompicsEvent, Boolean, Class>>> scenario
      = new LinkedList();
    scenario.add((Either)Either.right(HandleMngrPortHelper.seederConnect(torrentId, seederHId1, peerAdr1)));
    scenario.add((Either)Either.right(HandleMngrPortHelper.leecherConnect(torrentId, leecherHId2, peerAdr1)));
    scenario.add((Either)Either.right(HandleMngrPortHelper.seederConnect(torrentId, seederHId2, peerAdr1)));
    scenario.add((Either)Either.right(HandleMngrPortHelper.leecherConnect(torrentId, leecherHId3, peerAdr2)));
    scenario.add((Either)Either.right(HandleMngrPortHelper.seederConnect(torrentId, seederHId3, peerAdr2)));
    scenario.add((Either)Either.right(LeecherHandlePortHelper.download(torrentId, leecherHId1)));
    scenario.add((Either)Either.right(NetworkPortHelper.mockMsg(torrentId, leecherHId1, peerAdr1, selfAdr)));
    scenario.add((Either)Either.right(NetworkPortHelper.mockMsg(torrentId, seederHId1, peerAdr1, selfAdr)));
    scenario.add((Either)Either.right(HandleMngrPortHelper.leecherDisconnect(torrentId, leecherHId1)));
    scenario.add((Either)Either.right(HandleMngrPortHelper.leecherDisconnect(torrentId, leecherHId2)));
    scenario.add((Either)Either.right(HandleMngrPortHelper.leecherDisconnect(torrentId, leecherHId3)));
    scenario.add((Either)Either.right(HandleMngrPortHelper.seederDisconnect(torrentId, seederHId1)));
    scenario.add((Either)Either.right(HandleMngrPortHelper.seederDisconnect(torrentId, seederHId2)));
    scenario.add((Either)Either.right(HandleMngrPortHelper.seederDisconnect(torrentId, seederHId3)));
    return scenario;
  }
  
  private Set leecherReflectEvents() {
    Set<Class> reflect = new HashSet<>();
    reflect.add(LeecherHandleE.Download.class);
    reflect.add(BasicContentMsg.class);
    return reflect;
  }
  
  private Set seederReflectEvents() {
    Set<Class> reflect = new HashSet<>();
    reflect.add(BasicContentMsg.class);
    return reflect;
  }

  @Test
  public void test() {
    Port<HandleMngrPort> handle = handleMngr.getPositive(HandleMngrPort.class);
    Port<LeecherHandlePort> leecherH = handleMngr.getPositive(LeecherHandlePort.class);
    Port<Network> network = handleMngr.getNegative(Network.class);

    TestContext<HandleMngrComp> atc = tc;
    atc = atc.setTimeout(1000*3600);
    atc = atc.body()
      .expect(HandleMngrE.LeecherConnect.class, FSMHelper.anyEvent(HandleMngrE.LeecherConnect.class), handle, incoming)
      .inspect(HandleMngrHelper.inspectState(1))
      .expect(HandleMngrE.SeederConnect.class, FSMHelper.anyEvent(HandleMngrE.SeederConnect.class), handle, incoming)
      .inspect(HandleMngrHelper.inspectState(2))
      .expect(HandleMngrE.LeecherConnect.class, FSMHelper.anyEvent(HandleMngrE.LeecherConnect.class), handle, incoming)
      .inspect(HandleMngrHelper.inspectState(3))
      .expect(HandleMngrE.SeederConnect.class, FSMHelper.anyEvent(HandleMngrE.SeederConnect.class), handle, incoming)
      .inspect(HandleMngrHelper.inspectState(4))
      .expect(HandleMngrE.LeecherConnect.class, FSMHelper.anyEvent(HandleMngrE.LeecherConnect.class), handle, incoming)
      .inspect(HandleMngrHelper.inspectState(5))
      .expect(HandleMngrE.SeederConnect.class, FSMHelper.anyEvent(HandleMngrE.SeederConnect.class), handle, incoming)
      .inspect(HandleMngrHelper.inspectState(6))
      .expect(LeecherHandleE.Download.class, FSMHelper.anyEvent(LeecherHandleE.Download.class), leecherH, incoming)
      .expect(BasicContentMsg.class, FSMHelper.anyMsg(MockHandleEvent.class), network, incoming)
      .expect(BasicContentMsg.class, FSMHelper.anyMsg(MockHandleEvent.class), network, incoming)
      .inspect(HandleMngrHelper.inspectState(6))
      .expect(HandleMngrE.LeecherDisconnect.class, FSMHelper.anyEvent(HandleMngrE.LeecherDisconnect.class), handle, incoming)
      .inspect(HandleMngrHelper.inspectState(5))
      .expect(HandleMngrE.LeecherDisconnect.class, FSMHelper.anyEvent(HandleMngrE.LeecherDisconnect.class), handle, incoming)
      .inspect(HandleMngrHelper.inspectState(4))
      .expect(HandleMngrE.LeecherDisconnect.class, FSMHelper.anyEvent(HandleMngrE.LeecherDisconnect.class), handle, incoming)
      .inspect(HandleMngrHelper.inspectState(3))
      .expect(HandleMngrE.SeederDisconnect.class, FSMHelper.anyEvent(HandleMngrE.SeederDisconnect.class), handle, incoming)
      .inspect(HandleMngrHelper.inspectState(2))
      .expect(HandleMngrE.SeederDisconnect.class, FSMHelper.anyEvent(HandleMngrE.SeederDisconnect.class), handle, incoming)
      .inspect(HandleMngrHelper.inspectState(1))
      .expect(HandleMngrE.SeederDisconnect.class, FSMHelper.anyEvent(HandleMngrE.SeederDisconnect.class), handle, incoming)
      .inspect(HandleMngrHelper.inspectState(0))
      .repeat(1).body().end();
    assertEquals(atc.check(), atc.getFinalState());
  }
}
