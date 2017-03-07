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
package se.sics.cobweb.integ.overlord;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import java.util.LinkedList;
import org.javatuples.Triplet;
import static org.junit.Assert.assertEquals;
import org.junit.Before;
import org.junit.Test;
import se.sics.cobweb.DriverHelper;
import se.sics.cobweb.MyTorrentHelper;
import se.sics.cobweb.ReflectPort;
import se.sics.cobweb.ReflectPortFunc;
import se.sics.cobweb.TestHelper;
import se.sics.cobweb.conn.CLeecherHandleFSM;
import se.sics.cobweb.conn.CSeederHandleFSM;
import se.sics.cobweb.conn.ConnPort;
import se.sics.cobweb.conn.event.ConnE;
import se.sics.cobweb.overlord.OverlordComp;
import se.sics.cobweb.overlord.OverlordHelper;
import se.sics.cobweb.overlord.handle.LeecherHandleOverlordFSM;
import se.sics.cobweb.overlord.handle.LeecherHandleOverlordStates;
import se.sics.cobweb.overlord.handle.SeederHandleOverlordFSM;
import se.sics.cobweb.ports.ConnPortHelper;
import se.sics.cobweb.ports.CroupierPortHelper;
import se.sics.cobweb.ports.SpiderCtrlPortHelper;
import se.sics.cobweb.ports.TransferMngrPortHelper;
import se.sics.cobweb.transfer.TransferLeecherFSM;
import se.sics.cobweb.transfer.TransferSeederFSM;
import se.sics.cobweb.transfer.handlemngr.HandleMngrPort;
import se.sics.cobweb.transfer.handlemngr.event.HandleMngrE;
import se.sics.cobweb.transfer.instance.TransferPort;
import se.sics.cobweb.transfer.mngr.TransferMngrFSM;
import se.sics.cobweb.transfer.mngr.event.TransferE;
import se.sics.cobweb.util.FileId;
import se.sics.cobweb.util.HandleId;
import se.sics.fsm.helper.FSMHelper;
import se.sics.kompics.Component;
import se.sics.kompics.Direct;
import se.sics.kompics.KompicsEvent;
import se.sics.kompics.Negative;
import se.sics.kompics.Port;
import se.sics.kompics.testkit.Direction;
import se.sics.kompics.testkit.TestContext;
import se.sics.ktoolbox.nutil.fsm.ids.FSMIdRegistry;
import se.sics.ktoolbox.util.Either;
import se.sics.ktoolbox.util.identifiable.BasicBuilders;
import se.sics.ktoolbox.util.identifiable.overlay.OverlayId;
import se.sics.ktoolbox.util.identifiable.overlay.OverlayIdFactory;
import se.sics.ktoolbox.util.identifiable.overlay.OverlayRegistry;
import se.sics.ktoolbox.util.network.KAddress;
import se.sics.nstream.transfer.MyTorrent;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class OverlordLeecherTest {

  private static final Direction incoming = Direction.INCOMING;
  private static final Direction outgoing = Direction.OUTGOING;

  private KAddress leecherAdr;
  private KAddress seederAdr;
  private OverlayId torrentId;
  private OverlayId croupierId;
  private OverlayIdFactory torrentIdFactory;
  private TestContext<OverlordComp> tc;
  private Component leecherOverlord;
  private Component leecherSpider;
  private Component seederSpider;
  private Component leecherDriver;
  private Component seederDriver;
  private FileId fileId1;
  private HandleId seederHId1;
  private HandleId leecherHId1;
  private MyTorrent.Manifest torrent;

  @Before
  public void setup() {
    FSMIdRegistry.reset();
    OverlayRegistry.reset();
    String[] fsmNames
      = new String[]{LeecherHandleOverlordFSM.NAME, SeederHandleOverlordFSM.NAME, CLeecherHandleFSM.NAME,
        CSeederHandleFSM.NAME, TransferMngrFSM.NAME, TransferSeederFSM.NAME, TransferLeecherFSM.NAME};
    torrentIdFactory = FSMHelper.systemSetup("src/test/resources/integ/overlord/leecher/application.conf", fsmNames);
    leecherAdr = FSMHelper.getAddress(0);
    seederAdr = FSMHelper.getAddress(1);
    torrentId = torrentIdFactory.id(new BasicBuilders.IntBuilder(1));
    croupierId = torrentIdFactory.id(new BasicBuilders.IntBuilder(0)).changeType(OverlayId.BasicTypes.CROUPIER);
    fileId1 = new FileId(torrentId.baseId, 0);
    seederHId1 = fileId1.seeder(seederAdr.getId());
    leecherHId1 = fileId1.leecher(seederAdr.getId());
    torrent = MyTorrentHelper.torrent("src/test/resources/integ/overlord/leecher/manifest.json");
    
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

    tc = OverlordHelper.getContext(torrentId, leecherAdr);
    leecherOverlord = tc.getComponentUnderTest();
    leecherSpider = IntegrationHelper.connectParentSpider(tc, leecherAdr);
    seederSpider = IntegrationHelper.connectSpider(tc, seederAdr);
    IntegrationHelper.connectNetwork(tc, leecherSpider, seederSpider);
    leecherDriver = IntegrationHelper.connectDriver(tc, leecherSpider, reflectPort, leecherOnStart(), leecherScenario());
    seederDriver = IntegrationHelper.connectDriver(tc, seederSpider, reflectPort, seederOnStart(), seederScenario());
  }

  private Triplet seederOnStart() {
    return SpiderCtrlPortHelper.setup();
  }
  
  private LinkedList seederScenario() {
    LinkedList<Either<Function<Direct.Request, Direct.Response>, Triplet<KompicsEvent, Boolean, Class>>> scenario
      = new LinkedList();
    scenario.add((Either)Either.right(TransferMngrPortHelper.setupTorrent(torrentId, Optional.of(torrent))));
    scenario.add((Either)Either.right(TransferMngrPortHelper.startTorrent(torrentId)));
    scenario.add((Either)Either.right(DriverHelper.switchDrivers(leecherDriver)));
    return scenario;
  }
  
  private Triplet leecherOnStart() {
    return null;
  }

  private LinkedList leecherScenario() {
    LinkedList<Either<Function<Direct.Request, Direct.Response>, Triplet<KompicsEvent, Boolean, Class>>> scenario
      = new LinkedList();
    scenario.add((Either) Either.right(SpiderCtrlPortHelper.setup()));
    scenario.add((Either) Either.right(TransferMngrPortHelper.setupTorrent(torrentId, Optional.absent())));
    scenario.add((Either) Either.right(TransferMngrPortHelper.startTorrent(torrentId)));
    scenario.add((Either) Either.right(CroupierPortHelper.seederSample(croupierId, torrentId, seederAdr)));
    return scenario;
  }

  @Test
  public void test() {
    Port<ConnPort> conn = leecherOverlord.getNegative(ConnPort.class);
    Port<HandleMngrPort> handleMngr = leecherOverlord.getNegative(HandleMngrPort.class);
    Port<TransferPort> transfer = leecherOverlord.getNegative(TransferPort.class);
    TestContext<OverlordComp> atc = tc;
    atc = atc.setTimeout(1000 * 3600);
    atc = atc.body()
      .expect(TransferE.LeecherStarted.class, TestHelper.anyAndNext(leecherDriver, TransferE.LeecherStarted.class), transfer, incoming)
      .expect(ConnE.SeederSample.class, FSMHelper.anyEvent(ConnE.SeederSample.class), conn, incoming)
      .expect(ConnE.Connect1Request.class, ConnPortHelper.connectToSeeder(seederHId1), conn, outgoing)
//      .expect(ConnE.Connect1Accept.class, ConnPortHelper.connectedSeeder(seederHId1), conn, incoming) answer not intercepted
      .expect(HandleMngrE.LeecherConnect.class, FSMHelper.anyEvent(HandleMngrE.LeecherConnect.class), handleMngr, outgoing)
      .inspect(OverlordHelper.inspectLeecherHandleState(seederHId1, LeecherHandleOverlordStates.SETUP_HANDLE))
      .repeat(1).body().end();
    assertEquals(atc.check(), atc.getFinalState());
  }
}
