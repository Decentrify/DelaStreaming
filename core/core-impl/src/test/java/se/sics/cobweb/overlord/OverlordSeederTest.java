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
package se.sics.cobweb.overlord;

import com.google.common.base.Function;
import java.util.LinkedList;
import org.javatuples.Triplet;
import static org.junit.Assert.assertEquals;
import org.junit.Before;
import org.junit.Test;
import se.sics.cobweb.ReflectPort;
import se.sics.cobweb.ReflectPortFunc;
import se.sics.cobweb.TestHelper;
import se.sics.cobweb.conn.ConnPort;
import se.sics.cobweb.conn.event.ConnE;
import se.sics.cobweb.overlord.handle.LeecherHandleOverlordFSM;
import se.sics.cobweb.overlord.handle.SeederHandleOverlordFSM;
import se.sics.cobweb.overlord.handle.SeederHandleOverlordStates;
import se.sics.cobweb.ports.ConnPortHelper;
import se.sics.cobweb.ports.HandleMngrPortHelper;
import se.sics.cobweb.transfer.handlemngr.HandleMngrPort;
import se.sics.cobweb.transfer.handlemngr.event.HandleMngrE;
import se.sics.cobweb.transfer.instance.TransferPort;
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

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class OverlordSeederTest {

  private static final Direction incoming = Direction.INCOMING;
  private static final Direction outgoing = Direction.OUTGOING;

  private KAddress leecherAdr;
  private KAddress seederAdr;
  private OverlayId torrentId1;
  private static OverlayIdFactory torrentIdFactory;
  private TestContext<OverlordComp> tc;
  private Component seederOverlord;
  private Component driver;
  private FileId fileId1;
  private HandleId leecherHId1;

  @Before
  public void setup() {
    FSMIdRegistry.reset();
    OverlayRegistry.reset();
    String[] fsmNames = new String[]{LeecherHandleOverlordFSM.NAME, SeederHandleOverlordFSM.NAME};
    torrentIdFactory = FSMHelper.systemSetup("src/test/resources/overlord/seeder/application.conf", fsmNames);
    leecherAdr = FSMHelper.getAddress(0);
    seederAdr = FSMHelper.getAddress(1);
    torrentId1 = torrentIdFactory.id(new BasicBuilders.IntBuilder(1));
    fileId1 = new FileId(torrentId1.baseId, 1);
    leecherHId1 = fileId1.leecher(leecherAdr.getId());

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

    tc = OverlordHelper.getContext(torrentId1, seederAdr);
    seederOverlord = tc.getComponentUnderTest();
    driver = OverlordHelper.connectDriver(tc, seederOverlord, reflectPort, driverOnStart(), driverScenario());
  }

  private Triplet driverOnStart() {
    return ConnPortHelper.leecherConnect(torrentId1, leecherHId1, leecherAdr);
  }

  private LinkedList driverScenario() {
    LinkedList<Either<Function<Direct.Request, Direct.Response>, Triplet<KompicsEvent, Boolean, Class>>> scenario
      = new LinkedList();
    scenario.add((Either) Either.left(HandleMngrPortHelper.seederConnected()));
    return scenario;
  }

  @Test
  public void test() {
    Port<ConnPort> conn = seederOverlord.getNegative(ConnPort.class);
    Port<HandleMngrPort> handleMngr = seederOverlord.getNegative(HandleMngrPort.class);
    Port<TransferPort> transfer = seederOverlord.getNegative(TransferPort.class);

    TestContext<OverlordComp> atc = tc;
//    atc = atc.setTimeout(1000 * 3600);
    atc = atc.body()
      .expect(ConnE.Connect2Request.class, FSMHelper.anyEvent(ConnE.Connect2Request.class), conn, outgoing)
      .expect(HandleMngrE.SeederConnect.class, TestHelper.anyAndNext(driver, HandleMngrE.SeederConnect.class), handleMngr, outgoing)
      .inspect(OverlordHelper.inspectSeederHandleState(leecherHId1, SeederHandleOverlordStates.SETUP_HANDLE))
      .inspect(OverlordHelper.inspectSeederHandleSize(1))
      .repeat(1).body().end();
    assertEquals(atc.check(), atc.getFinalState());
  }

}
