///*
// * Copyright (C) 2009 Swedish Institute of Computer Science (SICS) Copyright (C)
// * 2009 Royal Institute of Technology (KTH)
// *
// * GVoD is free software; you can redistribute it and/or
// * modify it under the terms of the GNU General Public License
// * as published by the Free Software Foundation; either version 2
// * of the License, or (at your option) any later version.
// *
// * This program is distributed in the hope that it will be useful,
// * but WITHOUT ANY WARRANTY; without even the implied warranty of
// * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// * GNU General Public License for more details.
// *
// * You should have received a copy of the GNU General Public License
// * along with this program; if not, write to the Free Software
// * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
// */
//package se.sics.cobweb.integ.spider;
//
//import com.google.common.base.Function;
//import java.util.LinkedList;
//import org.javatuples.Triplet;
//import static org.junit.Assert.assertEquals;
//import org.junit.Before;
//import org.junit.Test;
//import se.sics.cobweb.ReflectPort;
//import se.sics.cobweb.ReflectPortFunc;
//import se.sics.cobweb.conn.CLeecherHandleFSM;
//import se.sics.cobweb.conn.CSeederHandleFSM;
//import se.sics.cobweb.mngr.SpiderComp;
//import se.sics.cobweb.overlord.handle.LeecherHandleOverlordFSM;
//import se.sics.cobweb.overlord.handle.SeederHandleOverlordFSM;
//import se.sics.cobweb.transfer.mngr.TransferMngrFSM;
//import se.sics.cobweb.util.FileId;
//import se.sics.cobweb.util.HandleId;
//import se.sics.fsm.helper.FSMHelper;
//import se.sics.kompics.Component;
//import se.sics.kompics.Direct;
//import se.sics.kompics.KompicsEvent;
//import se.sics.kompics.Negative;
//import se.sics.kompics.testkit.Direction;
//import se.sics.kompics.testkit.TestContext;
//import se.sics.ktoolbox.nutil.fsm.ids.FSMIdRegistry;
//import se.sics.ktoolbox.util.Either;
//import se.sics.ktoolbox.util.identifiable.BasicBuilders;
//import se.sics.ktoolbox.util.identifiable.overlay.OverlayId;
//import se.sics.ktoolbox.util.identifiable.overlay.OverlayIdFactory;
//import se.sics.ktoolbox.util.identifiable.overlay.OverlayRegistry;
//import se.sics.ktoolbox.util.network.KAddress;
//
///**
// * @author Alex Ormenisan <aaor@kth.se>
// */
//public class SpiderLeecherTest {
//  private static final Direction incoming = Direction.INCOMING;
//  private static final Direction outgoing = Direction.OUTGOING;
//
//  private KAddress leecherAdr;
//  private KAddress seederAdr;
//  private OverlayId torrentId;
//  private OverlayId croupierId;
//  private OverlayIdFactory torrentIdFactory;
//  private TestContext<SpiderComp> tc;
//  private Component leecherSpider;
//  private Component driver;
//  private FileId fileId1;
//  private HandleId seederHId1;
//  private HandleId leecherHId1;
//  
//  @Before
//  public void setup() {
//    FSMIdRegistry.reset();
//    OverlayRegistry.reset();
//    String[] fsmNames
//      = new String[]{LeecherHandleOverlordFSM.NAME, SeederHandleOverlordFSM.NAME, CLeecherHandleFSM.NAME,
//        CSeederHandleFSM.NAME, TransferMngrFSM.NAME};
//    torrentIdFactory = FSMHelper.systemSetup("src/test/resources/integ/spider/leecher/application.conf", fsmNames);
//    leecherAdr = FSMHelper.getAddress(0);
//    seederAdr = FSMHelper.getAddress(1);
//    torrentId = torrentIdFactory.id(new BasicBuilders.IntBuilder(1));
//    croupierId = torrentIdFactory.id(new BasicBuilders.IntBuilder(0)).changeType(OverlayId.BasicTypes.CROUPIER);
//    fileId1 = new FileId(torrentId.baseId, 1);
//    seederHId1 = fileId1.seeder(seederAdr.getId());
//    leecherHId1 = fileId1.leecher(seederAdr.getId());
//
//    ReflectPortFunc reflectPort = new ReflectPortFunc() {
//      private Negative<ReflectPort> port;
//
//      @Override
//      public void setPort(Negative<ReflectPort> port) {
//        this.port = port;
//      }
//
//      @Override
//      public Negative<ReflectPort> apply(Boolean f) {
//        return port;
//      }
//    };
//
//    tc = SpiderHelper.getContext(torrentId, leecherAdr);
////    leecherOverlord = tc.getComponentUnderTest();
////    leecherConn = IntegrationHelper.connectConn(tc, leecherOverlord, leecherAdr);
////    seederOverlord = OverlordHelper.connectOverlord(tc, torrentId, seederAdr);
////    seederConn = IntegrationHelper.connectConn(tc, seederOverlord, seederAdr);
////    IntegrationHelper.connectNetwork(tc, leecherConn, seederConn);
////    driver = IntegrationHelper.connectDriver(tc, leecherOverlord, leecherConn, seederOverlord, reflectPort,
////      driverOnStart(), driverScenario());
//  }
//
//  private Triplet driverOnStart() {
//    return null;
//  }
//
//  private LinkedList driverScenario() {
//    LinkedList<Either<Function<Direct.Request, Direct.Response>, Triplet<KompicsEvent, Boolean, Class>>> scenario
//      = new LinkedList();
//    return scenario;
//  }
//
//  @Test
//  public void test() {
//    
//    TestContext<SpiderComp> atc = tc;
////    atc = atc.setTimeout(1000 * 3600);
//    atc = atc.body()
//      .repeat(1).body().end();
//    assertEquals(atc.check(), atc.getFinalState());
//  }
//}
