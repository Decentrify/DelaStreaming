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
//package se.sics.cobweb.conn2;
//
//import se.sics.cobweb.conn2.ConnComp;
//import se.sics.cobweb.conn2.ConnId;
//import se.sics.cobweb.conn2.ConnPort;
//import com.google.common.base.Function;
//import com.google.common.base.Predicate;
//import java.util.HashMap;
//import java.util.HashSet;
//import java.util.LinkedList;
//import java.util.List;
//import java.util.Set;
//import org.javatuples.Pair;
//import org.javatuples.Triplet;
//import se.sics.cobweb.MockNetwork;
//import se.sics.cobweb.MockScenarioComp;
//import se.sics.cobweb.conn2.instance.LeecherConnFSM;
//import se.sics.cobweb.conn2.instance.LeecherConnStates;
//import se.sics.cobweb.conn2.instance.MockCroupierComp;
//import se.sics.cobweb.conn2.instance.SeederConnFSM;
//import se.sics.cobweb.conn2.instance.SeederConnStates;
//import se.sics.cobweb.conn2.instance.api.ConnectionDecider;
//import se.sics.cobweb.conn2.instance.event.ConnE;
//import se.sics.kompics.Component;
//import se.sics.kompics.Init;
//import se.sics.kompics.network.Network;
//import se.sics.kompics.testkit.TestContext;
//import se.sics.kompics.testkit.Testkit;
//import se.sics.ktoolbox.croupier.CroupierPort;
//import se.sics.ktoolbox.nutil.fsm.MultiFSM;
//import se.sics.ktoolbox.util.identifiable.overlay.OverlayId;
//import se.sics.ktoolbox.util.network.KAddress;
//
///**
// * @author Alex Ormenisan <aaor@kth.se>
// */
//public class ConnHelper {
//
//  public static TestContext<ConnComp> getContext(KAddress selfAdr, OverlayId torrentId,
//    ConnectionDecider.SeederSideConn seederDecider, ConnectionDecider.LeecherSideConn leecherDecider) {
//
//    ConnComp.Init init = new ConnComp.Init(selfAdr, torrentId, seederDecider, leecherDecider);
//    TestContext<ConnComp> context = Testkit.newTestContext(ConnComp.class, init);
//    return context;
//  }
//  
//  public static void setSeederTransfer(TestContext tc, Component seeder, 
//    Function<TestContext, LinkedList> seederScenario) {
//    List<Pair<Class, List<Class>>> positivePorts = new LinkedList<>();
//    List<Pair<Class, List<Class>>> negativePorts = new LinkedList<>();
//    List<Class> connEvents = new LinkedList<>();
//    connEvents.add(ConnE.LeecherConnect.class);
//    positivePorts.add((Pair)Pair.with(ConnPort.class, connEvents));
//    Triplet onStart = null;
//    HashMap markers = new HashMap<>();
//    Set reflectEvents = new HashSet();
//    Component seederTransfer = tc.create(MockScenarioComp.class, new MockScenarioComp.Init(positivePorts, negativePorts,
//      onStart, seederScenario.apply(tc), markers, reflectEvents));
//    tc.connect(seeder.getPositive(ConnPort.class), seederTransfer.getNegative(ConnPort.class));
//  }
//  
//  public static void setLeecherTransfer(TestContext tc, Component leecher, 
//    Function<TestContext, LinkedList> leecherScenario) {
//    List<Pair<Class, List<Class>>> positivePorts = new LinkedList<>();
//    List<Pair<Class, List<Class>>> negativePorts = new LinkedList<>();
//    List<Class> connEvents = new LinkedList<>();
//    connEvents.add(ConnE.SeederConnect.class);
//    positivePorts.add((Pair)Pair.with(ConnPort.class, connEvents));
//    Triplet onStart = null;
//    HashMap markers = new HashMap<>();
//    Set reflectEvents = new HashSet();
//    Component leecherTransfer = tc.create(MockScenarioComp.class, new MockScenarioComp.Init(positivePorts, negativePorts,
//      onStart, leecherScenario.apply(tc), markers, reflectEvents));
//    tc.connect(leecher.getPositive(ConnPort.class), leecherTransfer.getNegative(ConnPort.class));
//  }
//  
//  public static void setNetwork(TestContext tc, Component leecher, Component seeder) {
//    //cross connecting networks - shortcircuit
//    Component seederNetwork = tc.create(MockNetwork.class, Init.NONE);
//    Component leecherNetwork = tc.create(MockNetwork.class, Init.NONE);
//    ((MockNetwork) seederNetwork.getComponent()).setPair((MockNetwork) leecherNetwork.getComponent());
//    ((MockNetwork) leecherNetwork.getComponent()).setPair((MockNetwork) seederNetwork.getComponent());
//    tc.connect(leecher.getNegative(Network.class), leecherNetwork.getPositive(Network.class));
//    tc.connect(seeder.getNegative(Network.class), seederNetwork.getPositive(Network.class));
//  }
//  
//  public static void setCroupier(TestContext tc, List<KAddress> peers, Component leecher) {
//    OverlayId croupierId = null;
//    Component croupier = tc.create(MockCroupierComp.class, new MockCroupierComp.Init(croupierId, peers));
//    tc.connect(leecher.getNegative(CroupierPort.class), croupier.getPositive(CroupierPort.class));
//  }
//  
//  public static Predicate<ConnComp> inspectLeecherState(final ConnId connId, final LeecherConnStates expectedState) {
//    return new Predicate<ConnComp>() {
//      @Override
//      public boolean apply(ConnComp comp) {
//        MultiFSM mfsm = comp.getLeecherFSM();
//        return expectedState.equals(mfsm.getFSMState(LeecherConnFSM.NAME, connId));
//      }
//    };
//  }
//  
//  public static Predicate<ConnComp> inspectSeederState(final ConnId connId, final SeederConnStates expectedState) {
//    return new Predicate<ConnComp>() {
//      @Override
//      public boolean apply(ConnComp comp) {
//        MultiFSM mfsm = comp.getSeederFSM();
//        return expectedState.equals(mfsm.getFSMState(SeederConnFSM.NAME, connId));
//      }
//    };
//  }
//}