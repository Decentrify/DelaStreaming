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
//package se.sics.cobweb.conn2.mngr;
//
//import se.sics.cobweb.conn2.mngr.ConnMngrComp;
//import se.sics.cobweb.conn2.mngr.ConnMngrFSM;
//import se.sics.cobweb.conn2.mngr.ConnMngrStates;
//import com.google.common.base.Predicate;
//import se.sics.kompics.testkit.TestContext;
//import se.sics.kompics.testkit.Testkit;
//import se.sics.ktoolbox.nutil.fsm.MultiFSM;
//import se.sics.ktoolbox.util.identifiable.overlay.OverlayId;
//import se.sics.ktoolbox.util.network.KAddress;
//
///**
// * @author Alex Ormenisan <aaor@kth.se>
// */
//public class ConnMngrHelper {
//
//  public static TestContext<ConnMngrComp> getContext(KAddress selfAdr) {
//    ConnMngrComp.Init init = new ConnMngrComp.Init(selfAdr, new MockConnCreator());
//    TestContext<ConnMngrComp> context = Testkit.newTestContext(ConnMngrComp.class, init);
//    return context;
//  }
//
//  public static Predicate<ConnMngrComp> emptyState() {
//    return new Predicate<ConnMngrComp>() {
//      @Override
//      public boolean apply(ConnMngrComp t) {
//        return t.isEmpty();
//      }
//    };
//  }
//
//  public static Predicate<ConnMngrComp> inspectState(final OverlayId torrentId, final ConnMngrStates expectedState) {
//    return new Predicate<ConnMngrComp>() {
//      @Override
//      public boolean apply(ConnMngrComp comp) {
//        MultiFSM mfsm = comp.getConnFSM();
//        return expectedState.equals(mfsm.getFSMState(ConnMngrFSM.NAME, torrentId.baseId));
//      }
//    };
//  }
//}
