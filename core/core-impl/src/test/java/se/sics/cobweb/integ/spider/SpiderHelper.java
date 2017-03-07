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
//import se.sics.cobweb.mngr.SpiderComp;
//import se.sics.cobweb.overlord.conn.impl.ConnectionDeciderImpl;
//import se.sics.cobweb.transfer.TransferCreator;
//import se.sics.cobweb.transfer.TransferHelper;
//import se.sics.cobweb.transfer.handlemngr.LeecherHandleCreator;
//import se.sics.cobweb.transfer.handlemngr.SeederHandleCreator;
//import se.sics.cobweb.transfer.instance.LeecherHandleHelper;
//import se.sics.cobweb.transfer.instance.SeederHandleHelper;
//import se.sics.kompics.testkit.TestContext;
//import se.sics.kompics.testkit.Testkit;
//import se.sics.ktoolbox.util.identifiable.overlay.OverlayId;
//import se.sics.ktoolbox.util.network.KAddress;
//
///**
// *
// * @author Alex Ormenisan <aaor@kth.se>
// */
//public class SpiderHelper {
//  public static TestContext<SpiderComp> getContext(OverlayId torrentId, KAddress selfAdr) {
//    ConnectionDeciderImpl.SeederSide seederSide = new ConnectionDeciderImpl.SeederSide();
//    ConnectionDeciderImpl.LeecherSide leecherSide = new ConnectionDeciderImpl.LeecherSide();
//    LeecherHandleCreator leecherHandleCreator = LeecherHandleHelper.defaultCreator();
//    SeederHandleCreator seederHandleCreator = SeederHandleHelper.defaultCreator();
//    TransferCreator transferCreator = TransferHelper.defaultCreator();
//    SpiderComp.Init init = new SpiderComp.Init(selfAdr, leecherHandleCreator, seederHandleCreator, transferCreator, seederSide, leecherSide);
//    TestContext<SpiderComp> context = Testkit.newTestContext(SpiderComp.class, init);
//    return context;
//  }
//}
