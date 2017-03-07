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
//package se.sics.cobweb.conn2.instance;
//
//import com.google.common.base.Function;
//import java.util.LinkedList;
//import se.sics.cobweb.conn2.instance.event.ConnE;
//import se.sics.kompics.testkit.TestContext;
//import se.sics.ktoolbox.util.Either;
//
///**
// * @author Alex Ormenisan <aaor@kth.se>
// */
//public class Transfer1 {
//
//  public static Function<TestContext, LinkedList> seederTransferScenario() {
//    return new Function<TestContext, LinkedList>() {
//      @Override
//      public LinkedList apply(TestContext f) {
//        //LinkedList<Either<Function<Direct.Request, Direct.Response>, Triplet<KompicsEvent, Boolean, Class>>>
//        LinkedList nextEvents = new LinkedList<>();
//        nextEvents.add(Either.left(connected()));
//        return nextEvents;
//      }
//    };
//  }
//
//  public static Function<TestContext, LinkedList> leecherTransferScenario() {
//    return new Function<TestContext, LinkedList>() {
//      @Override
//      public LinkedList apply(TestContext f) {
//        //LinkedList<Either<Function<Direct.Request, Direct.Response>, Triplet<KompicsEvent, Boolean, Class>>>
//        LinkedList nextEvents = new LinkedList<>();
//        return nextEvents;
//      }
//    };
//  }
//
//  public static Function<ConnE.LeecherConnect, ConnE.LeecherConnected> connected() {
//    return new Function<ConnE.LeecherConnect, ConnE.LeecherConnected>() {
//      @Override
//      public ConnE.LeecherConnected apply(ConnE.LeecherConnect req) {
//        return req.success();
//      }
//    };
//  }
//}
