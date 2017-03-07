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
//import se.sics.cobweb.conn2.mngr.ConnCtrlPort;
//import se.sics.cobweb.conn2.mngr.ConnMngrPort;
//import java.util.LinkedList;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//import se.sics.cobweb.conn2.mngr.event.ConnCtrlE;
//import se.sics.cobweb.conn2.mngr.event.ConnMngrE;
//import se.sics.kompics.ComponentDefinition;
//import se.sics.kompics.Handler;
//import se.sics.kompics.KompicsEvent;
//import se.sics.kompics.Negative;
//import se.sics.kompics.Positive;
//import se.sics.kompics.Start;
//
///**
// * @author Alex Ormenisan <aaor@kth.se>
// */
//public class ConnMngrDriver extends ComponentDefinition {
//
//  private static final Logger LOG = LoggerFactory.getLogger(ConnMngrDriver.class);
//
//  private final Positive<ConnMngrPort> connMngr = requires(ConnMngrPort.class);
//  private final Negative<ConnCtrlPort> connCtrl = provides(ConnCtrlPort.class);
//  private final LinkedList<KompicsEvent> connMngrEvents;
//  
//  public ConnMngrDriver(Init init) {
//    LOG.info("initiating");
//    connMngrEvents = init.connMngrEvents;
//
//    subscribe(handleStart, control);
//    subscribe(handleSetupSuccess, connMngr);
//    subscribe(handleStartSuccess, connMngr);
//    subscribe(handleStopSuccess, connMngr);
//    subscribe(handleClean, connCtrl);
//  }
//
//  Handler handleStart = new Handler<Start>() {
//
//    @Override
//    public void handle(Start event) {
//      LOG.info("starting");
//      if(!connMngrEvents.isEmpty()) {
//        trigger(connMngrEvents.removeFirst(), connMngr);
//      }
//    }
//  };
//  
//  Handler handleSetupSuccess = new Handler<ConnMngrE.SetupSuccess>() {
//    @Override
//    public void handle(ConnMngrE.SetupSuccess event) {
//      LOG.info("{}", event);
//      if(!connMngrEvents.isEmpty()) {
//        trigger(connMngrEvents.removeFirst(), connMngr);
//      }
//    }
//  };
//  
//  Handler handleStartSuccess = new Handler<ConnMngrE.StartSuccess>() {
//    @Override
//    public void handle(ConnMngrE.StartSuccess event) {
//      LOG.info("{}", event);
//      if(!connMngrEvents.isEmpty()) {
//        trigger(connMngrEvents.removeFirst(), connMngr);
//      }
//    }
//  };
//  
//  Handler handleStopSuccess = new Handler<ConnMngrE.StopSuccess>() {
//    @Override
//    public void handle(ConnMngrE.StopSuccess event) {
//      LOG.info("{}", event);
//      if(!connMngrEvents.isEmpty()) {
//        trigger(connMngrEvents.removeFirst(), connMngr);
//      }
//    }
//  };
//  
//  Handler handleClean = new Handler<ConnCtrlE.CleanReq>() {
//    @Override
//    public void handle(ConnCtrlE.CleanReq req) {
//      LOG.info("{}", req);
//      answer(req, req.complete());
//    }
//  };
//
//  public static class Init extends se.sics.kompics.Init<ConnMngrDriver> {
//    public final LinkedList<KompicsEvent> connMngrEvents;
//    
//    public Init(LinkedList<KompicsEvent> connMngrEvents) {
//      this.connMngrEvents = connMngrEvents;
//    }
//  }
//}
