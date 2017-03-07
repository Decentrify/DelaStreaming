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
//package se.sics.cobweb.transfer.instance;
//
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//import se.sics.cobweb.conn2.instance.event.ConnE;
//import se.sics.cobweb.transfer.mngr.event.TransferCtrlE;
//import se.sics.ktoolbox.nutil.fsm.api.FSMException;
//import se.sics.ktoolbox.nutil.fsm.api.FSMStateName;
//import se.sics.ktoolbox.nutil.fsm.handler.FSMEventHandler;
//import se.sics.ktoolbox.nutil.fsm.handler.FSMMsgHandler;
//import se.sics.ktoolbox.util.network.KAddress;
//import se.sics.ktoolbox.util.network.KContentMsg;
//import se.sics.ktoolbox.util.network.KHeader;
//
///**
// * @author Alex Ormenisan <aaor@kth.se>
// */
//public class SeederTransferHandlers {
//  private static final Logger LOG = LoggerFactory.getLogger(LeecherTransferHandlers.class);
//
//  static FSMEventHandler handleEventFallback
//    = new FSMEventHandler<SeederTransferExternal, SeederTransferInternal, TransferFSMEvent>() {
//      @Override
//      public FSMStateName handle(FSMStateName state, SeederTransferExternal es, SeederTransferInternal is,
//        TransferFSMEvent event) throws
//      FSMException {
//        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
//      }
//    };
//
//  static FSMMsgHandler handleMsgFallback
//    = new FSMMsgHandler<SeederTransferExternal, SeederTransferInternal, TransferFSMEvent>() {
//      @Override
//      public FSMStateName handle(FSMStateName state, SeederTransferExternal es, SeederTransferInternal is,
//        TransferFSMEvent payload, KContentMsg<KAddress, KHeader<KAddress>, TransferFSMEvent> msg) throws FSMException {
//        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
//      }
//    };
//  
//  static FSMEventHandler handleStart
//    = new FSMEventHandler<SeederTransferExternal, SeederTransferInternal, TransferCtrlE.Start>() {
//      @Override
//      public FSMStateName handle(FSMStateName state, SeederTransferExternal es, SeederTransferInternal is,
//        TransferCtrlE.Start event) throws FSMException {
//        return SeederTransferStates.ACTIVE;
//      }
//    };
//  
//  static FSMEventHandler handleLeecherConnectedStart
//    = new FSMEventHandler<SeederTransferExternal, SeederTransferInternal, ConnE.LeecherConnect>() {
//      @Override
//      public FSMStateName handle(FSMStateName state, SeederTransferExternal es, SeederTransferInternal is,
//        ConnE.LeecherConnect event) throws FSMException {
//        es.transferView.leecherConnected(event);
//        return SeederTransferStates.ACTIVE;
//      }
//    };
//  
//  static FSMEventHandler handleLeecherConnectedNop
//    = new FSMEventHandler<SeederTransferExternal, SeederTransferInternal, ConnE.LeecherConnect>() {
//      @Override
//      public FSMStateName handle(FSMStateName state, SeederTransferExternal es, SeederTransferInternal is,
//        ConnE.LeecherConnect event) throws FSMException {
//        es.transferView.leecherConnected(event);
//        return state;
//      }
//    };
//}
//