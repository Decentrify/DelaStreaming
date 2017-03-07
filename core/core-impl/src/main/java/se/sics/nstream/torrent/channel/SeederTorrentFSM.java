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
//package se.sics.nstream.torrent.channel;
//
//import org.javatuples.Pair;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//import se.sics.ktoolbox.nutil.fsm.FSMBuilder;
//import se.sics.ktoolbox.nutil.fsm.FSMachineDef;
//import se.sics.ktoolbox.nutil.fsm.MultiFSM;
//import se.sics.ktoolbox.nutil.fsm.api.FSMBasicStateNames;
//import se.sics.ktoolbox.nutil.fsm.api.FSMException;
//import se.sics.ktoolbox.nutil.fsm.api.FSMInternalStateBuilder;
//import se.sics.ktoolbox.nutil.fsm.genericsetup.OnFSMExceptionAction;
//import se.sics.nstream.torrent.channel.msg.TorrentMsg;
//
///**
// * @author Alex Ormenisan <aaor@kth.se>
// */
//public class SeederTorrentFSM {
//  private static final Logger LOG = LoggerFactory.getLogger(SeederTorrentFSM.class);
//  public static final String NAME = "dela-torrent-seeder-channel-fsm";
//  
//  public static FSMachineDef fsm() throws FSMException {
//    Pair<FSMBuilder.Machine, FSMBuilder.Handlers> fp = prepare();
//    FSMachineDef m = FSMBuilder.fsmDef(NAME, fp.getValue0(), fp.getValue1());
//    return m;
//  }
//  
//  public static MultiFSM multifsm(SeederTorrentExternal es, OnFSMExceptionAction oexa) throws FSMException {
//    Pair<FSMBuilder.Machine, FSMBuilder.Handlers> fp = prepare();
//    
//    FSMInternalStateBuilder isb = new SeederTorrentInternal.Builder();
//    MultiFSM fsm = FSMBuilder.multiFSM(NAME, fp.getValue0(), fp.getValue1(), es, isb, oexa);
//
//    return fsm;
//  }
//  
//  private static Pair<FSMBuilder.Machine, FSMBuilder.Handlers> prepare() throws FSMException {
//    
//    FSMBuilder.Machine machine = FSMBuilder.machine()
//      .onState(FSMBasicStateNames.START)
//        .nextStates(SeederTorrentStates.CONNECTED)
//        .buildTransition()
//      .onState(SeederTorrentStates.CONNECTED)
//        .toFinal()
//        .buildTransition();
//
//    FSMBuilder.Handlers handlers = FSMBuilder.events()
//      .defaultFallback(SeederTorrentHandlers.handleEventFallback, SeederTorrentHandlers.handleMsgFallback)
//      .positiveNetwork()
//        .onMsg(TorrentMsg.Connect.class)
//          .subscribe(SeederTorrentHandlers.torrentConnectInInit, FSMBasicStateNames.START)
//          .buildMsgs();
//    
//    return Pair.with(machine, handlers);
//  }
//}
