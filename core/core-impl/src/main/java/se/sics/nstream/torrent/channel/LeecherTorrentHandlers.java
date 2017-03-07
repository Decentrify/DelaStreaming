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
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//import se.sics.kompics.network.Transport;
//import se.sics.ktoolbox.nutil.fsm.api.FSMEvent;
//import se.sics.ktoolbox.nutil.fsm.api.FSMException;
//import se.sics.ktoolbox.nutil.fsm.api.FSMStateName;
//import se.sics.ktoolbox.nutil.fsm.handler.FSMEventHandler;
//import se.sics.ktoolbox.nutil.fsm.handler.FSMMsgHandler;
//import se.sics.ktoolbox.util.identifiable.Identifiable;
//import se.sics.ktoolbox.util.network.KAddress;
//import se.sics.ktoolbox.util.network.KContentMsg;
//import se.sics.ktoolbox.util.network.KHeader;
//import se.sics.ktoolbox.util.network.basic.BasicContentMsg;
//import se.sics.ktoolbox.util.network.basic.BasicHeader;
//import se.sics.nstream.torrent.channel.msg.TorrentMsg;
//import se.sics.nstream.torrent.conn.event.Seeder;
//import se.sics.nutil.network.bestEffort.event.BestEffortMsg;
//
///**
// * @author Alex Ormenisan <aaor@kth.se>
// */
//public class LeecherTorrentHandlers {
//
//  private static final Logger LOG = LoggerFactory.getLogger(LeecherTorrentFSM.class);
//
//  static FSMEventHandler handleEventFallback
//    = new FSMEventHandler<LeecherTorrentExternal, LeecherTorrentInternal, FSMEvent>() {
//      @Override
//      public FSMStateName handle(FSMStateName state, LeecherTorrentExternal es, LeecherTorrentInternal is,
//        FSMEvent event) throws
//      FSMException {
//        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
//      }
//    };
//
//  static FSMMsgHandler handleMsgFallback
//    = new FSMMsgHandler<LeecherTorrentExternal, LeecherTorrentInternal, FSMEvent>() {
//      @Override
//      public FSMStateName handle(FSMStateName state, LeecherTorrentExternal es, LeecherTorrentInternal is,
//        FSMEvent payload, KContentMsg<KAddress, KHeader<KAddress>, FSMEvent> msg) throws FSMException {
//        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
//      }
//    };
//
//  static FSMEventHandler handleSeederConnect
//    = new FSMEventHandler<LeecherTorrentExternal, LeecherTorrentInternal, Seeder.Connect>() {
//      @Override
//      public FSMStateName handle(FSMStateName state, LeecherTorrentExternal es, LeecherTorrentInternal is,
//        Seeder.Connect event) throws FSMException {
//        LOG.debug("{} received:{}", is.getFSMId(), event);
//        is.setConnectSeeder(event);
//        sendBestEffort(es, is, is.peerConnecting.connectReq, is.target);
//        return LeecherTorrentStates.CONNECTING;
//      }
//    };
//
//  static FSMMsgHandler handleTorrentMsgConnect
//    = new FSMMsgHandler<SeederTorrentExternal, SeederTorrentInternal, TorrentMsg.Connected>() {
//      @Override
//      public FSMStateName handle(FSMStateName state, SeederTorrentExternal es, SeederTorrentInternal is,
//        TorrentMsg.Connected payload, KContentMsg<KAddress, KHeader<KAddress>, TorrentMsg.Connected> msg) throws
//      FSMException {
//        LOG.debug("{} received:{}", is.getFSMId(), msg);
//        return LeecherTorrentStates.CONNECTED;
//      }
//    };
//
//  private static void sendBestEffort(LeecherTorrentExternal es, LeecherTorrentInternal is, Identifiable payload, KAddress target) {
//    int retries = 5;
//    long rto = 500;
//    BestEffortMsg.Request be = new BestEffortMsg.Request(payload, retries, rto);
//    sendNetwork(es, is, payload, target);
//  }
//  
//  private static void sendNetwork(LeecherTorrentExternal es, LeecherTorrentInternal is, Object payload, KAddress target) {
//    BasicHeader requestHeader = new BasicHeader(es.selfAdr, target, Transport.UDP);
//    BasicContentMsg request = new BasicContentMsg(requestHeader, payload);
//    LOG.debug("{} sending:{}", new Object[]{is.getFSMId(), request});
//    es.getProxy().trigger(request, es.network);
//  }
//}
