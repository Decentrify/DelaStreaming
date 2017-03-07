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
//import se.sics.ktoolbox.nutil.fsm.api.FSMBasicStateNames;
//import se.sics.ktoolbox.nutil.fsm.api.FSMEvent;
//import se.sics.ktoolbox.nutil.fsm.api.FSMException;
//import se.sics.ktoolbox.nutil.fsm.api.FSMStateName;
//import se.sics.ktoolbox.nutil.fsm.handler.FSMEventHandler;
//import se.sics.ktoolbox.nutil.fsm.handler.FSMMsgHandler;
//import se.sics.ktoolbox.util.network.KAddress;
//import se.sics.ktoolbox.util.network.KContentMsg;
//import se.sics.ktoolbox.util.network.KHeader;
//import se.sics.nstream.torrent.channel.msg.TorrentMsg;
//import se.sics.nstream.torrent.channel.state.api.ConnectionState;
//import se.sics.nstream.torrent.channel.state.api.LeecherState;
//import se.sics.nstream.torrent.channel.state.api.TorrentState;
//
///**
// * @author Alex Ormenisan <aaor@kth.se>
// */
//public class SeederTorrentHandlers {
//
//  private static final Logger LOG = LoggerFactory.getLogger(SeederTorrentHandlers.class);
//
//  static FSMEventHandler handleEventFallback
//    = new FSMEventHandler<SeederTorrentExternal, SeederTorrentInternal, FSMEvent>() {
//      @Override
//      public FSMStateName handle(FSMStateName state, SeederTorrentExternal es, SeederTorrentInternal is, FSMEvent event)
//      throws
//      FSMException {
//        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
//      }
//    };
//
//  static FSMMsgHandler handleMsgFallback = new FSMMsgHandler<SeederTorrentExternal, SeederTorrentInternal, FSMEvent>() {
//    @Override
//    public FSMStateName handle(FSMStateName state, SeederTorrentExternal es, SeederTorrentInternal is,
//      FSMEvent payload, KContentMsg<KAddress, KHeader<KAddress>, FSMEvent> msg) throws FSMException {
//      throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
//    }
//  };
//
//  static FSMMsgHandler torrentConnectInInit
//    = new FSMMsgHandler<SeederTorrentExternal, SeederTorrentInternal, TorrentMsg.Connect>() {
//      @Override
//      public FSMStateName handle(FSMStateName state, SeederTorrentExternal es, SeederTorrentInternal is,
//        TorrentMsg.Connect payload, KContentMsg<KAddress, KHeader<KAddress>, TorrentMsg.Connect> msg) throws
//      FSMException {
//
//        LOG.debug("{}received:{}", is.getFSMId(), msg);
//        LeecherState leecher = es.overview.getLeecher(msg.getHeader().getSource());
//        TorrentState torrent = es.overview.getTorrent(payload.torrentId);
//        ConnectionState connection = es.torrentPolicy.canConnect(es.overview, leecher, torrent);
//        switch (connection) {
//          case UNCHOKE:
//          case CONNECT:
//            is.connectLeecher(payload, leecher.target);
//            es.overview.setConnection(connection, leecher, torrent);
//            answerNetwork(es, is, msg, payload.connected());
//            return SeederTorrentStates.CONNECTED;
//          case CHOKE:
//            is.connectLeecher(payload, leecher.target);
//            es.overview.setConnection(connection, leecher, torrent);
//            answerNetwork(es, is, msg, payload.choke());
//            return SeederTorrentStates.CHOKED;
//          case DISCONNECT:
//            answerNetwork(es, is, msg, payload.disconnect());
//            return FSMBasicStateNames.FINAL;
//          default:
//            throw new RuntimeException("unhandled conn resp:" + connection);
//        }
//      }
//    };
//
//  private static void answerNetwork(SeederTorrentExternal es, SeederTorrentInternal is, KContentMsg msg,
//    Object payload) {
//
//    KContentMsg resp = msg.answer(payload);
//    LOG.debug("{}answering:{} to:{}", new Object[]{is.getFSMId(), payload, resp.getHeader().getDestination()});
//    es.getProxy().trigger(resp, es.network);
//  }
//}
