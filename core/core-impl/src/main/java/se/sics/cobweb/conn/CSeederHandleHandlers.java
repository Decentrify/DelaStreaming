/*
 * Copyright (C) 2009 Swedish Institute of Computer Science (SICS) Copyright (C)
 * 2009 Royal Institute of Technology (KTH)
 *
 * GVoD is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
 */
package se.sics.cobweb.conn;

import com.google.common.base.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.cobweb.conn.event.ConnE;
import se.sics.cobweb.conn.msg.CHandleMsg;
import se.sics.ktoolbox.nutil.fsm.api.FSMException;
import se.sics.ktoolbox.nutil.fsm.api.FSMStateName;
import se.sics.ktoolbox.nutil.fsm.handler.FSMEventHandler;
import se.sics.ktoolbox.nutil.fsm.handler.FSMMsgHandler;
import se.sics.ktoolbox.util.identifiable.Identifiable;
import se.sics.ktoolbox.util.network.KAddress;
import se.sics.ktoolbox.util.network.KContentMsg;
import se.sics.ktoolbox.util.network.KHeader;
import se.sics.nutil.network.bestEffort.event.BestEffortMsg;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class CSeederHandleHandlers {

  private static final Logger LOG = LoggerFactory.getLogger(CSeederHandleHandlers.class);

  static FSMEventHandler handleEventFallback
    = new FSMEventHandler<CSeederHandleExternal, CSeederHandleInternal, CSeederHandleFSMEvent>() {
      @Override
      public FSMStateName handle(FSMStateName state, CSeederHandleExternal es, CSeederHandleInternal is,
        CSeederHandleFSMEvent event)
      throws
      FSMException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
      }
    };

  static FSMMsgHandler handleMsgFallback
    = new FSMMsgHandler<CSeederHandleExternal, CSeederHandleInternal, CSeederHandleFSMEvent>() {
      @Override
      public FSMStateName handle(FSMStateName state, CSeederHandleExternal es, CSeederHandleInternal is,
        CSeederHandleFSMEvent payload, KContentMsg<KAddress, KHeader<KAddress>, CSeederHandleFSMEvent> msg) throws
      FSMException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
      }
    };

  static FSMMsgHandler handleConnectToSeeder
    = new FSMMsgHandler<CSeederHandleExternal, CSeederHandleInternal, CHandleMsg.ConnectToSeeder>() {
      @Override
      public FSMStateName handle(FSMStateName state, CSeederHandleExternal es, CSeederHandleInternal is,
        CHandleMsg.ConnectToSeeder payload, KContentMsg<KAddress, KHeader<KAddress>, CHandleMsg.ConnectToSeeder> msg)
      throws FSMException {
        LOG.trace("{}");
        is.msg1 = Optional.of(msg);
        ConnE.Connect2Request req = new ConnE.Connect2Request(payload.torrentId, payload.handleId, msg.
          getHeader().getSource());
        is.msg2 = Optional.of(req);
        es.getProxy().trigger(req, es.conn);
        return CSeederHandleStates.WAITING_SETUP;
      }
    };

  static FSMEventHandler handleSetup
    = new FSMEventHandler<CSeederHandleExternal, CSeederHandleInternal, ConnE.Connect2Accept>() {
      @Override
      public FSMStateName handle(FSMStateName state, CSeederHandleExternal es, CSeederHandleInternal is,
        ConnE.Connect2Accept event) throws FSMException {
        LOG.trace("{}");
        CHandleMsg.Connected resp = is.msg1.get().getContent().success();
        answerBestEffort(es, is, is.msg1.get(), resp);
        return CSeederHandleStates.CONNECTED;
      }
    };

  private static void answerBestEffort(CSeederHandleExternal es, CSeederHandleInternal is, KContentMsg msg,
    Identifiable payload) {
    int retries = 5;
    long rto = 500;
    BestEffortMsg.Request be = new BestEffortMsg.Request(payload, retries, rto);
    answerNetwork(es, is, msg, be);
  }

  private static void answerNetwork(CSeederHandleExternal es, CSeederHandleInternal is, KContentMsg msg,
    Object payload) {

    KContentMsg resp = msg.answer(payload);
    LOG.debug("{}answering:{} to:{}", new Object[]{is.getFSMId(), payload, resp.getHeader().getDestination()});
    es.getProxy().trigger(resp, es.network);
  }
}
