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
import se.sics.kompics.network.Transport;
import se.sics.ktoolbox.nutil.fsm.api.FSMException;
import se.sics.ktoolbox.nutil.fsm.api.FSMStateName;
import se.sics.ktoolbox.nutil.fsm.handler.FSMEventHandler;
import se.sics.ktoolbox.nutil.fsm.handler.FSMMsgHandler;
import se.sics.ktoolbox.util.identifiable.Identifiable;
import se.sics.ktoolbox.util.network.KAddress;
import se.sics.ktoolbox.util.network.KContentMsg;
import se.sics.ktoolbox.util.network.KHeader;
import se.sics.ktoolbox.util.network.basic.BasicContentMsg;
import se.sics.ktoolbox.util.network.basic.BasicHeader;
import se.sics.nutil.network.bestEffort.event.BestEffortMsg;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class CLeecherHandleHandlers {

  private static final Logger LOG = LoggerFactory.getLogger(CLeecherHandleHandlers.class);

  static FSMEventHandler handleEventFallback
    = new FSMEventHandler<CLeecherHandleExternal, CLeecherHandleInternal, CLeecherHandleFSMEvent>() {
      @Override
      public FSMStateName handle(FSMStateName state, CLeecherHandleExternal es, CLeecherHandleInternal is,
        CLeecherHandleFSMEvent event) throws
      FSMException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
      }
    };

  static FSMMsgHandler handleMsgFallback
    = new FSMMsgHandler<CLeecherHandleExternal, CLeecherHandleInternal, CLeecherHandleFSMEvent>() {
      @Override
      public FSMStateName handle(FSMStateName state, CLeecherHandleExternal es, CLeecherHandleInternal is,
        CLeecherHandleFSMEvent payload, KContentMsg<KAddress, KHeader<KAddress>, CLeecherHandleFSMEvent> msg) throws
      FSMException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
      }
    };

  static FSMEventHandler handleConnectToSeeder
    = new FSMEventHandler<CLeecherHandleExternal, CLeecherHandleInternal, ConnE.Connect1Request>() {
      @Override
      public FSMStateName handle(FSMStateName state, CLeecherHandleExternal es, CLeecherHandleInternal is,
        ConnE.Connect1Request event) throws FSMException {
        LOG.trace("{}", event);
        is.msg1 = Optional.of(event);
        CHandleMsg.ConnectToSeeder req = new CHandleMsg.ConnectToSeeder(event.torrentId, event.handleId);
        sendBestEffort(es, is, req, event.seederAdr);
        return CLeecherHandleStates.WAITING_SEEDER;
      }
    };
  
  static FSMMsgHandler handleConnected
    = new FSMMsgHandler<CLeecherHandleExternal, CLeecherHandleInternal, CHandleMsg.Connected>() {
      @Override
      public FSMStateName handle(FSMStateName state, CLeecherHandleExternal es, CLeecherHandleInternal is,
        CHandleMsg.Connected payload, KContentMsg<KAddress, KHeader<KAddress>, CHandleMsg.Connected> msg) throws
      FSMException {
        LOG.trace("{}", msg);
        ConnE.Connect1Request req= is.msg1.get();
        es.getProxy().answer(req, req.accept());
        return CLeecherHandleStates.CONNECTED;
      }
    };

  private static void sendBestEffort(CLeecherHandleExternal es, CLeecherHandleInternal is, Identifiable payload,
    KAddress target) {
    int retries = 5;
    long rto = 500;
    BestEffortMsg.Request be = new BestEffortMsg.Request(payload, retries, rto);
    sendNetwork(es, is, be, target);
  }

  private static void sendNetwork(CLeecherHandleExternal es, CLeecherHandleInternal is, Object payload, KAddress target) {
    BasicHeader requestHeader = new BasicHeader(es.selfAdr, target, Transport.UDP);
    BasicContentMsg request = new BasicContentMsg(requestHeader, payload);
    LOG.debug("{} sending:{}", new Object[]{is.getFSMId(), request});
    es.getProxy().trigger(request, es.network);
  }
}
