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
package se.sics.cobweb.overlord.handle;

import com.google.common.base.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.cobweb.conn.event.ConnE;
import se.sics.cobweb.overlord.conn.api.LeecherState;
import se.sics.cobweb.overlord.conn.api.TorrentState;
import se.sics.cobweb.transfer.handle.event.SeederHandleCtrlE;
import se.sics.cobweb.transfer.handlemngr.event.HandleMngrE;
import se.sics.ktoolbox.nutil.fsm.api.FSMException;
import se.sics.ktoolbox.nutil.fsm.api.FSMStateName;
import se.sics.ktoolbox.nutil.fsm.handler.FSMEventHandler;
import se.sics.ktoolbox.nutil.fsm.handler.FSMMsgHandler;
import se.sics.ktoolbox.util.identifiable.overlay.OverlayId;
import se.sics.ktoolbox.util.network.KAddress;
import se.sics.ktoolbox.util.network.KContentMsg;
import se.sics.ktoolbox.util.network.KHeader;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class SeederHandleOverlordHandlers {

  private static final Logger LOG = LoggerFactory.getLogger(SeederHandleOverlordHandlers.class);

  static FSMEventHandler handleEventFallback
    = new FSMEventHandler<SeederHandleOverlordExternal, SeederHandleOverlordInternal, SeederHandleOverlordFSMEvent>() {
      @Override
      public FSMStateName handle(FSMStateName state, SeederHandleOverlordExternal es, SeederHandleOverlordInternal is,
        SeederHandleOverlordFSMEvent event) throws
      FSMException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
      }
    };

  static FSMMsgHandler handleMsgFallback
    = new FSMMsgHandler<SeederHandleOverlordExternal, SeederHandleOverlordInternal, SeederHandleOverlordFSMEvent>() {
      @Override
      public FSMStateName handle(FSMStateName state, SeederHandleOverlordExternal es, SeederHandleOverlordInternal is,
        SeederHandleOverlordFSMEvent payload, KContentMsg<KAddress, KHeader<KAddress>, SeederHandleOverlordFSMEvent> msg)
      throws FSMException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
      }
    };

  static FSMEventHandler handleLeecherConnectTry
    = new FSMEventHandler<SeederHandleOverlordExternal, SeederHandleOverlordInternal, ConnE.Connect2Request>() {
      @Override
      public FSMStateName handle(FSMStateName state, SeederHandleOverlordExternal es, SeederHandleOverlordInternal is,
        ConnE.Connect2Request event) throws FSMException {
        LOG.trace("{}", event);
        switch (es.decider.canConnect(es.seedersView, es.leechersView, getLeecherState(event.leecherAdr), getTorrentState(event.torrentId))) {
          case CONNECT:
            is.m1 = Optional.of(event);
            es.triggerSetupHandle(event);
            return SeederHandleOverlordStates.SETUP_HANDLE;
          default:
            throw new RuntimeException("fix me ");
        }
      }
    };

  static FSMEventHandler handleSetupSeeder
    = new FSMEventHandler<SeederHandleOverlordExternal, SeederHandleOverlordInternal, HandleMngrE.SeederConnected>() {
      @Override
      public FSMStateName handle(FSMStateName state, SeederHandleOverlordExternal es, SeederHandleOverlordInternal is,
        HandleMngrE.SeederConnected event) throws FSMException {
        LOG.trace("{}", event);
        is.m2 = Optional.of(event);
        es.getProxy().answer(is.m1.get(), is.m1.get().accept());
        if (ready(is)) {
          return LeecherHandleOverlordStates.ACTIVE;
        } else {
          return state;
        }
      }
    };

  static FSMEventHandler handleReadySeeder
    = new FSMEventHandler<SeederHandleOverlordExternal, SeederHandleOverlordInternal, SeederHandleCtrlE.Ready>() {
      @Override
      public FSMStateName handle(FSMStateName state, SeederHandleOverlordExternal es, SeederHandleOverlordInternal is,
        SeederHandleCtrlE.Ready event) throws FSMException {
        LOG.trace("event", event);
        is.m3 = Optional.of(event);
        if (ready(is)) {
          return LeecherHandleOverlordStates.ACTIVE;
        } else {
          return state;
        }
      }
    };
  
  private static LeecherState getLeecherState(KAddress leecherAdr) {
    return new LeecherState(leecherAdr);
  }

  private static TorrentState getTorrentState(OverlayId torrentId) {
    return new TorrentState(torrentId);
  }
  
  private static boolean ready(SeederHandleOverlordInternal is) {
    return is.m2.isPresent() && is.m3.isPresent();
  }
}
