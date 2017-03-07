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
import se.sics.cobweb.transfer.handle.event.LeecherHandleCtrlE;
import se.sics.cobweb.transfer.handlemngr.event.HandleMngrE;
import se.sics.ktoolbox.nutil.fsm.api.FSMException;
import se.sics.ktoolbox.nutil.fsm.api.FSMStateName;
import se.sics.ktoolbox.nutil.fsm.handler.FSMEventHandler;
import se.sics.ktoolbox.nutil.fsm.handler.FSMMsgHandler;
import se.sics.ktoolbox.util.network.KAddress;
import se.sics.ktoolbox.util.network.KContentMsg;
import se.sics.ktoolbox.util.network.KHeader;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class LeecherHandleOverlordHandlers {

  private static final Logger LOG = LoggerFactory.getLogger(LeecherHandleOverlordHandlers.class);

  static FSMEventHandler handleEventFallback
    = new FSMEventHandler<LeecherHandleOverlordExternal, LeecherHandleOverlordInternal, LeecherHandleOverlordFSMEvent>() {
      @Override
      public FSMStateName handle(FSMStateName state, LeecherHandleOverlordExternal es, LeecherHandleOverlordInternal is,
        LeecherHandleOverlordFSMEvent event) throws FSMException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
      }
    };

  static FSMMsgHandler handleMsgFallback
    = new FSMMsgHandler<LeecherHandleOverlordExternal, LeecherHandleOverlordInternal, LeecherHandleOverlordFSMEvent>() {
      @Override
      public FSMStateName handle(FSMStateName state, LeecherHandleOverlordExternal es, LeecherHandleOverlordInternal is,
        LeecherHandleOverlordFSMEvent payload,
        KContentMsg<KAddress, KHeader<KAddress>, LeecherHandleOverlordFSMEvent> msg) throws FSMException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
      }
    };

  static FSMEventHandler handleSeederConnect
    = new FSMEventHandler<LeecherHandleOverlordExternal, LeecherHandleOverlordInternal, ConnE.Connect1Accept>() {
      @Override
      public FSMStateName handle(FSMStateName state, LeecherHandleOverlordExternal es, LeecherHandleOverlordInternal is,
        ConnE.Connect1Accept event) throws FSMException {
        LOG.trace("{}", event);
        es.triggerSetupHandle(event);
        return LeecherHandleOverlordStates.SETUP_HANDLE;
      }
    };

  static FSMEventHandler handleSetupLeecher
    = new FSMEventHandler<LeecherHandleOverlordExternal, LeecherHandleOverlordInternal, HandleMngrE.LeecherConnected>() {
      @Override
      public FSMStateName handle(FSMStateName state, LeecherHandleOverlordExternal es, LeecherHandleOverlordInternal is,
        HandleMngrE.LeecherConnected event) throws FSMException {
        LOG.trace("event", event);
        is.m1 = Optional.of(event);
        if (ready(is)) {
          return LeecherHandleOverlordStates.ACTIVE;
        } else {
          return state;
        }
      }
    };

  static FSMEventHandler handleReadyLeecher
    = new FSMEventHandler<LeecherHandleOverlordExternal, LeecherHandleOverlordInternal, LeecherHandleCtrlE.Ready>() {
      @Override
      public FSMStateName handle(FSMStateName state, LeecherHandleOverlordExternal es, LeecherHandleOverlordInternal is,
        LeecherHandleCtrlE.Ready event) throws FSMException {
        LOG.trace("event", event);
        is.m2 = Optional.of(event);
        if (ready(is)) {
          return LeecherHandleOverlordStates.ACTIVE;
        } else {
          return state;
        }
      }
    };

  private static boolean ready(LeecherHandleOverlordInternal is) {
    return is.m1.isPresent() && is.m2.isPresent();
  }
}
