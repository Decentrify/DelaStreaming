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
package se.sics.cobweb.transfer.mngr;

import com.google.common.base.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.cobweb.transfer.mngr.event.TransferCtrlE;
import se.sics.cobweb.transfer.mngr.event.TransferFaultE;
import se.sics.cobweb.transfer.mngr.event.TransferMngrE;
import se.sics.kompics.Kill;
import se.sics.kompics.Start;
import se.sics.ktoolbox.nutil.fsm.api.FSMBasicStateNames;
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
public class TransferMngrHandlers {
  private static final Logger LOG = LoggerFactory.getLogger(TransferMngrHandlers.class);

  static FSMEventHandler handleEventFallback
    = new FSMEventHandler<TransferMngrExternal, TransferMngrInternal, TransferMngrFSMEvent>() {
      @Override
      public FSMStateName handle(FSMStateName state, TransferMngrExternal es, TransferMngrInternal is,
        TransferMngrFSMEvent event) throws FSMException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
      }
    };

  static FSMMsgHandler handleMsgFallback
    = new FSMMsgHandler<TransferMngrExternal, TransferMngrInternal, TransferMngrFSMEvent>() {
      @Override
      public FSMStateName handle(FSMStateName state, TransferMngrExternal es, TransferMngrInternal is,
        TransferMngrFSMEvent payload, KContentMsg<KAddress, KHeader<KAddress>, TransferMngrFSMEvent> msg) throws FSMException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
      }
    };

  static FSMEventHandler handleSetup
    = new FSMEventHandler<TransferMngrExternal, TransferMngrInternal, TransferMngrE.SetupReq>() {
      @Override
      public FSMStateName handle(FSMStateName state, TransferMngrExternal es, TransferMngrInternal is,
        TransferMngrE.SetupReq req) throws FSMException {
        LOG.debug("setup connection:{}", req);
        is.torrentId = req.torrentId;
        is.comp = es.createConnComp(req.torrentId, req.torrent);
        es.setChannels(req.torrentId, is.comp);
        es.getProxy().answer(req, req.success());
        return TransferMngrStates.SETUP;
      }
    };

  static FSMEventHandler handleStart
    = new FSMEventHandler<TransferMngrExternal, TransferMngrInternal, TransferMngrE.StartReq>() {
      @Override
      public FSMStateName handle(FSMStateName state, TransferMngrExternal es, TransferMngrInternal is,
        TransferMngrE.StartReq req) throws FSMException {
        LOG.debug("start connection:{}", req);
        is.startReq = Optional.of(req);
        es.getProxy().trigger(Start.event, is.comp.control());
        es.getProxy().answer(req, req.success());
        return TransferMngrStates.ACTIVE;
      }
    };
  
  static FSMEventHandler handleStopSetup
    = new FSMEventHandler<TransferMngrExternal, TransferMngrInternal, TransferMngrE.StopReq>() {
      @Override
      public FSMStateName handle(FSMStateName state, TransferMngrExternal es, TransferMngrInternal is,
        TransferMngrE.StopReq req) throws FSMException {
        LOG.debug("stop connection:{}", req);
        is.stopReq = Optional.of(req);
        es.getProxy().trigger(new TransferCtrlE.CleanReq(is.torrentId), es.transferCtrl);
        return TransferMngrStates.CLEANING;
      }
    };

  static FSMEventHandler handleStopActive
    = new FSMEventHandler<TransferMngrExternal, TransferMngrInternal, TransferMngrE.StopReq>() {
      @Override
      public FSMStateName handle(FSMStateName state, TransferMngrExternal es, TransferMngrInternal is,
        TransferMngrE.StopReq req) throws FSMException {
        LOG.debug("stop connection:{}", req);
        is.startReq = Optional.absent();
        is.stopReq = Optional.of(req);
        es.getProxy().trigger(new TransferCtrlE.CleanReq(is.torrentId), es.transferCtrl);
        return TransferMngrStates.CLEANING;
      }
    };
  
  static FSMEventHandler handleStopCleaning
    = new FSMEventHandler<TransferMngrExternal, TransferMngrInternal, TransferMngrE.StopReq>() {
      @Override
      public FSMStateName handle(FSMStateName state, TransferMngrExternal es, TransferMngrInternal is,
        TransferMngrE.StopReq req) throws FSMException {
        LOG.debug("stop connection:{}", req);
        is.startReq = Optional.absent();
        is.stopReq = Optional.of(req);
        return TransferMngrStates.CLEANING;
      }
    };

  static FSMEventHandler handleFaultRunning
    = new FSMEventHandler<TransferMngrExternal, TransferMngrInternal, TransferFaultE>() {
      @Override
      public FSMStateName handle(FSMStateName state, TransferMngrExternal es, TransferMngrInternal is,
        TransferFaultE req) throws FSMException {
        LOG.info("running fault:{}", req);
        is.runningFault = Optional.of(req.fault);
        es.getProxy().trigger(new TransferCtrlE.CleanReq(is.torrentId), es.transferCtrl);
        return TransferMngrStates.CLEANING;
      }
    };

  static FSMEventHandler handleFaultCleaning
    = new FSMEventHandler<TransferMngrExternal, TransferMngrInternal, TransferFaultE>() {
      @Override
      public FSMStateName handle(FSMStateName state, TransferMngrExternal es, TransferMngrInternal is,
        TransferFaultE req) throws FSMException {
        LOG.info("cleaning fault:{}", req);
        is.cleaningFault = Optional.of(req.fault);
        localClean(es, is);
        return FSMBasicStateNames.FINAL;
      }
    };

  static FSMEventHandler handleCleaned
    = new FSMEventHandler<TransferMngrExternal, TransferMngrInternal, TransferCtrlE.CleanResp>() {
      @Override
      public FSMStateName handle(FSMStateName state, TransferMngrExternal es, TransferMngrInternal is,
        TransferCtrlE.CleanResp resp) throws FSMException {
        LOG.info("cleaned:{}", resp);
        localClean(es, is);
        return FSMBasicStateNames.FINAL;
      }
    };

  private static void localClean(TransferMngrExternal es, TransferMngrInternal is) {
    es.cleanChannels(is.torrentId, is.comp);
    es.getProxy().trigger(Kill.event, is.comp.control());
    is.answerCleaned(es);
  }
}
