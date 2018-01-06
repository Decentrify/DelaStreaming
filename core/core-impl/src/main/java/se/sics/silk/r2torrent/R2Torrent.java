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
package se.sics.silk.r2torrent;

import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.kompics.ComponentProxy;
import se.sics.kompics.KompicsEvent;
import se.sics.kompics.PatternExtractor;
import se.sics.kompics.fsm.BaseIdExtractor;
import se.sics.kompics.fsm.FSMBasicStateNames;
import se.sics.kompics.fsm.FSMBuilder;
import se.sics.kompics.fsm.FSMEvent;
import se.sics.kompics.fsm.FSMException;
import se.sics.kompics.fsm.FSMExternalState;
import se.sics.kompics.fsm.FSMInternalState;
import se.sics.kompics.fsm.FSMInternalStateBuilder;
import se.sics.kompics.fsm.FSMStateName;
import se.sics.kompics.fsm.MultiFSM;
import se.sics.kompics.fsm.OnFSMExceptionAction;
import se.sics.kompics.fsm.event.FSMWrongState;
import se.sics.kompics.fsm.handler.FSMBasicEventHandler;
import se.sics.kompics.fsm.handler.FSMPatternEventHandler;
import se.sics.kompics.fsm.id.FSMIdentifier;
import se.sics.kompics.fsm.id.FSMIdentifierFactory;
import se.sics.kompics.timer.Timeout;
import se.sics.kompics.util.Identifier;
import se.sics.ktoolbox.util.identifiable.overlay.OverlayId;
import se.sics.silk.r2torrent.event.R2TorrentCtrlEvents;
import se.sics.silk.r2torrent.event.R2TorrentTransferEvents;
import se.sics.silk.r2torrent.util.R2TorrentStatus;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class R2Torrent {

  private static final Logger LOG = LoggerFactory.getLogger(FSM.class);
  public static final String NAME = "dela-r2-torrent-fsm";

  public static enum States implements FSMStateName {

    META_GET,
    DATA_STORAGE,
    HASH,
    UPLOAD,
    TRANSFER,
    CLEAN_META,
    CLEAN_HASH,
    CLEAN_UPLOAD,
    CLEAN_TRANSFER
  }

  public static interface Event extends FSMEvent {

    public Identifier getR2TorrentFSMId();
  }

  public static interface TransferEvent extends Event {
  }

  public static interface CtrlEvent extends Event {
  }

  public static class IS implements FSMInternalState {

    private final FSMIdentifier fsmId;
    private OverlayId torrentId;
    public final ISCtrlEvents ctrl = new ISCtrlEvents();

    public IS(FSMIdentifier fsmId) {
      this.fsmId = fsmId;
    }

    @Override
    public FSMIdentifier getFSMId() {
      return fsmId;
    }

    public void setGetMetaReq(R2TorrentCtrlEvents.MetaGetReq req) {
      this.torrentId = req.torrentId;
      ctrl.getMetaReq = req;
    }

    public void setStopReq(R2TorrentCtrlEvents.Stop req) {
      ctrl.stopReq = req;
    }

    public OverlayId getTorrentId() {
      return torrentId;
    }
  }

  public static class ISCtrlEvents {

    private R2TorrentCtrlEvents.MetaGetReq getMetaReq;
    private R2TorrentCtrlEvents.Stop stopReq;
  }

  public static class ISBuilder implements FSMInternalStateBuilder {

    @Override
    public FSMInternalState newState(FSMIdentifier fsmId) {
      return new IS(fsmId);
    }
  }

  public static class ES implements FSMExternalState {

    public final R2TorrentComp.Ports ports;
    private ComponentProxy proxy;

    public ES(R2TorrentComp.Ports ports) {
      this.ports = ports;
    }

    @Override
    public void setProxy(ComponentProxy proxy) {
      this.proxy = proxy;
    }

    @Override
    public ComponentProxy getProxy() {
      return proxy;
    }
  }

  public static class FSM {

    private static FSMBuilder.StructuralDefinition structuralDef() throws FSMException {
      return FSMBuilder.structuralDef()
        .onStart()
        .nextStates(States.META_GET)
        .buildTransition()
        .onState(States.META_GET)
        .nextStates(States.DATA_STORAGE, States.CLEAN_META)
        .toFinal()
        .buildTransition()
        .onState(States.DATA_STORAGE)
        .nextStates(States.HASH, States.CLEAN_HASH)
        .buildTransition()
        .onState(States.HASH)
        .nextStates(States.TRANSFER, States.CLEAN_HASH, States.CLEAN_META)
        .buildTransition()
        .onState(States.TRANSFER)
        .nextStates(States.CLEAN_HASH)
        .buildTransition()
        .onState(States.CLEAN_HASH)
        .nextStates(States.CLEAN_META)
        .buildTransition()
        .onState(States.CLEAN_META)
        .toFinal()
        .buildTransition();
    }

    private static FSMBuilder.SemanticDefinition semanticDef() throws FSMException {
      return FSMBuilder.semanticDef()
        .defaultFallback(Handlers.basicDefault(), Handlers.patternDefault())
        .negativePort(R2TorrentCtrlPort.class)
        .basicEvent(R2TorrentCtrlEvents.MetaGetReq.class)
        .subscribeOnStart(Handlers.getMetaReq)
        .basicEvent(R2TorrentCtrlEvents.DataStorage.class)
        .subscribe(Handlers.dataStorage, States.DATA_STORAGE)
        .basicEvent(R2TorrentCtrlEvents.Stop.class)
        .subscribe(Handlers.stop1, States.META_GET)
        .subscribe(Handlers.stop2, States.DATA_STORAGE, States.HASH, States.TRANSFER)
        .buildEvents()
        .positivePort(R2TorrentTransferPort.class)
        .basicEvent(R2TorrentTransferEvents.MetaGetSucc.class)
        .subscribe(Handlers.getMetaSucc, States.META_GET)
        .basicEvent(R2TorrentTransferEvents.MetaGetFail.class)
        .subscribe(Handlers.getMetaFail, States.META_GET)
        .basicEvent(R2TorrentTransferEvents.MetaStopAck.class)
        .subscribe(Handlers.cleanMeta, States.CLEAN_META)
        .basicEvent(R2TorrentTransferEvents.HashSucc.class)
        .subscribe(Handlers.hashSucc, States.HASH)
        .basicEvent(R2TorrentTransferEvents.HashFail.class)
        .subscribe(Handlers.hashFail, States.HASH)
        .basicEvent(R2TorrentTransferEvents.HashStopAck.class)
        .subscribe(Handlers.cleanHash, States.CLEAN_HASH)
        .buildEvents();
    }

    static BaseIdExtractor baseIdExtractor = new BaseIdExtractor() {

      @Override
      public Optional<Identifier> fromEvent(KompicsEvent event) throws FSMException {
        if (event instanceof Event) {
          return Optional.of(((Event) event).getR2TorrentFSMId());
        }
        return Optional.empty();
      }
    };

    public static MultiFSM multifsm(FSMIdentifierFactory fsmIdFactory, ES es, OnFSMExceptionAction oexa)
      throws FSMException {
      FSMInternalStateBuilder isb = new ISBuilder();
      return FSMBuilder.multiFSM(fsmIdFactory, NAME, structuralDef(), semanticDef(), es, isb, oexa, baseIdExtractor);
    }
  }

  public static class Handlers {

    static FSMBasicEventHandler basicDefault() {
      return new FSMBasicEventHandler<ES, IS, KompicsEvent>() {
        @Override
        public FSMStateName handle(FSMStateName state, ES es, IS is, KompicsEvent req) {
          if (req instanceof CtrlEvent) {
            es.getProxy().trigger(new FSMWrongState(req), es.ports.ctrl);
          }
          if (req instanceof TransferEvent) {
            if (!(req instanceof Timeout)) {
              es.getProxy().trigger(new FSMWrongState(req), es.ports.transfer);
            }
          }
          if (FSMBasicStateNames.START.equals(state)) {
            return FSMBasicStateNames.FINAL;
          } else {
            return state;
          }
        }
      };
    }

    static FSMPatternEventHandler patternDefault() {
      return new FSMPatternEventHandler<ES, IS, KompicsEvent>() {
        @Override
        public FSMStateName handle(FSMStateName state, ES es, IS is, KompicsEvent req,
          PatternExtractor<Class, KompicsEvent> container) {
          if (FSMBasicStateNames.START.equals(state)) {
            return FSMBasicStateNames.FINAL;
          } else {
            return state;
          }
        }
      };
    }

    //****************************************************CTRL**********************************************************
    static FSMBasicEventHandler stop1 = new FSMBasicEventHandler<ES, IS, R2TorrentCtrlEvents.Stop>() {
      @Override
      public FSMStateName handle(FSMStateName state, ES es, IS is, R2TorrentCtrlEvents.Stop req) {
        is.setStopReq(req);
        R2TorrentTransferEvents.MetaStop r = new R2TorrentTransferEvents.MetaStop(is.getTorrentId());
        sendTransfer(es, r);
        return States.CLEAN_META;
      }
    };

    static FSMBasicEventHandler stop2 = new FSMBasicEventHandler<ES, IS, R2TorrentCtrlEvents.Stop>() {
      @Override
      public FSMStateName handle(FSMStateName state, ES es, IS is, R2TorrentCtrlEvents.Stop req) {
        is.setStopReq(req);
        R2TorrentTransferEvents.HashStop r = new R2TorrentTransferEvents.HashStop(is.getTorrentId());
        sendTransfer(es, r);
        return States.CLEAN_HASH;
      }
    };

    static FSMBasicEventHandler getMetaReq = new FSMBasicEventHandler<ES, IS, R2TorrentCtrlEvents.MetaGetReq>() {
      @Override
      public FSMStateName handle(FSMStateName state, ES es, IS is, R2TorrentCtrlEvents.MetaGetReq req) {
        is.setGetMetaReq(req);
        R2TorrentTransferEvents.MetaGetReq r = new R2TorrentTransferEvents.MetaGetReq(is.getTorrentId());
        sendTransfer(es, r);
        return States.META_GET;
      }
    };

    static FSMBasicEventHandler dataStorage = new FSMBasicEventHandler<ES, IS, R2TorrentCtrlEvents.DataStorage>() {
      @Override
      public FSMStateName handle(FSMStateName state, ES es, IS is, R2TorrentCtrlEvents.DataStorage req) {
        R2TorrentTransferEvents.HashReq r = new R2TorrentTransferEvents.HashReq(is.getTorrentId());
        sendTransfer(es, r);
        return States.HASH;
      }
    };
    //****************************************************TRANSFER******************************************************
    static FSMBasicEventHandler cleanMeta = new FSMBasicEventHandler<ES, IS, R2TorrentTransferEvents.MetaStopAck>() {
      @Override
      public FSMStateName handle(FSMStateName state, ES es, IS is, R2TorrentTransferEvents.MetaStopAck ack) {
        if (is.ctrl.stopReq != null) {
          sendCtrl(es, is.ctrl.stopReq.ack());
        }
        return FSMBasicStateNames.FINAL;
      }
    };

    static FSMBasicEventHandler getMetaSucc = new FSMBasicEventHandler<ES, IS, R2TorrentTransferEvents.MetaGetSucc>() {
      @Override
      public FSMStateName handle(FSMStateName state, ES es, IS is, R2TorrentTransferEvents.MetaGetSucc resp) {
        sendCtrl(es, is.ctrl.getMetaReq.success());
        return States.DATA_STORAGE;
      }
    };

    static FSMBasicEventHandler getMetaFail = new FSMBasicEventHandler<ES, IS, R2TorrentTransferEvents.MetaGetFail>() {
      @Override
      public FSMStateName handle(FSMStateName state, ES es, IS is, R2TorrentTransferEvents.MetaGetFail resp) {
        sendCtrl(es, is.ctrl.getMetaReq.fail());
        return FSMBasicStateNames.FINAL;
      }
    };

    static FSMBasicEventHandler hashSucc = new FSMBasicEventHandler<ES, IS, R2TorrentTransferEvents.HashSucc>() {
      @Override
      public FSMStateName handle(FSMStateName state, ES es, IS is, R2TorrentTransferEvents.HashSucc resp) {
        return States.TRANSFER;
      }
    };

    static FSMBasicEventHandler hashFail = new FSMBasicEventHandler<ES, IS, R2TorrentTransferEvents.HashFail>() {
      @Override
      public FSMStateName handle(FSMStateName state, ES es, IS is, R2TorrentTransferEvents.HashFail resp) {
        R2TorrentCtrlEvents.TorrentBaseInfo ind = new R2TorrentCtrlEvents.TorrentBaseInfo(is.getTorrentId(),
          R2TorrentStatus.ERROR);
        sendCtrl(es, ind);
        R2TorrentTransferEvents.MetaStop r = new R2TorrentTransferEvents.MetaStop(is.getTorrentId());
        sendTransfer(es, r);
        return States.CLEAN_META;
      }
    };

    static FSMBasicEventHandler cleanHash = new FSMBasicEventHandler<ES, IS, R2TorrentTransferEvents.HashStopAck>() {
      @Override
      public FSMStateName handle(FSMStateName state, ES es, IS is, R2TorrentTransferEvents.HashStopAck ack) {
        R2TorrentTransferEvents.MetaStop r = new R2TorrentTransferEvents.MetaStop(is.getTorrentId());
        sendTransfer(es, r);
        return States.CLEAN_META;
      }
    };

    private static void sendTransfer(ES es, TransferEvent e) {
      es.getProxy().trigger(e, es.ports.transfer);
    }

    private static void sendCtrl(ES es, CtrlEvent e) {
      es.getProxy().trigger(e, es.ports.ctrl);
    }
  }
}
