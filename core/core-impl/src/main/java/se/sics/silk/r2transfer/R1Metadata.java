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
package se.sics.silk.r2transfer;

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
import se.sics.kompics.util.Identifier;
import se.sics.silk.r2torrent.R2TorrentTransferPort;
import se.sics.silk.r2torrent.event.R2TorrentTransferEvents;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class R1Metadata {

  private static final Logger LOG = LoggerFactory.getLogger(FSM.class);
  public static final String NAME = "dela-r1-metadata-fsm";

  public static enum States implements FSMStateName {

    GET,
    SERVE
  }

  public static interface Event extends FSMEvent {

    public Identifier getR1MetadataFSMId();
  }

  public static interface TransferEvent extends Event {
  }

  public static class IS implements FSMInternalState {

    private final FSMIdentifier fsmId;

    public IS(FSMIdentifier fsmId) {
      this.fsmId = fsmId;
    }

    @Override
    public FSMIdentifier getFSMId() {
      return fsmId;
    }
  }

  public static class ISBuilder implements FSMInternalStateBuilder {

    @Override
    public FSMInternalState newState(FSMIdentifier fsmId) {
      return new IS(fsmId);
    }
  }

  public static class ES implements FSMExternalState {

    private R2TransferComp.Ports ports;
    private ComponentProxy proxy;

    public ES(R2TransferComp.Ports ports) {
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
        .nextStates(States.GET, States.SERVE)
        .toFinal()
        .buildTransition()
        .onState(States.GET)
        .nextStates(States.SERVE)
        .toFinal()
        .buildTransition()
        .onState(States.SERVE)
        .toFinal()
        .buildTransition();
    }

    private static FSMBuilder.SemanticDefinition semanticDef() throws FSMException {
      return FSMBuilder.semanticDef()
        .defaultFallback(Handlers.basicDefault(), Handlers.patternDefault())
        .negativePort(R2TorrentTransferPort.class)
        .basicEvent(R2TorrentTransferEvents.MetaGetReq.class)
        .subscribeOnStart(Handlers.metadataGet)
        .basicEvent(R2TorrentTransferEvents.MetaServeReq.class)
        .subscribeOnStart(Handlers.metadataServe)
        .basicEvent(R2TorrentTransferEvents.MetaStop.class)
        .subscribeOnStart(Handlers.stop1)
        .subscribe(Handlers.stop2, States.GET, States.SERVE)
        .buildEvents();
    }

    static BaseIdExtractor baseIdExtractor = new BaseIdExtractor() {

      @Override
      public Optional<Identifier> fromEvent(KompicsEvent event) throws FSMException {
        if (event instanceof Event) {
          return Optional.of(((Event) event).getR1MetadataFSMId());
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
          if (req instanceof TransferEvent) {
            es.getProxy().trigger(new FSMWrongState(req), es.ports.transfer);
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

    static FSMBasicEventHandler metadataGet = new FSMBasicEventHandler<ES, IS, R2TorrentTransferEvents.MetaGetReq>() {
      @Override
      public FSMStateName handle(FSMStateName state, ES es, IS is, R2TorrentTransferEvents.MetaGetReq req) {
        sendTransfer(es, req.success());
        return States.SERVE;
      }
    };
    
    static FSMBasicEventHandler metadataServe = new FSMBasicEventHandler<ES, IS, R2TorrentTransferEvents.MetaServeReq>() {
      @Override
      public FSMStateName handle(FSMStateName state, ES es, IS is, R2TorrentTransferEvents.MetaServeReq req) {
        sendTransfer(es, req.success());
        return States.SERVE;
      }
    };

    static FSMBasicEventHandler stop1 = new FSMBasicEventHandler<ES, IS, R2TorrentTransferEvents.MetaStop>() {
      @Override
      public FSMStateName handle(FSMStateName state, ES es, IS is, R2TorrentTransferEvents.MetaStop req) {
        sendTransfer(es, req.ack());
        return FSMBasicStateNames.FINAL;
      }
    };
    
    static FSMBasicEventHandler stop2 = new FSMBasicEventHandler<ES, IS, R2TorrentTransferEvents.MetaStop>() {
      @Override
      public FSMStateName handle(FSMStateName state, ES es, IS is, R2TorrentTransferEvents.MetaStop req) {
        sendTransfer(es, req.ack());
        return FSMBasicStateNames.FINAL;
      }
    };

    private static void sendTransfer(ES es, TransferEvent e) {
      es.getProxy().trigger(e, es.ports.transfer);
    }
  }
}
