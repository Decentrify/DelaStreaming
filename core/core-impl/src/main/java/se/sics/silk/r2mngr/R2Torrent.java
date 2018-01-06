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
package se.sics.silk.r2mngr;

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
import se.sics.silk.r2mngr.event.R2TorrentEvents;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class R2Torrent {

  private static final Logger LOG = LoggerFactory.getLogger(FSM.class);
  public static final String NAME = "dela-r2-torrent-fsm";

  public static enum States implements FSMStateName {

    GET_META,
    SERVE_META,
    HASHING,
    DOWNLOAD,
    UPLOAD,
    STOP
  }

  public interface Event extends FSMEvent {

    public Identifier getR2TorrentFSMId();
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

    public final R2MngrComp.Ports ports;
    private ComponentProxy proxy;

    public ES(R2MngrComp.Ports ports) {
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
        .nextStates(States.GET_META, States.SERVE_META)
        .buildTransition()
        .onState(States.GET_META)
        .nextStates(States.SERVE_META)
        .toFinal()
        .buildTransition()
        .onState(States.SERVE_META)
        .nextStates(States.DOWNLOAD, States.UPLOAD, States.HASHING)
        .toFinal()
        .buildTransition()
        .onState(States.HASHING)
        .nextStates(States.DOWNLOAD, States.UPLOAD)
        .toFinal()
        .buildTransition()
        .onState(States.DOWNLOAD)
        .nextStates(States.DOWNLOAD, States.UPLOAD)
        .toFinal()
        .buildTransition()
        .onState(States.UPLOAD)
        .nextStates(States.UPLOAD)
        .toFinal()
        .buildTransition();
    }

    private static FSMBuilder.SemanticDefinition semanticDef() throws FSMException {
      return FSMBuilder.semanticDef()
        .defaultFallback(Handlers.basicDefault(), Handlers.patternDefault())
        .negativePort(R2TorrentPort.class)
        .basicEvent(R2TorrentEvents.GetMeta.class)
        .subscribeOnStart(Handlers.getMeta)
        .basicEvent(R2TorrentEvents.ServeMeta.class)
        .subscribeOnStart(Handlers.serveMeta)
        .subscribe(Handlers.serveMeta, States.GET_META)
        .basicEvent(R2TorrentEvents.Hashing.class)
        .subscribe(Handlers.hashing, States.SERVE_META)
        .basicEvent(R2TorrentEvents.Download.class)
        .subscribe(Handlers.download, States.SERVE_META, States.HASHING)
        .basicEvent(R2TorrentEvents.Upload.class)
        .subscribe(Handlers.upload, States.SERVE_META, States.HASHING, States.DOWNLOAD)
        .basicEvent(R2TorrentEvents.Stop.class)
        .subscribe(Handlers.stop, States.GET_META, States.SERVE_META, States.HASHING, States.DOWNLOAD, States.UPLOAD)
        .buildEvents()
        ;
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
          if (!(req instanceof FSMWrongState) && !(req instanceof Timeout)) {
            es.getProxy().trigger(new FSMWrongState(req), es.ports.torrent);
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

    static FSMBasicEventHandler getMeta = new FSMBasicEventHandler<ES, IS, R2TorrentEvents.GetMeta>() {
        @Override
        public FSMStateName handle(FSMStateName state, ES es, IS is, R2TorrentEvents.GetMeta event) {
          return States.GET_META;
        }
      };
    
    static FSMBasicEventHandler serveMeta = new FSMBasicEventHandler<ES, IS, R2TorrentEvents.ServeMeta>() {
        @Override
        public FSMStateName handle(FSMStateName state, ES es, IS is, R2TorrentEvents.ServeMeta event) {
          return States.SERVE_META;
        }
      };
    
    static FSMBasicEventHandler hashing = new FSMBasicEventHandler<ES, IS, R2TorrentEvents.Hashing>() {
        @Override
        public FSMStateName handle(FSMStateName state, ES es, IS is, R2TorrentEvents.Hashing event) {
          return States.HASHING;
        }
      };
    
    static FSMBasicEventHandler download = new FSMBasicEventHandler<ES, IS, R2TorrentEvents.Download>() {
        @Override
        public FSMStateName handle(FSMStateName state, ES es, IS is, R2TorrentEvents.Download event) {
          return States.DOWNLOAD;
        }
      };
    
    static FSMBasicEventHandler upload = new FSMBasicEventHandler<ES, IS, R2TorrentEvents.Upload>() {
        @Override
        public FSMStateName handle(FSMStateName state, ES es, IS is, R2TorrentEvents.Upload event) {
          return States.UPLOAD;
        }
      };
    
     static FSMBasicEventHandler stop = new FSMBasicEventHandler<ES, IS, R2TorrentEvents.Stop>() {
        @Override
        public FSMStateName handle(FSMStateName state, ES es, IS is, R2TorrentEvents.Stop event) {
          return FSMBasicStateNames.FINAL;
        }
      };
  }
}
