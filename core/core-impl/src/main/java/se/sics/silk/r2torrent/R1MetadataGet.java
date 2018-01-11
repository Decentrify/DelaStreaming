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

import se.sics.silk.r2torrent.conn.R2NodeSeeder;
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
import se.sics.kompics.fsm.handler.FSMBasicEventHandler;
import se.sics.kompics.fsm.handler.FSMPatternEventHandler;
import se.sics.kompics.fsm.id.FSMIdentifier;
import se.sics.kompics.fsm.id.FSMIdentifierFactory;
import se.sics.kompics.util.Identifier;
import se.sics.ktoolbox.util.identifiable.basic.PairIdentifier;
import se.sics.ktoolbox.util.identifiable.overlay.OverlayId;
import se.sics.ktoolbox.util.network.KAddress;
import se.sics.silk.r2torrent.event.R1MetadataEvents;
import se.sics.silk.r2torrent.conn.event.R2NodeSeederEvents;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class R1MetadataGet {

  private static final Logger LOG = LoggerFactory.getLogger(FSM.class);
  public static final String NAME = "dela-r1-torrent-metadata-fsm";

  public static Identifier fsmBaseId(OverlayId torrentId, Identifier fileId, Identifier seederId) {
    return new PairIdentifier(new PairIdentifier(torrentId, fileId), seederId);
  }
  
  public static enum States implements FSMStateName {
    CONNECT
  }

  public static interface Event extends FSMEvent {

    public Identifier getR1MetadataFSMId();
  }

  public static interface TorrentEvent extends Event {
  }
  
  public static interface ConnEvent extends Event {
  }

  public static class IS implements FSMInternalState {

    FSMIdentifier fsmId;
    OverlayId torrentId;
    KAddress seeder;
    R1MetadataEvents.MetaGetReq metaGetReq;
    
    public IS(FSMIdentifier fsmId) {
      this.fsmId = fsmId;
    }

    @Override
    public FSMIdentifier getFSMId() {
      return fsmId;
    }
    
    public void setMetaGetReq(R1MetadataEvents.MetaGetReq req) {
      torrentId = req.torrentId;
      seeder = req.seeder;
      metaGetReq = req;
    }
  }
  
  public static class ISBuilder implements FSMInternalStateBuilder {

    @Override
    public FSMInternalState newState(FSMIdentifier fsmId) {
      return new IS(fsmId);
    }
  }

  public static class ES implements FSMExternalState {

    private R2TorrentComp.Ports ports;
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
        .nextStates(States.CONNECT)
        .toFinal()
        .buildTransition()
        .onState(States.CONNECT)
        .toFinal()
        .buildTransition();
    }

    private static FSMBuilder.SemanticDefinition semanticDef() throws FSMException {
      return FSMBuilder.semanticDef()
        .defaultFallback(Handlers.basicDefault(), Handlers.patternDefault())
        .positivePort(R2TorrentPort.class)
        .basicEvent(R1MetadataEvents.MetaGetReq.class)
        .subscribeOnStart(Handlers.metadataGet)
        .basicEvent(R2NodeSeederEvents.ConnectSucc.class)
        .subscribe(Handlers.connSucc, States.CONNECT)
        .basicEvent(R2NodeSeederEvents.ConnectFail.class)
        .subscribe(Handlers.connFail, States.CONNECT)
        .basicEvent(R1MetadataEvents.MetaStop.class)
        .subscribeOnStart(Handlers.stop1)
        .subscribe(Handlers.stop2, States.CONNECT)
        .buildEvents()
        ;
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

    static FSMBasicEventHandler metadataGet = new FSMBasicEventHandler<ES, IS, R1MetadataEvents.MetaGetReq>() {
      @Override
      public FSMStateName handle(FSMStateName state, ES es, IS is, R1MetadataEvents.MetaGetReq req) {
        is.setMetaGetReq(req);
        R2NodeSeederEvents.ConnectReq r = new R2NodeSeederEvents.ConnectReq(is.torrentId, is.seeder);
        sendNodeSeeder(es, r);
        return States.CONNECT;
      }
    };
    
    static FSMBasicEventHandler connSucc = new FSMBasicEventHandler<ES, IS, R2NodeSeederEvents.ConnectSucc>() {
      @Override
      public FSMStateName handle(FSMStateName state, ES es, IS is, R2NodeSeederEvents.ConnectSucc event) throws
        FSMException {
        sendTorrent(es, is.metaGetReq.success());
        return FSMBasicStateNames.FINAL;
      }
    };
    
    static FSMBasicEventHandler connFail = new FSMBasicEventHandler<ES, IS, R2NodeSeederEvents.ConnectSucc>() {
      @Override
      public FSMStateName handle(FSMStateName state, ES es, IS is, R2NodeSeederEvents.ConnectSucc event) throws
        FSMException {
        sendTorrent(es, is.metaGetReq.fail());
        return FSMBasicStateNames.FINAL;
      }
    };
    
    static FSMBasicEventHandler stop1 = new FSMBasicEventHandler<ES, IS, R1MetadataEvents.MetaStop>() {
      @Override
      public FSMStateName handle(FSMStateName state, ES es, IS is, R1MetadataEvents.MetaStop req) {
        sendTorrent(es, req.ack());
        return FSMBasicStateNames.FINAL;
      }
    };
    
    static FSMBasicEventHandler stop2 = new FSMBasicEventHandler<ES, IS, R1MetadataEvents.MetaStop>() {
      @Override
      public FSMStateName handle(FSMStateName state, ES es, IS is, R1MetadataEvents.MetaStop req) {
        sendTorrent(es, req.ack());
        return FSMBasicStateNames.FINAL;
      }
    };

    private static void sendTorrent(ES es, R2Torrent.MetadataEvent e) {
      es.getProxy().trigger(e, es.ports.loopbackSend);
    }
    
    private static void sendNodeSeeder(ES es, R2NodeSeeder.Event e) {
      es.getProxy().trigger(e, es.ports.loopbackSend);
    }
  }
}
