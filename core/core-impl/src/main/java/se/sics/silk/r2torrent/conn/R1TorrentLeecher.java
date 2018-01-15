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
package se.sics.silk.r2torrent.conn;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.kompics.ComponentProxy;
import se.sics.kompics.KompicsEvent;
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
import se.sics.kompics.fsm.id.FSMIdentifier;
import se.sics.kompics.fsm.id.FSMIdentifierFactory;
import se.sics.kompics.util.Identifiable;
import se.sics.kompics.util.Identifier;
import se.sics.ktoolbox.util.identifiable.basic.PairIdentifier;
import se.sics.ktoolbox.util.identifiable.overlay.OverlayId;
import se.sics.ktoolbox.util.network.KAddress;
import se.sics.silk.DefaultHandlers;
import se.sics.silk.event.SilkEvent;
import se.sics.silk.r2torrent.R2TorrentComp;
import se.sics.silk.r2torrent.R2TorrentPort;
import se.sics.silk.r2torrent.conn.event.R1TorrentLeecherEvents;
import se.sics.silk.r2torrent.conn.event.R2NodeLeecherEvents;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class R1TorrentLeecher {

  private static final Logger LOG = LoggerFactory.getLogger(FSM.class);
  public static final String NAME = "dela-r1-torrent-leecher-fsm";

  public static enum States implements FSMStateName {

    CONNECT,
    CONNECTED
  }
  
  public static Identifier fsmBaseId(OverlayId torrentId, Identifier leecherId) {
    return new PairIdentifier(torrentId, leecherId);
  }

  public static interface Event1 extends FSMEvent, Identifiable, SilkEvent.TorrentEvent, SilkEvent.NodeEvent {
  }

  public static interface Event2 extends FSMEvent, Identifiable, SilkEvent.TorrentEvent, SilkEvent.FileEvent,
    SilkEvent.NodeEvent {
  }

  public static class IS implements FSMInternalState {

    private final FSMIdentifier fsmId;
    KAddress leecher;
    OverlayId torrentId;
    ReqTracker reqTracker = new ReqTracker();

    public IS(FSMIdentifier fsmId) {
      this.fsmId = fsmId;
    }

    @Override
    public FSMIdentifier getFSMId() {
      return fsmId;
    }
  }

  public static class ReqTracker {

    Map<Identifier, R1TorrentLeecherEvents.ConnectReq> reqs = new HashMap<>();

    void connect(R1TorrentLeecherEvents.ConnectReq req) {
      reqs.put(new PairIdentifier(req.torrentId, req.fileId), req);
    }

    void connected(Consumer<R1TorrentLeecherEvents.Ind> answerR0) {
      reqs.values().stream().forEach((req) -> {
        answerR0.accept(req.success());
      });
    }

    void failed(Consumer<R1TorrentLeecherEvents.Ind> answerR0) {
      reqs.values().stream().forEach((req) -> {
        answerR0.accept(req.fail());
      });
    }

    void disconnected(Consumer<R1TorrentLeecherEvents.Ind> answerR0) {
      reqs.values().stream().forEach((req) -> {
        answerR0.accept(req.fail());
      });
    }

    void disconnect(R1TorrentLeecherEvents.Disconnect req) {
      reqs.remove(new PairIdentifier(req.torrentId, req.fileId));
    }

    boolean empty() {
      return reqs.isEmpty();
    }
  }

  public static class ISBuilder implements FSMInternalStateBuilder {

    @Override
    public FSMInternalState newState(FSMIdentifier fsmId) {
      return new IS(fsmId);
    }
  }

  public static class ES implements FSMExternalState {

    private ComponentProxy proxy;
    R2TorrentComp.Ports ports;

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
        .onStart().nextStates(States.CONNECT).buildTransition()
        .onState(States.CONNECT).nextStates(States.CONNECT, States.CONNECTED).toFinal().buildTransition()
        .onState(States.CONNECTED).nextStates(States.CONNECTED).toFinal().buildTransition();
    }

    private static FSMBuilder.SemanticDefinition semanticDef() throws FSMException {
      FSMBuilder.SemanticDefinition def = FSMBuilder.semanticDef()
        .defaultFallback(DefaultHandlers.basicDefault(), DefaultHandlers.patternDefault());
      def = def.positivePort(R2TorrentPort.class)
        .basicEvent(R1TorrentLeecherEvents.ConnectReq.class)
        .subscribeOnStart(Handlers.connect0)
        .subscribe(Handlers.connect1, States.CONNECT)
        .subscribe(Handlers.connect2, States.CONNECTED)
        .basicEvent(R1TorrentLeecherEvents.Disconnect.class)
        .subscribe(Handlers.disconnect, States.CONNECT, States.CONNECTED)
        .basicEvent(R2NodeLeecherEvents.ConnectSucc.class)
        .subscribe(Handlers.connected, States.CONNECT)
        .basicEvent(R2NodeLeecherEvents.ConnectFail.class)
        .subscribe(Handlers.failed, States.CONNECT, States.CONNECTED)
        .buildEvents();
      return def;
    }

    static BaseIdExtractor baseIdExtractor = new BaseIdExtractor() {

      @Override
      public Optional<Identifier> fromEvent(KompicsEvent event) throws FSMException {
        if (event instanceof Event1) {
          Event1 e = (Event1)event;
          return Optional.of(fsmBaseId(e.torrentId(), e.nodeId()));
        } else if(event instanceof Event2) {
          Event2 e = (Event2) event;
          return Optional.of(fsmBaseId(e.torrentId(), e.nodeId()));
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

    static FSMBasicEventHandler connect0 = new FSMBasicEventHandler<ES, IS, R1TorrentLeecherEvents.ConnectReq>() {
      @Override
      public FSMStateName handle(FSMStateName state, ES es, IS is, R1TorrentLeecherEvents.ConnectReq req) {
        is.leecher = req.node;
        is.torrentId = req.torrentId;
        is.reqTracker.connect(req);
        R2NodeLeecherEvents.Req r = new R2NodeLeecherEvents.ConnectReq(is.torrentId, is.leecher.getId());
        sendR2(es, r);
        return States.CONNECT;
      }
    };

    static FSMBasicEventHandler connect1 = new FSMBasicEventHandler<ES, IS, R1TorrentLeecherEvents.ConnectReq>() {
      @Override
      public FSMStateName handle(FSMStateName state, ES es, IS is, R1TorrentLeecherEvents.ConnectReq req) {
        is.reqTracker.connect(req);
        return States.CONNECT;
      }
    };

    static FSMBasicEventHandler connect2 = new FSMBasicEventHandler<ES, IS, R1TorrentLeecherEvents.ConnectReq>() {
      @Override
      public FSMStateName handle(FSMStateName state, ES es, IS is, R1TorrentLeecherEvents.ConnectReq req) {
        is.reqTracker.connect(req);
        sendR0(es, req.success());
        return States.CONNECTED;
      }
    };

    static FSMBasicEventHandler disconnect = new FSMBasicEventHandler<ES, IS, R1TorrentLeecherEvents.Disconnect>() {
      @Override
      public FSMStateName handle(FSMStateName state, ES es, IS is, R1TorrentLeecherEvents.Disconnect req) {
        is.reqTracker.disconnect(req);
        if (is.reqTracker.empty()) {
          R2NodeLeecherEvents.Disconnect r = new R2NodeLeecherEvents.Disconnect(is.torrentId, is.leecher.getId());
          sendR2(es, r);
          return FSMBasicStateNames.FINAL;
        }
        return state;
      }
    };

    static FSMBasicEventHandler connected = new FSMBasicEventHandler<ES, IS, R2NodeLeecherEvents.ConnectSucc>() {
      @Override
      public FSMStateName handle(FSMStateName state, ES es, IS is, R2NodeLeecherEvents.ConnectSucc resp) {
        is.reqTracker.connected(sendR0(es));
        return States.CONNECTED;
      }
    };

    static FSMBasicEventHandler failed = new FSMBasicEventHandler<ES, IS, R2NodeLeecherEvents.ConnectFail>() {
      @Override
      public FSMStateName handle(FSMStateName state, ES es, IS is, R2NodeLeecherEvents.ConnectFail resp) {
        is.reqTracker.failed(sendR0(es));
        return FSMBasicStateNames.FINAL;
      }
    };

    private static void sendR2(ES es, R2NodeLeecherEvents.Req event) {
      es.getProxy().trigger(event, es.ports.loopbackSend);
    }

    private static void sendR0(ES es, R1TorrentLeecherEvents.Ind event) {
      es.getProxy().trigger(event, es.ports.loopbackSend);
    }

    private static Consumer<R1TorrentLeecherEvents.Ind> sendR0(ES es) {
      return new Consumer<R1TorrentLeecherEvents.Ind>() {
        @Override
        public void accept(R1TorrentLeecherEvents.Ind ind) {
          sendR0(es, ind);
        }
      };
    }
  }
}
