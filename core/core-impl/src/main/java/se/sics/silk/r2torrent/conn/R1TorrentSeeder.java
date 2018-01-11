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
import se.sics.silk.r2torrent.conn.event.R1TorrentSeederEvents;
import se.sics.silk.r2torrent.conn.event.R2NodeSeederEvents;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class R1TorrentSeeder {

  private static final Logger LOG = LoggerFactory.getLogger(FSM.class);
  public static final String NAME = "dela-r1-torrent-seeder-fsm";

  public static enum States implements FSMStateName {

    CONNECT,
    CONNECTED
  }

  public static interface Event1 extends FSMEvent, Identifiable, SilkEvent.TorrentEvent, SilkEvent.NodeEvent {
  }

  public static interface Event2 extends Event1, SilkEvent.FileEvent {
  }

  public static class IS implements FSMInternalState {

    final FSMIdentifier fsmId;
    KAddress seeder;
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

    Map<Identifier, R1TorrentSeederEvents.ConnectReq> reqs = new HashMap<>();

    void connect(R1TorrentSeederEvents.ConnectReq req) {
      reqs.put(new PairIdentifier(req.torrentId, req.fileId), req);
    }

    void connected(Consumer<R1TorrentSeederEvents.Ind> answerR0) {
      reqs.values().stream().forEach((req) -> {
        answerR0.accept(req.success());
      });
    }

    void failed(Consumer<R1TorrentSeederEvents.Ind> answerR0) {
      reqs.values().stream().forEach((req) -> {
        answerR0.accept(req.fail());
      });
    }

    void disconnected(Consumer<R1TorrentSeederEvents.Ind> answerR0) {
      reqs.values().stream().forEach((req) -> {
        answerR0.accept(req.fail());
      });
    }

    void disconnect(R1TorrentSeederEvents.Disconnect req) {
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

    R2TorrentComp.Ports ports;
    ComponentProxy proxy;

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
        .basicEvent(R1TorrentSeederEvents.ConnectReq.class)
        .subscribeOnStart(Handlers.connect0)
        .subscribe(Handlers.connect1, States.CONNECT)
        .subscribe(Handlers.connect2, States.CONNECTED)
        .basicEvent(R1TorrentSeederEvents.Disconnect.class)
        .subscribe(Handlers.disconnect, States.CONNECT, States.CONNECTED)

        .basicEvent(R2NodeSeederEvents.ConnectSucc.class)
        .subscribe(Handlers.connected, States.CONNECT)
        .basicEvent(R2NodeSeederEvents.ConnectFail.class)
        .subscribe(Handlers.failed, States.CONNECT, States.CONNECTED)
        .buildEvents()
        ;
      return def;
    }

    static BaseIdExtractor baseIdExtractor = new BaseIdExtractor() {

      @Override
      public Optional<Identifier> fromEvent(KompicsEvent event) throws FSMException {
        if (event instanceof Event1) {
          Event1 e = (Event1) event;
          return Optional.of(new PairIdentifier(e.torrentId(), e.nodeId()));
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

    static FSMBasicEventHandler connect0 = new FSMBasicEventHandler<ES, IS, R1TorrentSeederEvents.ConnectReq>() {
      @Override
      public FSMStateName handle(FSMStateName state, ES es, IS is, R1TorrentSeederEvents.ConnectReq req) {
        is.seeder = req.seeder;
        is.torrentId = req.torrentId;
        is.reqTracker.connect(req);
        R2NodeSeederEvents.Req r = new R2NodeSeederEvents.ConnectReq(is.torrentId, is.seeder);
        sendR2(es, r);
        return States.CONNECT;
      }
    };

    static FSMBasicEventHandler connect1 = new FSMBasicEventHandler<ES, IS, R1TorrentSeederEvents.ConnectReq>() {
      @Override
      public FSMStateName handle(FSMStateName state, ES es, IS is, R1TorrentSeederEvents.ConnectReq req) {
        is.reqTracker.connect(req);
        return States.CONNECT;
      }
    };

    static FSMBasicEventHandler connect2 = new FSMBasicEventHandler<ES, IS, R1TorrentSeederEvents.ConnectReq>() {
      @Override
      public FSMStateName handle(FSMStateName state, ES es, IS is, R1TorrentSeederEvents.ConnectReq req) {
        is.reqTracker.connect(req);
        sendR0(es, req.success());
        return States.CONNECTED;
      }
    };
    
    static FSMBasicEventHandler disconnect = new FSMBasicEventHandler<ES, IS, R1TorrentSeederEvents.Disconnect>() {
      @Override
      public FSMStateName handle(FSMStateName state, ES es, IS is, R1TorrentSeederEvents.Disconnect req) {
        is.reqTracker.disconnect(req);
        if (is.reqTracker.empty()) {
          R2NodeSeederEvents.Disconnect r = new R2NodeSeederEvents.Disconnect(is.torrentId, is.seeder.getId());
          sendR2(es, r);
          return FSMBasicStateNames.FINAL;
        }
        return state;
      }
    };

    static FSMBasicEventHandler connected = new FSMBasicEventHandler<ES, IS, R2NodeSeederEvents.ConnectSucc>() {
      @Override
      public FSMStateName handle(FSMStateName state, ES es, IS is, R2NodeSeederEvents.ConnectSucc resp) {
        is.reqTracker.connected(sendR0(es));
        return States.CONNECTED;
      }
    };

    static FSMBasicEventHandler failed = new FSMBasicEventHandler<ES, IS, R2NodeSeederEvents.ConnectFail>() {
      @Override
      public FSMStateName handle(FSMStateName state, ES es, IS is, R2NodeSeederEvents.ConnectFail resp) {
        is.reqTracker.failed(sendR0(es));
        return FSMBasicStateNames.FINAL;
      }
    };
  }

  private static void sendR2(ES es, R2NodeSeederEvents.Req event) {
    es.getProxy().trigger(event, es.ports.loopbackSend);
  }

  private static void sendR0(ES es, R1TorrentSeederEvents.Ind event) {
    es.getProxy().trigger(event, es.ports.loopbackSend);
  }
  
  private static Consumer<R1TorrentSeederEvents.Ind> sendR0(ES es) {
    return new Consumer<R1TorrentSeederEvents.Ind>() {
      @Override
      public void accept(R1TorrentSeederEvents.Ind ind) {
        sendR0(es, ind);
      }
    };
  }
}
