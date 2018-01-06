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

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;
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
import se.sics.kompics.network.Network;
import se.sics.kompics.util.Identifiable;
import se.sics.kompics.util.Identifier;
import se.sics.ktoolbox.util.identifiable.overlay.OverlayId;
import se.sics.ktoolbox.util.network.KAddress;
import se.sics.ktoolbox.util.network.KContentMsg;
import se.sics.ktoolbox.util.network.basic.BasicContentMsg;
import se.sics.silk.r2mngr.event.R2ConnLeecherEvents;
import se.sics.silk.r2mngr.msg.R2ConnMsgs;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class R2ConnLeecher {

  private static final Logger LOG = LoggerFactory.getLogger(R2ConnLeecher.class);
  public static final String NAME = "dela-rs-conn-leecher-fsm";

  public static enum States implements FSMStateName {

    CONNECTED
  }

  public interface Msg extends FSMEvent, Identifiable {
  }
  
  public interface Event extends FSMEvent, Identifiable {

    public Identifier getConnLeecherFSMId();
  }
  
  static BaseIdExtractor baseIdExtractor = new BaseIdExtractor() {

      @Override
      public Optional<Identifier> fromEvent(KompicsEvent event) throws FSMException {
        if (event instanceof Event) {
          return Optional.of(((Event) event).getConnLeecherFSMId());
        } else if (event instanceof BasicContentMsg) {
          BasicContentMsg msg = (BasicContentMsg) event;
          
          if(msg.getContent() instanceof Msg) {
            return Optional.of(msg.getSource().getId());
          }
        }
        return Optional.empty();
      }
    };

  public static class IS implements FSMInternalState {

    private final FSMIdentifier fsmId;
    private KAddress leecherAdr;
    private R2ConnMsgs.ConnectReq req;
    public final TorrentMngr torrentMngr = new TorrentMngr();
    

    public IS(FSMIdentifier fsmId) {
      this.fsmId = fsmId;
    }

    @Override
    public FSMIdentifier getFSMId() {
      return fsmId;
    }

    public KAddress getLeecherAdr() {
      return leecherAdr;
    }

    public void setLeecherAdr(KAddress leecherAdr) {
      this.leecherAdr = leecherAdr;
    }

    public R2ConnMsgs.ConnectReq getReq() {
      return req;
    }

    public void setReq(R2ConnMsgs.ConnectReq req) {
      this.req = req;
    }
  }

  public static class PingTracker {

    private int missedPings = 0;

    public void ping() {
      missedPings = 0;
    }

    public void expectedPing() {
      missedPings++;
    }

    public boolean healthy() {
      return missedPings < 5;
    }
  }
  
  public static class TorrentMngr {

    private static final int MAX_TORRENTS_PER_LEECHER = 10;
    private final Map<OverlayId, Torrent> torrents = new HashMap<>();

    public R2ConnLeecherEvents.ConnectInd request(R2ConnLeecherEvents.ConnectReq req) {
      if (torrents.size() <= MAX_TORRENTS_PER_LEECHER) {
        torrents.put(req.torrentId, new Torrent(req));
        return req.accept();
      } else {
        return req.reject();
      }
    }
    
    public void disconnect(OverlayId torrentId) {
      torrents.remove(torrentId);
    }
    
    public void disconnectAll(Consumer disc) {
      for(Torrent t: torrents.values()) {
        t.disconnect(disc);
      }
    }
  }

  public static class Torrent {

    public final R2ConnLeecherEvents.ConnectReq req;

    public Torrent(R2ConnLeecherEvents.ConnectReq req) {
      this.req = req;
    }
    
    public void disconnect(Consumer disc) {
      disc.accept(req.disconnect());
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
    public final R2MngrComp.Ports ports;
    public final KAddress selfAdr;

    public ES(R2MngrComp.Ports ports, KAddress selfAdr) {
      this.ports = ports;
      this.selfAdr = selfAdr;
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
        .nextStates(States.CONNECTED)
        .toFinal()
        .buildTransition()
        .onState(States.CONNECTED)
        .nextStates(States.CONNECTED)
        .toFinal()
        .buildTransition();
    }

    private static FSMBuilder.SemanticDefinition semanticDef() throws FSMException {
      return FSMBuilder.semanticDef()
        .defaultFallback(Handlers.basicDefault(), Handlers.patternDefault())
        .negativePort(R2ConnLeecherPort.class)
        .basicEvent(R2ConnLeecherEvents.ConnectReq.class)
        .subscribe(Handlers.locConnReq, States.CONNECTED)
        .basicEvent(R2ConnLeecherEvents.Disconnect.class)
        .subscribe(Handlers.locDisc, States.CONNECTED)
        .buildEvents()
        .positivePort(Network.class)
        .patternEvent(R2ConnMsgs.ConnectReq.class, BasicContentMsg.class)
        .subscribeOnStart(Handlers.netConnReq1)
        .subscribe(Handlers.netConnReq2, States.CONNECTED)
        .patternEvent(R2ConnMsgs.Disconnect.class, BasicContentMsg.class)
        .subscribe(Handlers.netDiscReq, States.CONNECTED)
        .patternEvent(R2ConnMsgs.Ping.class, BasicContentMsg.class)
        .subscribe(Handlers.netPingReq, States.CONNECTED)
        .buildEvents();
    }

    public static MultiFSM multifsm(FSMIdentifierFactory fsmIdFactory, ES es, OnFSMExceptionAction oexa)
      throws FSMException {
      FSMInternalStateBuilder isb = new ISBuilder();
      return FSMBuilder.multiFSM(fsmIdFactory, NAME, structuralDef(), semanticDef(), es, isb, oexa, baseIdExtractor);
    }
  }

  public static class Handlers {

    static FSMBasicEventHandler basicDefault() {
      return new FSMBasicEventHandler<ES, IS, Event>() {
        @Override
        public FSMStateName handle(FSMStateName state, ES es, IS is, Event req) {
          if (!(req instanceof FSMWrongState)) {
            es.getProxy().trigger(new FSMWrongState(req), es.ports.leechers);
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

    static FSMPatternEventHandler netConnReq1 = new FSMPatternEventHandler<ES, IS, R2ConnMsgs.ConnectReq>() {

      @Override
      public FSMStateName handle(FSMStateName state, ES es, IS is, R2ConnMsgs.ConnectReq payload,
        PatternExtractor<Class, R2ConnMsgs.ConnectReq> container) throws FSMException {
        BasicContentMsg msg = (BasicContentMsg) container;
        KAddress leecherAdr = msg.getSource();
        is.setLeecherAdr(leecherAdr);
        is.setReq(payload);
        answerNet(es, container, payload.accept());
        return States.CONNECTED;
      }
    };

    static FSMPatternEventHandler netConnReq2 = new FSMPatternEventHandler<ES, IS, R2ConnMsgs.ConnectReq>() {

      @Override
      public FSMStateName handle(FSMStateName state, ES es, IS is, R2ConnMsgs.ConnectReq payload,
        PatternExtractor<Class, R2ConnMsgs.ConnectReq> container) throws FSMException {
        answerNet(es, container, payload.accept());
        return States.CONNECTED;
      }
    };

    static FSMPatternEventHandler netDiscReq = new FSMPatternEventHandler<ES, IS, R2ConnMsgs.Disconnect>() {

      @Override
      public FSMStateName handle(FSMStateName state, ES es, IS is, R2ConnMsgs.Disconnect payload,
        PatternExtractor<Class, R2ConnMsgs.Disconnect> container) throws FSMException {
        answerNet(es, container, payload.ack());
        is.torrentMngr.disconnectAll(answerConnConsumer(es));
        return FSMBasicStateNames.FINAL;
      }
    };
    
    static FSMPatternEventHandler netPingReq = new FSMPatternEventHandler<ES, IS, R2ConnMsgs.Ping>() {

      @Override
      public FSMStateName handle(FSMStateName state, ES es, IS is, R2ConnMsgs.Ping payload,
        PatternExtractor<Class, R2ConnMsgs.Ping> container) throws FSMException {
        answerNet(es, container, payload.ack());
        return States.CONNECTED;
      }
    };

    static FSMBasicEventHandler locConnReq = new FSMBasicEventHandler<ES, IS, R2ConnLeecherEvents.ConnectReq>() {
      @Override
      public FSMStateName handle(FSMStateName state, ES es, IS is, R2ConnLeecherEvents.ConnectReq req) {
        R2ConnLeecherEvents.ConnectInd resp = is.torrentMngr.request(req);
        answerConn(es, resp);
        return States.CONNECTED;
      }
    };
    
    static FSMBasicEventHandler locDisc = new FSMBasicEventHandler<ES, IS, R2ConnLeecherEvents.Disconnect>() {
      @Override
      public FSMStateName handle(FSMStateName state, ES es, IS is, R2ConnLeecherEvents.Disconnect req) {
        is.torrentMngr.disconnect(req.torrentId);
        return States.CONNECTED;
      }
    };

    private static void answerConn(ES es, KompicsEvent content) {
      es.getProxy().trigger(content, es.ports.leechers);
    }

    private static <C extends KompicsEvent & Identifiable> void answerNet(ES es, PatternExtractor container, C content) {
      BasicContentMsg msg = (BasicContentMsg) container;
      KContentMsg resp = msg.answer(content);
      es.getProxy().trigger(resp, es.ports.network);
    }
    
    private static Consumer<KompicsEvent> answerConnConsumer(ES es) {
      return (KompicsEvent e) -> answerConn(es, e);
    };
  }
}
