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
import java.util.UUID;
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
import se.sics.kompics.fsm.handler.FSMBasicEventHandler;
import se.sics.kompics.fsm.handler.FSMPatternEventHandler;
import se.sics.kompics.fsm.id.FSMIdentifier;
import se.sics.kompics.fsm.id.FSMIdentifierFactory;
import se.sics.kompics.network.Network;
import se.sics.kompics.timer.CancelPeriodicTimeout;
import se.sics.kompics.timer.SchedulePeriodicTimeout;
import se.sics.kompics.timer.Timer;
import se.sics.kompics.util.Identifiable;
import se.sics.kompics.util.Identifier;
import se.sics.ktoolbox.util.identifiable.overlay.OverlayId;
import se.sics.ktoolbox.util.network.KAddress;
import se.sics.ktoolbox.util.network.KContentMsg;
import se.sics.ktoolbox.util.network.basic.BasicContentMsg;
import se.sics.nutil.network.bestEffort.event.BestEffortMsg;
import se.sics.silk.DefaultHandlers;
import se.sics.silk.event.SilkEvent;
import se.sics.silk.r2torrent.R2TorrentComp;
import se.sics.silk.r2torrent.R2TorrentPort;
import se.sics.silk.r2torrent.conn.event.R2NodeLeecherEvents;
import se.sics.silk.r2torrent.conn.event.R2NodeLeecherTimeout;
import se.sics.silk.r2torrent.conn.msg.R2NodeConnMsgs;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class R2NodeLeecher {

  private static final Logger LOG = LoggerFactory.getLogger(R2NodeLeecher.class);
  public static final String NAME = "dela-r2-node-leecher-fsm";

  public static enum States implements FSMStateName {

    CONNECTED
  }

  public static class HardCodedConfig {
    public static final long pingTimerPeriod = 1000;
    public static final int deadPings = 5;
    public static final int MAX_TORRENTS_PER_LEECHER = 10;
  }
  
  public static interface Msg extends FSMEvent, Identifiable {
  }
  
  public static interface Event extends FSMEvent, Identifiable, SilkEvent.NodeEvent, SilkEvent.TorrentEvent {
  }
  
  public static interface Timeout extends FSMEvent, SilkEvent.NodeEvent {
  }
  
  static BaseIdExtractor baseIdExtractor = new BaseIdExtractor() {

      @Override
      public Optional<Identifier> fromEvent(KompicsEvent event) throws FSMException {
        if (event instanceof Event) {
          return Optional.of(((Event)event).nodeId());
        } else if (event instanceof Timeout) {
          return Optional.of(((Timeout)event).nodeId());
        } else if (event instanceof BasicContentMsg) {
          BasicContentMsg msg = (BasicContentMsg) event;

          Object content = msg.getContent();
          if (content instanceof Msg) {
            return Optional.of(msg.getSource().getId());
          } else if(content instanceof BestEffortMsg.Timeout) {
            Object wrappedContent = ((BestEffortMsg.Timeout) content).content;
            if(wrappedContent instanceof Msg) {
              return Optional.of(msg.getSource().getId());
            }
          }
        }
        return Optional.empty();
      }
    };

  public static class IS implements FSMInternalState {

    private final FSMIdentifier fsmId;
    KAddress leecherAdr;
    R2NodeConnMsgs.ConnectReq req;
    final TorrentReqTracker torrentMngr = new TorrentReqTracker();
    final PingTracker pingTracker = new PingTracker();
    private UUID connPingTimer;

    public IS(FSMIdentifier fsmId) {
      this.fsmId = fsmId;
    }

    @Override
    public FSMIdentifier getFSMId() {
      return fsmId;
    }
  }

  public static class PingTracker {

    private int missedPings = 0;

    public void ping() {
      missedPings = 0;
    }

    public void timerPing() {
      missedPings++;
    }

    public boolean healthy() {
      return missedPings < HardCodedConfig.deadPings;
    }
  }
  
  public static class TorrentReqTracker {

    private final Map<OverlayId, Torrent> torrents = new HashMap<>();

    public R2NodeLeecherEvents.ConnectInd connect(R2NodeLeecherEvents.ConnectReq req) {
      if (torrents.size() < HardCodedConfig.MAX_TORRENTS_PER_LEECHER) {
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
        t.connFail(disc);
      }
      torrents.clear();
    }
  }

  public static class Torrent {

    public final R2NodeLeecherEvents.ConnectReq req;

    public Torrent(R2NodeLeecherEvents.ConnectReq req) {
      this.req = req;
    }
    
    public void connFail(Consumer disc) {
      disc.accept(req.reject());
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
    public final R2TorrentComp.Ports ports;
    public final KAddress selfAdr;

    public ES(R2TorrentComp.Ports ports, KAddress selfAdr) {
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
        .defaultFallback(DefaultHandlers.basicDefault(), DefaultHandlers.patternDefault())
        .positivePort(R2TorrentPort.class)
        .basicEvent(R2NodeLeecherEvents.ConnectReq.class)
        .subscribe(Handlers.locConnReq, States.CONNECTED)
        .basicEvent(R2NodeLeecherEvents.Disconnect.class)
        .subscribe(Handlers.locDisc, States.CONNECTED)
        .buildEvents()
        .positivePort(Network.class)
        .patternEvent(R2NodeConnMsgs.ConnectReq.class, BasicContentMsg.class)
        .subscribeOnStart(Handlers.netConnReq1)
        .subscribe(Handlers.netConnReq2, States.CONNECTED)
        .patternEvent(R2NodeConnMsgs.Disconnect.class, BasicContentMsg.class)
        .subscribe(Handlers.netDiscReq, States.CONNECTED)
        .patternEvent(R2NodeConnMsgs.Ping.class, BasicContentMsg.class)
        .subscribe(Handlers.netPingReq, States.CONNECTED)
        .buildEvents()
        .positivePort(Timer.class)
        .basicEvent(R2NodeLeecherTimeout.class)
        .subscribe(Handlers.timerPing, States.CONNECTED)
        .buildEvents();
    }

    public static MultiFSM multifsm(FSMIdentifierFactory fsmIdFactory, ES es, OnFSMExceptionAction oexa)
      throws FSMException {
      FSMInternalStateBuilder isb = new ISBuilder();
      return FSMBuilder.multiFSM(fsmIdFactory, NAME, structuralDef(), semanticDef(), es, isb, oexa, baseIdExtractor);
    }
  }

  public static class Handlers {

    static FSMPatternEventHandler netConnReq1 = new FSMPatternEventHandler<ES, IS, R2NodeConnMsgs.ConnectReq>() {

      @Override
      public FSMStateName handle(FSMStateName state, ES es, IS is, R2NodeConnMsgs.ConnectReq payload,
        PatternExtractor<Class, R2NodeConnMsgs.ConnectReq> container) throws FSMException {
        BasicContentMsg msg = (BasicContentMsg) container;
        is.leecherAdr = msg.getSource();
        is.req = payload;
        scheduleConnPing(es, is);
        answerNet(es, container, payload.accept());
        return States.CONNECTED;
      }
    };

    static FSMPatternEventHandler netConnReq2 = new FSMPatternEventHandler<ES, IS, R2NodeConnMsgs.ConnectReq>() {

      @Override
      public FSMStateName handle(FSMStateName state, ES es, IS is, R2NodeConnMsgs.ConnectReq payload,
        PatternExtractor<Class, R2NodeConnMsgs.ConnectReq> container) throws FSMException {
        answerNet(es, container, payload.accept());
        return States.CONNECTED;
      }
    };

    static FSMPatternEventHandler netDiscReq = new FSMPatternEventHandler<ES, IS, R2NodeConnMsgs.Disconnect>() {

      @Override
      public FSMStateName handle(FSMStateName state, ES es, IS is, R2NodeConnMsgs.Disconnect payload,
        PatternExtractor<Class, R2NodeConnMsgs.Disconnect> container) throws FSMException {
        is.torrentMngr.disconnectAll(sendR1(es));
        cancelConnPing(es, is);
        return FSMBasicStateNames.FINAL;
      }
    };
    
    static FSMPatternEventHandler netPingReq = new FSMPatternEventHandler<ES, IS, R2NodeConnMsgs.Ping>() {

      @Override
      public FSMStateName handle(FSMStateName state, ES es, IS is, R2NodeConnMsgs.Ping payload,
        PatternExtractor<Class, R2NodeConnMsgs.Ping> container) throws FSMException {
        is.pingTracker.ping();
        answerNet(es, container, payload.ack());
        return States.CONNECTED;
      }
    };
    
    static FSMBasicEventHandler timerPing = new FSMBasicEventHandler<ES, IS, R2NodeLeecherTimeout>() {
      @Override
      public FSMStateName handle(FSMStateName state, ES es, IS is, R2NodeLeecherTimeout req) {
        if(!is.pingTracker.healthy()) {
          is.torrentMngr.disconnectAll(sendR1(es));
          cancelConnPing(es, is);
          return FSMBasicStateNames.FINAL;
        }
        is.pingTracker.timerPing();
        return States.CONNECTED;
      }
    };

    static FSMBasicEventHandler locConnReq = new FSMBasicEventHandler<ES, IS, R2NodeLeecherEvents.ConnectReq>() {
      @Override
      public FSMStateName handle(FSMStateName state, ES es, IS is, R2NodeLeecherEvents.ConnectReq req) {
        R2NodeLeecherEvents.ConnectInd resp = is.torrentMngr.connect(req);
        localSend(es, resp);
        return States.CONNECTED;
      }
    };
    
    static FSMBasicEventHandler locDisc = new FSMBasicEventHandler<ES, IS, R2NodeLeecherEvents.Disconnect>() {
      @Override
      public FSMStateName handle(FSMStateName state, ES es, IS is, R2NodeLeecherEvents.Disconnect req) {
        is.torrentMngr.disconnect(req.torrentId);
        return States.CONNECTED;
      }
    };

    private static <C extends KompicsEvent & Identifiable> void answerNet(ES es, PatternExtractor container, C content) {
      BasicContentMsg msg = (BasicContentMsg) container;
      KContentMsg resp = msg.answer(content);
      es.getProxy().trigger(resp, es.ports.network);
    }
    
    private static Consumer<R2NodeLeecherEvents.Ind> sendR1(ES es) {
      return (R2NodeLeecherEvents.Ind e) -> localSend(es, e);
    };
    
    private static void localSend(ES es, R2NodeLeecherEvents.Ind event) {
      es.getProxy().trigger(event, es.ports.loopbackSend);
    }
    
    private static void scheduleConnPing(ES es, IS is) {
      SchedulePeriodicTimeout spt = new SchedulePeriodicTimeout(HardCodedConfig.pingTimerPeriod, HardCodedConfig.pingTimerPeriod);
      R2NodeLeecherTimeout rt = new R2NodeLeecherTimeout(spt, is.leecherAdr.getId());
      is.connPingTimer = rt.getTimeoutId();
      spt.setTimeoutEvent(rt);
      es.getProxy().trigger(spt, es.ports.timer);
    }

    private static void cancelConnPing(ES es, IS is) {
      if (is.connPingTimer != null) {
        CancelPeriodicTimeout cpt = new CancelPeriodicTimeout(is.connPingTimer);
        es.getProxy().trigger(cpt, es.ports.timer);
        is.connPingTimer = null;
      }
    }
  }
}
