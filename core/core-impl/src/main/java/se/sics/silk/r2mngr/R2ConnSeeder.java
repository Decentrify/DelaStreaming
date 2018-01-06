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
import java.util.UUID;
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
import se.sics.kompics.network.Transport;
import se.sics.kompics.timer.CancelPeriodicTimeout;
import se.sics.kompics.timer.SchedulePeriodicTimeout;
import se.sics.kompics.timer.Timeout;
import se.sics.kompics.timer.Timer;
import se.sics.kompics.util.Identifiable;
import se.sics.kompics.util.Identifier;
import se.sics.ktoolbox.util.identifiable.overlay.OverlayId;
import se.sics.ktoolbox.util.network.KAddress;
import se.sics.ktoolbox.util.network.KContentMsg;
import se.sics.ktoolbox.util.network.KHeader;
import se.sics.ktoolbox.util.network.basic.BasicContentMsg;
import se.sics.ktoolbox.util.network.basic.BasicHeader;
import se.sics.nutil.network.bestEffort.event.BestEffortMsg;
import se.sics.silk.r2mngr.event.R2ConnSeederEvents;
import se.sics.silk.r2mngr.event.R2ConnSeederTimeout;
import se.sics.silk.r2mngr.msg.R2ConnMsgs;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class R2ConnSeeder {

  private static final Logger LOG = LoggerFactory.getLogger(R2ConnSeeder.class);
  public static final String NAME = "dela-r2-conn-seeder-fsm";

  public static enum States implements FSMStateName {

    CONNECTING,
    CONNECTED,
    DISCONNECTING
  }

  public interface Msg extends FSMEvent, Identifiable {
  }

  public interface Event extends FSMEvent, Identifiable {

    public Identifier getConnSeederFSMId();
  }

  public static class ISBuilder implements FSMInternalStateBuilder {

    @Override
    public FSMInternalState newState(FSMIdentifier fsmId) {
      return new IS(fsmId);
    }
  }
  
  static BaseIdExtractor baseIdExtractor = new BaseIdExtractor() {

      @Override
      public Optional<Identifier> fromEvent(KompicsEvent event) throws FSMException {
        if (event instanceof Event) {
          return Optional.of(((Event) event).getConnSeederFSMId());
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

  public static class FSM {

    private static FSMBuilder.StructuralDefinition structuralDef() throws FSMException {
      return FSMBuilder.structuralDef()
        .onStart()
        .nextStates(States.CONNECTING)
        .buildTransition()
        .onState(States.CONNECTING)
        .nextStates(States.CONNECTING, States.CONNECTED, States.DISCONNECTING)
        .toFinal()
        .buildTransition()
        .onState(States.CONNECTED)
        .nextStates(States.CONNECTED, States.DISCONNECTING)
        .buildTransition()
        .onState(States.DISCONNECTING)
        .nextStates(States.DISCONNECTING)
        .toFinal()
        .buildTransition();
    }

    private static FSMBuilder.SemanticDefinition semanticDef() throws FSMException {
      return FSMBuilder.semanticDef()
        .defaultFallback(R2ConnSeeder.Handlers.basicDefault(), R2ConnSeeder.Handlers.patternDefault())
        .negativePort(R2ConnSeederPort.class)
        .basicEvent(R2ConnSeederEvents.ConnectReq.class)
        .subscribeOnStart(Handlers.conn0)
        .subscribe(Handlers.conn1, States.CONNECTING)
        .subscribe(Handlers.conn2, States.CONNECTED)
        .subscribe(Handlers.conn3, States.DISCONNECTING)
        .basicEvent(R2ConnSeederEvents.Disconnect.class)
        .subscribe(Handlers.disc1, States.CONNECTING)
        .subscribe(Handlers.disc2, States.CONNECTED)
        .buildEvents()
        .positivePort(Network.class)
        .patternEvent(BestEffortMsg.Timeout.class, BasicContentMsg.class)
        .subscribe(Handlers.beTout1, States.CONNECTING)
        .subscribe(Handlers.beTout2, States.CONNECTED)
        .patternEvent(R2ConnMsgs.ConnectAcc.class, BasicContentMsg.class)
        .subscribe(Handlers.connAcc, States.CONNECTING)
        .patternEvent(R2ConnMsgs.ConnectRej.class, BasicContentMsg.class)
        .subscribe(Handlers.connRej, States.CONNECTING)
        .patternEvent(R2ConnMsgs.Pong.class, BasicContentMsg.class)
        .subscribe(Handlers.connPong, States.CONNECTED)
        .patternEvent(R2ConnMsgs.DisconnectAck.class, BasicContentMsg.class)
        .subscribe(Handlers.discAck, States.DISCONNECTING)
        .buildEvents()
        .positivePort(Timer.class)
        .basicEvent(R2ConnSeederTimeout.class)
        .subscribe(Handlers.connPing, States.CONNECTED)
        .buildEvents();
    }

    public static MultiFSM multifsm(FSMIdentifierFactory fsmIdFactory, ES es, OnFSMExceptionAction oexa)
      throws FSMException {
      FSMInternalStateBuilder isb = new ISBuilder();
      return FSMBuilder.multiFSM(fsmIdFactory, NAME, structuralDef(), semanticDef(), es, isb, oexa, baseIdExtractor);
    }
  }

  public static class ES implements FSMExternalState {

    private ComponentProxy proxy;
    public final R2MngrComp.Ports ports;
    public final KAddress selfAdr;
    private final int retries;
    private final long retryInterval;

    public ES(R2MngrComp.Ports ports, KAddress selfAdr, int retries, long retryInterval) {
      this.ports = ports;
      this.selfAdr = selfAdr;
      this.retries = retries;
      this.retryInterval = retryInterval;
    }

    @Override
    public void setProxy(ComponentProxy proxy) {
      this.proxy = proxy;
    }

    @Override
    public ComponentProxy getProxy() {
      return proxy;
    }

    public int getRetries() {
      return retries;
    }

    public long getRetryInterval() {
      return retryInterval;
    }
  }

  public static class IS implements FSMInternalState {

    private final FSMIdentifier fsmId;
    private KAddress seederAdr;
    private final Map<OverlayId, R2ConnSeederEvents.ConnectReq> connecting = new HashMap<>();
    Map<OverlayId, R2ConnSeederEvents.ConnectReq> connected = new HashMap<>();
    private final PingTracker pingTracker = new PingTracker();
    private UUID connPingTimer;
    public final long pingTimerPeriod = 1000;

    public IS(FSMIdentifier fsmId) {
      this.fsmId = fsmId;
    }

    @Override
    public FSMIdentifier getFSMId() {
      return fsmId;
    }

    public KAddress getSeederAdr() {
      return seederAdr;
    }

    public void setSeederAdr(KAddress seederAdr) {
      this.seederAdr = seederAdr;
    }

    public Map<OverlayId, R2ConnSeederEvents.ConnectReq> getConnected() {
      return connected;
    }

    public void setConnected(Map<OverlayId, R2ConnSeederEvents.ConnectReq> connected) {
      this.connected = connected;
    }

    public UUID getConnPingTimer() {
      return connPingTimer;
    }

    public void setConnPingTimer(UUID connPingTimer) {
      this.connPingTimer = connPingTimer;
    }
  }

  public static class PingTracker {

    private int missedPings = 0;

    public void ping() {
      missedPings++;
    }

    public void pong() {
      missedPings = 0;
    }

    public boolean healthy() {
      return missedPings < 5;
    }
  }

  public static class Handlers {

    static FSMBasicEventHandler basicDefault() {
      return new FSMBasicEventHandler<ES, IS, KompicsEvent>() {
        @Override
        public FSMStateName handle(FSMStateName state, ES es, IS is, KompicsEvent req) {
          if (!(req instanceof FSMWrongState) && !(req instanceof Timeout)) {
            es.getProxy().trigger(new FSMWrongState(req), es.ports.seeders);
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

    static FSMBasicEventHandler conn0 = new FSMBasicEventHandler<ES, IS, R2ConnSeederEvents.ConnectReq>() {
      @Override
      public FSMStateName handle(FSMStateName state, ES es, IS is, R2ConnSeederEvents.ConnectReq req) {
        is.connecting.put(req.torrentId, req);
        is.setSeederAdr(req.seederAdr);
        R2ConnMsgs.ConnectReq r = new R2ConnMsgs.ConnectReq();
        bestEffortMsg(es, is, r);
        return States.CONNECTING;
      }
    };

    static FSMBasicEventHandler conn1 = new FSMBasicEventHandler<ES, IS, R2ConnSeederEvents.ConnectReq>() {
      @Override
      public FSMStateName handle(FSMStateName state, ES es, IS is, R2ConnSeederEvents.ConnectReq req) {
        is.connecting.put(req.torrentId, req);
        return States.CONNECTING;
      }
    };

    static FSMBasicEventHandler conn2 = new FSMBasicEventHandler<ES, IS, R2ConnSeederEvents.ConnectReq>() {
      @Override
      public FSMStateName handle(FSMStateName state, ES es, IS is, R2ConnSeederEvents.ConnectReq req) {
        is.connected.put(req.torrentId, req);
        es.getProxy().trigger(req.success(), es.ports.seeders);
        return States.CONNECTED;
      }
    };

    static FSMBasicEventHandler conn3 = new FSMBasicEventHandler<ES, IS, R2ConnSeederEvents.ConnectReq>() {
      @Override
      public FSMStateName handle(FSMStateName state, ES es, IS is, R2ConnSeederEvents.ConnectReq req) {
        es.getProxy().trigger(req.fail(), es.ports.seeders);
        cancelConnPing(es, is);
        return States.DISCONNECTING;
      }
    };

    static FSMBasicEventHandler disc1 = new FSMBasicEventHandler<ES, IS, R2ConnSeederEvents.Disconnect>() {
      @Override
      public FSMStateName handle(FSMStateName state, ES es, IS is, R2ConnSeederEvents.Disconnect req) {
        is.connecting.remove(req.torrentId);
        return States.CONNECTING;
      }
    };

    static FSMBasicEventHandler disc2 = new FSMBasicEventHandler<ES, IS, R2ConnSeederEvents.Disconnect>() {
      @Override
      public FSMStateName handle(FSMStateName state, ES es, IS is, R2ConnSeederEvents.Disconnect req) {
        is.connected.remove(req.torrentId);
        if (is.connected.isEmpty()) {
          R2ConnMsgs.Disconnect msg
            = new R2ConnMsgs.Disconnect();
          bestEffortMsg(es, is, msg);
          cancelConnPing(es, is);
          return States.DISCONNECTING;
        }
        return state;
      }
    };

    static FSMBasicEventHandler connPing = new FSMBasicEventHandler<ES, IS, R2ConnSeederTimeout>() {
      @Override
      public FSMStateName handle(FSMStateName state, ES es, IS is, R2ConnSeederTimeout req) {
        if (!is.pingTracker.healthy()) {
          connectedFail(es, is);
          return FSMBasicStateNames.FINAL;
        }
        R2ConnMsgs.Ping ping = new R2ConnMsgs.Ping();
        bestEffortMsg(es, is, ping);
        is.pingTracker.ping();
        return States.CONNECTED;
      }
    };

    static FSMPatternEventHandler connPong = new FSMPatternEventHandler<ES, IS, R2ConnMsgs.Pong>() {

      @Override
      public FSMStateName handle(FSMStateName state, ES es, IS is, R2ConnMsgs.Pong payload,
        PatternExtractor<Class, R2ConnMsgs.Pong> container) throws FSMException {
        is.pingTracker.pong();
        return state;
      }
    };

    static FSMPatternEventHandler connAcc = new FSMPatternEventHandler<ES, IS, R2ConnMsgs.ConnectAcc>() {

      @Override
      public FSMStateName handle(FSMStateName state, ES es, IS is, R2ConnMsgs.ConnectAcc payload,
        PatternExtractor<Class, R2ConnMsgs.ConnectAcc> container) throws FSMException {
        if (is.connecting.isEmpty()) {
          R2ConnMsgs.Disconnect msg
            = new R2ConnMsgs.Disconnect();
          bestEffortMsg(es, is, msg);
          return States.DISCONNECTING;
        }
        scheduleConnPing(es, is);
        for (R2ConnSeederEvents.ConnectReq req : is.connecting.values()) {
          es.getProxy().trigger(req.success(), es.ports.seeders);
          is.connected.put(req.torrentId, req);
        }
        is.connecting.clear();
        return States.CONNECTED;
      }
    };

    static FSMPatternEventHandler connRej = new FSMPatternEventHandler<ES, IS, R2ConnMsgs.ConnectRej>() {

      @Override
      public FSMStateName handle(FSMStateName state, ES es, IS is, R2ConnMsgs.ConnectRej payload,
        PatternExtractor<Class, R2ConnMsgs.ConnectRej> container) throws FSMException {
        connectingFail(es, is);
        return FSMBasicStateNames.FINAL;
      }
    };

    static FSMPatternEventHandler discAck = new FSMPatternEventHandler<ES, IS, R2ConnMsgs.DisconnectAck>() {

      @Override
      public FSMStateName handle(FSMStateName state, ES es, IS is, R2ConnMsgs.DisconnectAck payload,
        PatternExtractor<Class, R2ConnMsgs.DisconnectAck> container) throws FSMException {
        return FSMBasicStateNames.FINAL;
      }
    };

    static FSMPatternEventHandler beTout1 = new FSMPatternEventHandler<ES, IS, BestEffortMsg.Timeout>() {

      @Override
      public FSMStateName handle(FSMStateName state, ES es, IS is, BestEffortMsg.Timeout payload,
        PatternExtractor<Class, BestEffortMsg.Timeout> container) throws FSMException {
        if (payload.content instanceof R2ConnMsgs.ConnectReq) {
          connectingFail(es, is);
          return FSMBasicStateNames.FINAL;
        }
        return state;
      }
    };

    static FSMPatternEventHandler beTout2 = new FSMPatternEventHandler<ES, IS, BestEffortMsg.Timeout>() {

      @Override
      public FSMStateName handle(FSMStateName state, ES es, IS is, BestEffortMsg.Timeout payload,
        PatternExtractor<Class, BestEffortMsg.Timeout> container) throws FSMException {
        if (payload.content instanceof R2ConnMsgs.Ping) {
          //we ignore R2ConnMsgs.Ping be timeouts - handled by the ping mechanism itself
        } else {
          throw new RuntimeException("unexpected msg:" + container);
        }
        return state;
      }
    };

    private static <C extends KompicsEvent & Identifiable> void bestEffortMsg(ES es, IS is, C content) {
      KHeader header = new BasicHeader(es.selfAdr, is.getSeederAdr(), Transport.UDP);
      BestEffortMsg.Request wrap = new BestEffortMsg.Request(content, es.getRetries(), es.getRetryInterval());
      KContentMsg msg = new BasicContentMsg(header, wrap);
      es.getProxy().trigger(msg, es.ports.network);
    }

    private static void connectingFail(ES es, IS is) {
      for (R2ConnSeederEvents.ConnectReq req : is.connecting.values()) {
        es.getProxy().trigger(req.fail(), es.ports.seeders);
      }
      is.connecting.clear();
    }

    private static void connectedFail(ES es, IS is) {
      for (R2ConnSeederEvents.ConnectReq req : is.connected.values()) {
        es.getProxy().trigger(req.fail(), es.ports.seeders);
      }
      is.connected.clear();
    }

    private static void scheduleConnPing(ES es, IS is) {
      SchedulePeriodicTimeout spt = new SchedulePeriodicTimeout(is.pingTimerPeriod, is.pingTimerPeriod);
      R2ConnSeederTimeout rt = new R2ConnSeederTimeout(spt, is.seederAdr.getId());
      is.setConnPingTimer(rt.getTimeoutId());
      spt.setTimeoutEvent(rt);
      es.getProxy().trigger(spt, es.ports.timer);
    }

    private static void cancelConnPing(ES es, IS is) {
      if (is.getConnPingTimer() != null) {
        CancelPeriodicTimeout cpt = new CancelPeriodicTimeout(is.getConnPingTimer());
        es.getProxy().trigger(cpt, es.ports.timer);
        is.setConnPingTimer(null);
      }
    }
  }
}
