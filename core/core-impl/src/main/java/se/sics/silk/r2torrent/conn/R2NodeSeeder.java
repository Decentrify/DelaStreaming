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
import se.sics.kompics.fsm.BaseIdExtractor;
import se.sics.kompics.fsm.FSMBasicStateNames;
import se.sics.kompics.fsm.FSMBuilder;
import se.sics.kompics.fsm.FSMEvent;
import se.sics.kompics.fsm.FSMException;
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
import se.sics.kompics.network.Transport;
import se.sics.kompics.timer.CancelPeriodicTimeout;
import se.sics.kompics.timer.SchedulePeriodicTimeout;
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
import se.sics.silk.DefaultHandlers;
import se.sics.silk.event.SilkEvent;
import se.sics.silk.r2torrent.R2TorrentComp;
import se.sics.silk.r2torrent.R2TorrentES;
import se.sics.silk.SelfPort;
import se.sics.silk.r2torrent.conn.event.R2NodeSeederEvents;
import se.sics.silk.r2torrent.conn.event.R2NodeSeederTimeout;
import se.sics.silk.r2torrent.conn.msg.R2NodeConnMsgs;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class R2NodeSeeder {

  private static final Logger LOG = LoggerFactory.getLogger(R2NodeSeeder.class);
  public static final String NAME = "dela-r2-node-seeder-fsm";

  public static enum States implements FSMStateName {

    CONNECT,
    CONNECTED
  }
  
  public static class HardCodedConfig {
    public static final long pingTimerPeriod = 1000;
    public static final int deadPings = 5;
    public static  final int retries = 5;
    public static final long retryInterval = 300;
  }
  
  public static interface Msg extends FSMEvent, Identifiable {
  }

  public static interface Event extends FSMEvent, Identifiable, SilkEvent.TorrentEvent, SilkEvent.NodeEvent {
  }
  
  public static interface Timeout extends FSMEvent, SilkEvent.NodeEvent { 
  }

  public static Identifier fsmBaseId(KAddress seeder) {
    return seeder.getId();
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

  public static class FSM {

    private static FSMBuilder.StructuralDefinition structuralDef() throws FSMException {
      return FSMBuilder.structuralDef()
        .onStart()
        .nextStates(States.CONNECT)
        .buildTransition()
        .onState(States.CONNECT)
        .nextStates(States.CONNECT, States.CONNECTED)
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
        .positivePort(SelfPort.class)
        .basicEvent(R2NodeSeederEvents.ConnectReq.class)
        .subscribeOnStart(Handlers.conn0)
        .subscribe(Handlers.conn1, States.CONNECT)
        .subscribe(Handlers.conn2, States.CONNECTED)
        .basicEvent(R2NodeSeederEvents.Disconnect.class)
        .subscribe(Handlers.localDisc1, States.CONNECT)
        .subscribe(Handlers.localDisc2, States.CONNECTED)
        .buildEvents()
        .positivePort(Network.class)
        .patternEvent(BestEffortMsg.Timeout.class, BasicContentMsg.class)
        .subscribe(Handlers.beTout1, States.CONNECT)
        .subscribe(Handlers.beTout2, States.CONNECTED)
        .patternEvent(R2NodeConnMsgs.ConnectAcc.class, BasicContentMsg.class)
        .subscribe(Handlers.netConnAcc, States.CONNECT)
        .patternEvent(R2NodeConnMsgs.ConnectRej.class, BasicContentMsg.class)
        .subscribe(Handlers.netConnRej, States.CONNECT)
        .patternEvent(R2NodeConnMsgs.Pong.class, BasicContentMsg.class)
        .subscribe(Handlers.netConnPong, States.CONNECTED)
        .patternEvent(R2NodeConnMsgs.Disconnect.class, BasicContentMsg.class)
        .subscribe(Handlers.netDisc1, States.CONNECT)
        .subscribe(Handlers.netDisc2, States.CONNECTED)
        .buildEvents()
        .positivePort(Timer.class)
        .basicEvent(R2NodeSeederTimeout.class)
        .subscribe(Handlers.timerConnPing, States.CONNECTED)
        .buildEvents();
    }

    public static MultiFSM multifsm(FSMIdentifierFactory fsmIdFactory, ES es, OnFSMExceptionAction oexa)
      throws FSMException {
      FSMInternalStateBuilder isb = new ISBuilder();
      return FSMBuilder.multiFSM(fsmIdFactory, NAME, structuralDef(), semanticDef(), es, isb, oexa, baseIdExtractor);
    }
  }

  public static class ES implements R2TorrentES {

    private ComponentProxy proxy;
    public R2TorrentComp.Ports ports;
    public final KAddress selfAdr;

    public ES(KAddress selfAdr) {
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

    @Override
    public void setPorts(R2TorrentComp.Ports ports) {
      this.ports = ports;
    }
  }

  public static class IS implements FSMInternalState {

    private final FSMIdentifier fsmId;
    KAddress seederAdr;
    final TorrentReqTracker reqTracker = new TorrentReqTracker();
    final PingTracker pingTracker = new PingTracker();
    UUID connPingTimer;

    public IS(FSMIdentifier fsmId) {
      this.fsmId = fsmId;
    }

    @Override
    public FSMIdentifier getFSMId() {
      return fsmId;
    }
  }
  
  public static class TorrentReqTracker {

    Map<OverlayId, R2NodeSeederEvents.ConnectReq> reqs = new HashMap<>();

    void connect(R2NodeSeederEvents.ConnectReq req) {
      reqs.put(req.torrentId, req);
    }

    void connected(Consumer<R2NodeSeederEvents.Ind> answerR0) {
      reqs.values().stream().forEach((req) -> {
        answerR0.accept(req.success());
      });
    }

    void failed(Consumer<R2NodeSeederEvents.Ind> answerR0) {
      reqs.values().stream().forEach((req) -> {
        answerR0.accept(req.fail());
      });
    }

    void disconnect(R2NodeSeederEvents.Disconnect req) {
      reqs.remove(req.torrentId);
    }

    boolean empty() {
      return reqs.isEmpty();
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
      return missedPings < HardCodedConfig.deadPings;
    }
  }

  public static class Handlers {

    static FSMBasicEventHandler conn0 = new FSMBasicEventHandler<ES, IS, R2NodeSeederEvents.ConnectReq>() {
      @Override
      public FSMStateName handle(FSMStateName state, ES es, IS is, R2NodeSeederEvents.ConnectReq req) {
        is.seederAdr = req.seederAdr;
        is.reqTracker.connect(req);
        R2NodeConnMsgs.ConnectReq r = new R2NodeConnMsgs.ConnectReq();
        bestEffortMsg(es, is, r);
        return States.CONNECT;
      }
    };

    static FSMBasicEventHandler conn1 = new FSMBasicEventHandler<ES, IS, R2NodeSeederEvents.ConnectReq>() {
      @Override
      public FSMStateName handle(FSMStateName state, ES es, IS is, R2NodeSeederEvents.ConnectReq req) {
        is.reqTracker.connect(req);
        return States.CONNECT;
      }
    };

    static FSMBasicEventHandler conn2 = new FSMBasicEventHandler<ES, IS, R2NodeSeederEvents.ConnectReq>() {
      @Override
      public FSMStateName handle(FSMStateName state, ES es, IS is, R2NodeSeederEvents.ConnectReq req) {
        is.reqTracker.connect(req);
        sendR1(es, req.success());
        return States.CONNECTED;
      }
    };

    static FSMBasicEventHandler localDisc1 = new FSMBasicEventHandler<ES, IS, R2NodeSeederEvents.Disconnect>() {
      @Override
      public FSMStateName handle(FSMStateName state, ES es, IS is, R2NodeSeederEvents.Disconnect req) {
        is.reqTracker.disconnect(req);
        if (is.reqTracker.empty()) {
          R2NodeConnMsgs.Disconnect msg = new R2NodeConnMsgs.Disconnect();
          bestEffortMsg(es, is, msg);
          //ping not initiated yet
          return FSMBasicStateNames.FINAL;
        }
        return States.CONNECT;
      }
    };

    static FSMBasicEventHandler localDisc2 = new FSMBasicEventHandler<ES, IS, R2NodeSeederEvents.Disconnect>() {
      @Override
      public FSMStateName handle(FSMStateName state, ES es, IS is, R2NodeSeederEvents.Disconnect req) {
        is.reqTracker.disconnect(req);
        if (is.reqTracker.empty()) {
          R2NodeConnMsgs.Disconnect msg = new R2NodeConnMsgs.Disconnect();
          bestEffortMsg(es, is, msg);
          cancelConnPing(es, is);
          return FSMBasicStateNames.FINAL;
        }
        return state;
      }
    };

    static FSMBasicEventHandler timerConnPing = new FSMBasicEventHandler<ES, IS, R2NodeSeederTimeout>() {
      @Override
      public FSMStateName handle(FSMStateName state, ES es, IS is, R2NodeSeederTimeout req) {
        if (!is.pingTracker.healthy()) {
          cancelConnPing(es, is);
          is.reqTracker.failed(sendR1(es));
          return FSMBasicStateNames.FINAL;
        }
        R2NodeConnMsgs.Ping ping = new R2NodeConnMsgs.Ping();
        bestEffortMsg(es, is, ping);
        is.pingTracker.ping();
        return States.CONNECTED;
      }
    };

    static FSMPatternEventHandler netConnPong = new FSMPatternEventHandler<ES, IS, R2NodeConnMsgs.Pong, BasicContentMsg>() {

      @Override
      public FSMStateName handle(FSMStateName state, ES es, IS is, R2NodeConnMsgs.Pong payload,
        BasicContentMsg msg) throws FSMException {
        is.pingTracker.pong();
        return state;
      }
    };

    static FSMPatternEventHandler netConnAcc = new FSMPatternEventHandler<ES, IS, R2NodeConnMsgs.ConnectAcc, BasicContentMsg>() {

      @Override
      public FSMStateName handle(FSMStateName state, ES es, IS is, R2NodeConnMsgs.ConnectAcc payload,
        BasicContentMsg msg) throws FSMException {
        scheduleConnPing(es, is);
        is.reqTracker.connected(sendR1(es));
        return States.CONNECTED;
      }
    };

    static FSMPatternEventHandler netConnRej = new FSMPatternEventHandler<ES, IS, R2NodeConnMsgs.ConnectRej, BasicContentMsg>() {

      @Override
      public FSMStateName handle(FSMStateName state, ES es, IS is, R2NodeConnMsgs.ConnectRej payload,
        BasicContentMsg msg) throws FSMException {
        is.reqTracker.failed(sendR1(es));
        return FSMBasicStateNames.FINAL;
      }
    };

    static FSMPatternEventHandler netDisc1 = new FSMPatternEventHandler<ES, IS, R2NodeConnMsgs.Disconnect, BasicContentMsg>() {

      @Override
      public FSMStateName handle(FSMStateName state, ES es, IS is, R2NodeConnMsgs.Disconnect payload,
        BasicContentMsg container) throws FSMException {
        is.reqTracker.failed(sendR1(es));
        return FSMBasicStateNames.FINAL;
      }
    };
    
    static FSMPatternEventHandler netDisc2 = new FSMPatternEventHandler<ES, IS, R2NodeConnMsgs.Disconnect, BasicContentMsg>() {

      @Override
      public FSMStateName handle(FSMStateName state, ES es, IS is, R2NodeConnMsgs.Disconnect payload,
        BasicContentMsg container) throws FSMException {
        cancelConnPing(es, is);
        is.reqTracker.failed(sendR1(es));
        return FSMBasicStateNames.FINAL;
      }
    };
    
    static FSMPatternEventHandler beTout1 = new FSMPatternEventHandler<ES, IS, BestEffortMsg.Timeout, BasicContentMsg>() {

      @Override
      public FSMStateName handle(FSMStateName state, ES es, IS is, BestEffortMsg.Timeout payload,
        BasicContentMsg msg) throws FSMException {
        if (payload.content instanceof R2NodeConnMsgs.ConnectReq) {
          is.reqTracker.failed(sendR1(es));
          return FSMBasicStateNames.FINAL;
        }
        return state;
      }
    };

    static FSMPatternEventHandler beTout2 = new FSMPatternEventHandler<ES, IS, BestEffortMsg.Timeout, BasicContentMsg>() {

      @Override
      public FSMStateName handle(FSMStateName state, ES es, IS is, BestEffortMsg.Timeout payload,
        BasicContentMsg msg) throws FSMException {
        if (payload.content instanceof R2NodeConnMsgs.Ping) {
          //we ignore R2ConnMsgs.Ping be timeouts - handled by the ping mechanism itself
        } else {
          throw new RuntimeException("unexpected msg:" + msg);
        }
        return state;
      }
    };

    private static <C extends KompicsEvent & Identifiable> void bestEffortMsg(ES es, IS is, C content) {
      KHeader header = new BasicHeader(es.selfAdr, is.seederAdr, Transport.UDP);
      BestEffortMsg.Request wrap = new BestEffortMsg.Request(content, HardCodedConfig.retries, HardCodedConfig.retryInterval);
      KContentMsg msg = new BasicContentMsg(header, wrap);
      es.getProxy().trigger(msg, es.ports.network);
    }
    
    private static void sendR1(ES es, R2NodeSeederEvents.Ind event) {
      es.getProxy().trigger(event, es.ports.loopbackSend);
    }
    
    private static Consumer<R2NodeSeederEvents.Ind> sendR1(ES es) {
    return new Consumer<R2NodeSeederEvents.Ind>() {
      @Override
      public void accept(R2NodeSeederEvents.Ind ind) {
        sendR1(es, ind);
      }
    };
  }
    
    private static void scheduleConnPing(ES es, IS is) {
      SchedulePeriodicTimeout spt = new SchedulePeriodicTimeout(HardCodedConfig.pingTimerPeriod, HardCodedConfig.pingTimerPeriod);
      R2NodeSeederTimeout rt = new R2NodeSeederTimeout(spt, is.seederAdr.getId());
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
