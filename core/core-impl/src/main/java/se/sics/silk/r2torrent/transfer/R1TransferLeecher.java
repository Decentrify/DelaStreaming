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
package se.sics.silk.r2torrent.transfer;

import java.util.Optional;
import java.util.UUID;
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
import se.sics.ktoolbox.util.identifiable.basic.PairIdentifier;
import se.sics.ktoolbox.util.identifiable.overlay.OverlayId;
import se.sics.ktoolbox.util.network.KAddress;
import se.sics.ktoolbox.util.network.KContentMsg;
import se.sics.ktoolbox.util.network.KHeader;
import se.sics.ktoolbox.util.network.basic.BasicContentMsg;
import se.sics.ktoolbox.util.network.basic.BasicHeader;
import se.sics.silk.DefaultHandlers;
import se.sics.silk.SelfPort;
import se.sics.silk.event.SilkEvent;
import se.sics.silk.r2torrent.R2TorrentComp;
import se.sics.silk.r2torrent.R2TorrentES;
import se.sics.silk.r2torrent.conn.R2NodeLeecher;
import se.sics.silk.r2torrent.transfer.R1TransferSeeder.HardCodedConfig;
import se.sics.silk.r2torrent.transfer.events.R1TransferLeecherEvents;
import se.sics.silk.r2torrent.transfer.events.R1TransferLeecherPing;
import se.sics.silk.r2torrent.transfer.msgs.R1TransferConnMsgs;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class R1TransferLeecher {

  private static final Logger LOG = LoggerFactory.getLogger(FSM.class);
  public static final String NAME = "dela-r1-torrent-transfer-leecher-fsm";

  public static enum States implements FSMStateName {

    CONNECT,
    ACTIVE
  }

  public static interface Event extends FSMEvent, Identifiable, SilkEvent.TorrentEvent, SilkEvent.FileEvent,
    SilkEvent.NodeEvent {
  }

  public static interface CtrlEvent extends Event {
  }

  public static interface Timeout extends Event {
  }

  public static interface Msg extends FSMEvent, Identifiable, SilkEvent.TorrentEvent, SilkEvent.FileEvent {
  }

  public static Identifier fsmBasicId(OverlayId torrentId, Identifier fileId, Identifier leecherId) {
    return new PairIdentifier(new PairIdentifier(torrentId, fileId), leecherId);
  }

  public static class IS implements FSMInternalState {

    private final FSMIdentifier fsmId;
    KAddress leecherAdr;
    OverlayId torrentId;
    Identifier fileId;
    UUID pingTimeoutId;
    PingTracker pingTracker = new PingTracker();

    public IS(FSMIdentifier fsmId) {
      this.fsmId = fsmId;
    }

    @Override
    public FSMIdentifier getFSMId() {
      return fsmId;
    }

    public void init(KAddress leecherAdr, R1TransferConnMsgs.Connect req) {
      this.leecherAdr = leecherAdr;
      this.torrentId = req.torrentId;
      this.fileId = req.fileId;
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
      return missedPings < R2NodeLeecher.HardCodedConfig.deadPings;
    }
  }
  
  public static class ISBuilder implements FSMInternalStateBuilder {

    @Override
    public FSMInternalState newState(FSMIdentifier fsmId) {
      return new IS(fsmId);
    }
  }

  public static class ES implements R2TorrentES {

    private ComponentProxy proxy;
    R2TorrentComp.Ports ports;
    KAddress selfAdr;

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

  public static class FSM {

    private static FSMBuilder.StructuralDefinition structuralDef() throws FSMException {
      return FSMBuilder.structuralDef()
        .onStart()
        .nextStates(States.CONNECT)
        .buildTransition()
        .onState(States.CONNECT)
        .nextStates(States.ACTIVE)
        .toFinal()
        .buildTransition()
        .onState(States.ACTIVE)
        .nextStates(States.ACTIVE)
        .toFinal()
        .buildTransition();
    }

    private static FSMBuilder.SemanticDefinition semanticDef() throws FSMException {
      FSMBuilder.SemanticDefinition def = FSMBuilder.semanticDef()
        .defaultFallback(DefaultHandlers.basicDefault(), DefaultHandlers.patternDefault());
      def = def
        .positivePort(SelfPort.class)
        .basicEvent(R1TransferLeecherEvents.ConnectAcc.class)
        .subscribe(Handlers.connectAcc, States.CONNECT)
        .basicEvent(R1TransferLeecherEvents.Disconnect.class)
        .subscribe(Handlers.localDisconnect, States.ACTIVE)
        .buildEvents();
      def = def
        .positivePort(Network.class)
        .patternEvent(R1TransferConnMsgs.Connect.class, BasicContentMsg.class)
        .subscribeOnStart(Handlers.netConnect1)
        .subscribe(Handlers.netConnect2, States.ACTIVE)
        .patternEvent(R1TransferConnMsgs.Ping.class, BasicContentMsg.class)
        .subscribe(Handlers.netPing, States.ACTIVE)
        .patternEvent(R1TransferConnMsgs.Disconnect.class, BasicContentMsg.class)
        .subscribe(Handlers.netDisconnect1, States.CONNECT)
        .subscribe(Handlers.netDisconnect2, States.ACTIVE)
        .buildEvents();
      def = def
        .positivePort(Timer.class)
        .basicEvent(R1TransferLeecherPing.class)
        .subscribe(Handlers.timerPing, States.ACTIVE)
        .buildEvents();
      return def;
    }

    static BaseIdExtractor baseIdExtractor = new BaseIdExtractor() {

      @Override
      public Optional<Identifier> fromEvent(KompicsEvent event) throws FSMException {
        if (event instanceof Event) {
          Event e = (Event) event;
          return Optional.of(fsmBasicId(e.torrentId(), e.fileId(), e.nodeId()));
        } else if (event instanceof BasicContentMsg) {
          BasicContentMsg msg = (BasicContentMsg) event;
          Msg payload = (Msg) msg.getContent();
          Identifier leecherId = msg.getSource().getId();
          return Optional.of(fsmBasicId(payload.torrentId(), payload.fileId(), leecherId));
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

    static FSMPatternEventHandler netConnect1
      = new FSMPatternEventHandler<ES, IS, R1TransferConnMsgs.Connect, BasicContentMsg>() {

        @Override
        public FSMStateName handle(FSMStateName state, ES es, IS is, R1TransferConnMsgs.Connect payload,
          BasicContentMsg container) throws FSMException {
          is.init(container.getSource(), payload);
          R1TransferLeecherEvents.ConnectReq connect
          = new R1TransferLeecherEvents.ConnectReq(is.torrentId, is.fileId, is.leecherAdr);
          sendCtrl(es, is, connect);
          return States.CONNECT;
        }
      };

    static FSMPatternEventHandler netConnect2
      = new FSMPatternEventHandler<ES, IS, R1TransferConnMsgs.Connect, BasicContentMsg>() {

        @Override
        public FSMStateName handle(FSMStateName state, ES es, IS is, R1TransferConnMsgs.Connect payload,
          BasicContentMsg container) throws FSMException {
          sendMsg(es, is, payload.accept());
          return States.ACTIVE;
        }
      };
    static FSMBasicEventHandler connectAcc = new FSMBasicEventHandler<ES, IS, R1TransferLeecherEvents.ConnectAcc>() {

      @Override
      public FSMStateName handle(FSMStateName state, ES es, IS is, R1TransferLeecherEvents.ConnectAcc event)
        throws FSMException {
        R1TransferConnMsgs.ConnectAcc connected = new R1TransferConnMsgs.ConnectAcc(is.torrentId, is.fileId);
        sendMsg(es, is, connected);
        schedulePing(es, is);
        return States.ACTIVE;
      }
    };
    
    static FSMBasicEventHandler timerPing = new FSMBasicEventHandler<ES, IS, R1TransferLeecherPing>() {
      @Override
      public FSMStateName handle(FSMStateName state, ES es, IS is, R1TransferLeecherPing event) throws FSMException {
        is.pingTracker.timerPing();
        if (!is.pingTracker.healthy()) {
          R1TransferLeecherEvents.Disconnected disconnected
            = new R1TransferLeecherEvents.Disconnected(is.torrentId, is.fileId, is.leecherAdr.getId());
          sendCtrl(es, is, disconnected);
          R1TransferConnMsgs.Disconnect disconnect = new R1TransferConnMsgs.Disconnect(is.torrentId, is.fileId);
          sendMsg(es, is, disconnect);
          cancelPing(es, is);
          return FSMBasicStateNames.FINAL;
        }
        return state;
      }
    };
    
    static FSMPatternEventHandler netPing
      = new FSMPatternEventHandler<ES, IS, R1TransferConnMsgs.Ping, BasicContentMsg>() {

        @Override
        public FSMStateName handle(FSMStateName state, ES es, IS is, R1TransferConnMsgs.Ping ping,
          BasicContentMsg container) throws FSMException {
          is.pingTracker.ping();
          sendMsg(es, is, ping.pong());
          return state;
        }
      };

    static FSMBasicEventHandler localDisconnect
      = new FSMBasicEventHandler<ES, IS, R1TransferLeecherEvents.Disconnect>() {

        @Override
        public FSMStateName handle(FSMStateName state, ES es, IS is, R1TransferLeecherEvents.Disconnect event)
        throws FSMException {
          R1TransferConnMsgs.Disconnect disconnect = new R1TransferConnMsgs.Disconnect(is.torrentId, is.fileId);
          sendMsg(es, is, disconnect);
          cancelPing(es, is);
          return FSMBasicStateNames.FINAL;
        }
      };

    static FSMPatternEventHandler netDisconnect1
      = new FSMPatternEventHandler<ES, IS, R1TransferConnMsgs.Disconnect, BasicContentMsg>() {

        @Override
        public FSMStateName handle(FSMStateName state, ES es, IS is, R1TransferConnMsgs.Disconnect payload,
          BasicContentMsg container) throws FSMException {
          R1TransferLeecherEvents.Disconnected disconnected 
            = new R1TransferLeecherEvents.Disconnected(is.torrentId, is.fileId, is.leecherAdr.getId());
          sendCtrl(es, is, disconnected);
          return FSMBasicStateNames.FINAL;
        }
      };

    static FSMPatternEventHandler netDisconnect2
      = new FSMPatternEventHandler<ES, IS, R1TransferConnMsgs.Disconnect, BasicContentMsg>() {

        @Override
        public FSMStateName handle(FSMStateName state, ES es, IS is, R1TransferConnMsgs.Disconnect payload,
          BasicContentMsg container) throws FSMException {
          R1TransferLeecherEvents.Disconnected disconnected 
            = new R1TransferLeecherEvents.Disconnected(is.torrentId, is.fileId, is.leecherAdr.getId());
          sendCtrl(es, is, disconnected);
          cancelPing(es, is);
          return FSMBasicStateNames.FINAL;
        }
      };
    
    private static <C extends KompicsEvent & Identifiable> void sendMsg(ES es, IS is, C content) {
      KHeader header = new BasicHeader(es.selfAdr, is.leecherAdr, Transport.UDP);
      KContentMsg msg = new BasicContentMsg(header, content);
      es.proxy.trigger(msg, es.ports.network);
    }

    private static void sendCtrl(ES es, IS is, KompicsEvent content) {
      es.proxy.trigger(content, es.ports.loopbackSend);
    }

    private static void schedulePing(ES es, IS is) {
      SchedulePeriodicTimeout spt
        = new SchedulePeriodicTimeout(HardCodedConfig.pingTimerPeriod, HardCodedConfig.pingTimerPeriod);
      R1TransferLeecherPing rt = new R1TransferLeecherPing(spt, is.torrentId, is.fileId, is.leecherAdr.getId());
      is.pingTimeoutId = rt.getTimeoutId();
      spt.setTimeoutEvent(rt);
      es.proxy.trigger(spt, es.ports.timer);
    }

    private static void cancelPing(ES es, IS is) {
      if (is.pingTimeoutId != null) {
        CancelPeriodicTimeout cpt = new CancelPeriodicTimeout(is.pingTimeoutId);
        es.proxy.trigger(cpt, es.ports.timer);
        is.pingTimeoutId = null;
      }
    }
  }
}
