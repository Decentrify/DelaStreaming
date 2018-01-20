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
import se.sics.nutil.network.bestEffort.event.BestEffortMsg;
import se.sics.silk.DefaultHandlers;
import se.sics.silk.SelfPort;
import se.sics.silk.event.SilkEvent;
import se.sics.silk.r2torrent.transfer.events.R1TransferSeederEvents;
import se.sics.silk.r2torrent.transfer.events.R1TransferSeederPing;
import se.sics.silk.r2torrent.transfer.msgs.R1TransferMsgs;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class R1TransferSeeder {

  private static final Logger LOG = LoggerFactory.getLogger(FSM.class);
  public static final String NAME = "dela-r1-torrent-transfer-seeder-fsm";

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

  public static Identifier fsmBasicId(OverlayId torrentId, Identifier fileId, Identifier nodeId) {
    return new PairIdentifier(new PairIdentifier(torrentId, fileId), nodeId);
  }

  public static class HardCodedConfig {

    public static int beRetries = 1;
    public static int beRetryInterval = 2000;
    public static final long pingTimerPeriod = 1000;
    public static final int deadPings = 5;
  }

  public static class IS implements FSMInternalState {

    private final FSMIdentifier fsmId;
    UUID pingTimeoutId;
    PingTracker pingTracker = new PingTracker();

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
      missedPings++;
    }

    public void pong() {
      missedPings = 0;
    }

    public boolean healthy() {
      return missedPings < HardCodedConfig.deadPings;
    }
  }

  public static class ISBuilder implements FSMInternalStateBuilder {

    @Override
    public FSMInternalState newState(FSMIdentifier fsmId) {
      return new IS(fsmId);
    }
  }

  public static class ES implements DownloadES {

    private ComponentProxy proxy;
    R1TransferSeederComp.Ports ports;
    KAddress selfAdr;
    KAddress seederAdr;
    OverlayId torrentId;
    Identifier fileId;

    public ES(KAddress selfAdr, KAddress seederAdr, OverlayId torrentId, Identifier fileId) {
      this.selfAdr = seederAdr;
      this.seederAdr = seederAdr;
      this.torrentId = torrentId;
      this.fileId = fileId;
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
    public void setPorts(R1TransferSeederComp.Ports ports) {
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
        .basicEvent(R1TransferSeederEvents.Connect.class)
        .subscribeOnStart(Handlers.connect)
        .basicEvent(R1TransferSeederEvents.Disconnect.class)
        .subscribe(Handlers.disconnect, States.CONNECT, States.ACTIVE)
        .buildEvents();
      def = def
        .positivePort(Network.class)
        .patternEvent(R1TransferMsgs.ConnectAcc.class, BasicContentMsg.class)
        .subscribe(Handlers.connectAcc, States.CONNECT)
        .patternEvent(R1TransferMsgs.Pong.class, BasicContentMsg.class)
        .subscribe(Handlers.pong, States.ACTIVE)
        .buildEvents();
      def = def
        .positivePort(Timer.class)
        .basicEvent(R1TransferSeederPing.class)
        .subscribe(Handlers.pingTimeout, States.ACTIVE)
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
          Identifier seederId = msg.getSource().getId();
          return Optional.of(fsmBasicId(payload.torrentId(), payload.fileId(), seederId));
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

    static FSMBasicEventHandler connect = new FSMBasicEventHandler<ES, IS, R1TransferSeederEvents.Connect>() {

      @Override
      public FSMStateName handle(FSMStateName state, ES es, IS is, R1TransferSeederEvents.Connect event)
        throws FSMException {
        R1TransferMsgs.Connect connect = new R1TransferMsgs.Connect(es.torrentId, es.fileId);
        bestEffortMsg(es, is, connect);
        return States.CONNECT;
      }
    };

    static FSMPatternEventHandler connectAcc
      = new FSMPatternEventHandler<ES, IS, R1TransferMsgs.ConnectAcc, BasicContentMsg>() {

        @Override
        public FSMStateName handle(FSMStateName state, ES es, IS is, R1TransferMsgs.ConnectAcc payload,
          BasicContentMsg container) throws FSMException {
          R1TransferSeederEvents.Connected connected
          = new R1TransferSeederEvents.Connected(es.torrentId, es.fileId, es.seederAdr.getId());
          sendCtrl(es, is, connected);
          schedulePing(es, is);
          return States.ACTIVE;
        }
      };

    static FSMBasicEventHandler pingTimeout = new FSMBasicEventHandler<ES, IS, R1TransferSeederPing>() {
      @Override
      public FSMStateName handle(FSMStateName state, ES es, IS is, R1TransferSeederPing event) throws FSMException {
        if (is.pingTracker.healthy()) {
          R1TransferMsgs.Ping ping = new R1TransferMsgs.Ping(es.torrentId, es.fileId);
          msg(es, is, ping);
          is.pingTracker.ping();
          return state;
        } else {
          R1TransferSeederEvents.Disconnected disconnected
            = new R1TransferSeederEvents.Disconnected(es.torrentId, es.fileId, es.seederAdr.getId());
          sendCtrl(es, is, disconnected);
          R1TransferMsgs.Disconnect connect = new R1TransferMsgs.Disconnect(es.torrentId, es.fileId);
          msg(es, is, connect);
          return FSMBasicStateNames.FINAL;
        }
      }
    };

    static FSMPatternEventHandler pong
      = new FSMPatternEventHandler<ES, IS, R1TransferMsgs.Pong, BasicContentMsg>() {

        @Override
        public FSMStateName handle(FSMStateName state, ES es, IS is, R1TransferMsgs.Pong payload,
          BasicContentMsg container) throws FSMException {
          is.pingTracker.pong();
          return state;
        }
      };
    static FSMBasicEventHandler disconnect = new FSMBasicEventHandler<ES, IS, R1TransferSeederEvents.Disconnect>() {

      @Override
      public FSMStateName handle(FSMStateName state, ES es, IS is, R1TransferSeederEvents.Disconnect event)
        throws FSMException {
        R1TransferMsgs.Disconnect connect = new R1TransferMsgs.Disconnect(es.torrentId, es.fileId);
        msg(es, is, connect);
        return FSMBasicStateNames.FINAL;
      }
    };

    static FSMPatternEventHandler msgTimeout
      = new FSMPatternEventHandler<ES, IS, BestEffortMsg.Timeout, BasicContentMsg>() {

        @Override
        public FSMStateName handle(FSMStateName state, ES es, IS is, BestEffortMsg.Timeout payload,
          BasicContentMsg container) throws FSMException {
          R1TransferSeederEvents.Disconnected stopped
          = new R1TransferSeederEvents.Disconnected(es.torrentId, es.fileId, es.seederAdr.getId());
          sendCtrl(es, is, stopped);
          return FSMBasicStateNames.FINAL;
        }
      };

    private static void sendCtrl(ES es, IS is, KompicsEvent content) {
      es.proxy.trigger(content, es.ports.loopbackSend);
    }

    private static <C extends KompicsEvent & Identifiable> void bestEffortMsg(ES es, IS is, C content) {
      KHeader header = new BasicHeader(es.selfAdr, es.seederAdr, Transport.UDP);
      BestEffortMsg.Request wrap = new BestEffortMsg.Request(content, R1TransferSeederComp.HardCodedConfig.beRetries,
        R1TransferSeederComp.HardCodedConfig.beRetryInterval);
      KContentMsg msg = new BasicContentMsg(header, wrap);
      es.proxy.trigger(msg, es.ports.networkP);
    }

    private static <C extends KompicsEvent & Identifiable> void msg(ES es, IS is, C content) {
      KHeader header = new BasicHeader(es.selfAdr, es.seederAdr, Transport.UDP);
      KContentMsg msg = new BasicContentMsg(header, content);
      es.proxy.trigger(msg, es.ports.networkP);
    }

    private static void schedulePing(ES es, IS is) {
      SchedulePeriodicTimeout spt
        = new SchedulePeriodicTimeout(HardCodedConfig.pingTimerPeriod, HardCodedConfig.pingTimerPeriod);
      R1TransferSeederPing rt = new R1TransferSeederPing(spt, es.torrentId, es.fileId, es.seederAdr.getId());
      is.pingTimeoutId = rt.getTimeoutId();
      spt.setTimeoutEvent(rt);
      es.proxy.trigger(spt, es.ports.timerP);
    }

    private static void cancelPing(ES es, IS is) {
      if (is.pingTimeoutId != null) {
        CancelPeriodicTimeout cpt = new CancelPeriodicTimeout(is.pingTimeoutId);
        es.proxy.trigger(cpt, es.ports.timerP);
        is.pingTimeoutId = null;
      }
    }
  }
}
