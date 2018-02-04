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
import java.util.function.BiConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.kompics.Channel;
import se.sics.kompics.Component;
import se.sics.kompics.ComponentProxy;
import se.sics.kompics.Kill;
import se.sics.kompics.KompicsEvent;
import se.sics.kompics.Start;
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
import se.sics.silk.event.SilkEvent;
import se.sics.silk.r2torrent.R2TorrentComp;
import se.sics.silk.r2torrent.R2TorrentES;
import se.sics.silk.r2torrent.torrent.R1FileDownload;
import se.sics.silk.r2torrent.torrent.util.R1FileMetadata;
import se.sics.silk.r2torrent.transfer.events.R1TransferSeederEvents;
import se.sics.silk.r2torrent.transfer.events.R1TransferSeederPing;
import se.sics.silk.r2torrent.transfer.msgs.R1TransferConnMsgs;

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

  public static Identifier fsmBasicId(OverlayId torrentId, Identifier fileId, Identifier seederId) {
    return new PairIdentifier(new PairIdentifier(torrentId, fileId), seederId);
  }

  public static class HardCodedConfig {

    public static int beRetries = 1;
    public static int beRetryInterval = 2000;
    public static final long pingTimerPeriod = 1000;
    public static final int deadPings = 5;
  }

  public static class IS implements FSMInternalState {

    private final FSMIdentifier fsmId;
    KAddress seederAdr;
    OverlayId torrentId;
    Identifier fileId;
    R1FileMetadata fileMetadata;
    UUID pingTimeoutId;
    PingTracker pingTracker = new PingTracker();
    Component downloadComp;

    public IS(FSMIdentifier fsmId) {
      this.fsmId = fsmId;
    }

    @Override
    public FSMIdentifier getFSMId() {
      return fsmId;
    }

    public void init(R1TransferSeederEvents.Connect req) {
      this.torrentId = req.torrentId;
      this.fileId = req.fileId;
      this.seederAdr = req.seederAdr;
      this.fileMetadata = req.fileMetadata;
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
        .negativePort(R1TransferSeederCtrl.class)
        .basicEvent(R1TransferSeederEvents.Connect.class)
        .subscribeOnStart(Handlers.connect)
        .basicEvent(R1TransferSeederEvents.Disconnect.class)
        .subscribe(Handlers.disconnect, States.CONNECT, States.ACTIVE)
        .buildEvents();
      def = def
        .positivePort(Network.class)
        .patternEvent(R1TransferConnMsgs.ConnectAcc.class, BasicContentMsg.class)
        .subscribe(Handlers.netConnectAcc, States.CONNECT)
        .patternEvent(R1TransferConnMsgs.Pong.class, BasicContentMsg.class)
        .subscribe(Handlers.netPong, States.ACTIVE)
        .patternEvent(R1TransferConnMsgs.Disconnect.class, BasicContentMsg.class)
        .subscribe(Handlers.netDisconnect, States.ACTIVE)
        .buildEvents();
      def = def
        .positivePort(Timer.class)
        .basicEvent(R1TransferSeederPing.class)
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
        is.init(event);
        bestEffortMsg(es, is, new R1TransferConnMsgs.Connect(is.torrentId, is.fileId));
        return States.CONNECT;
      }
    };

    static FSMPatternEventHandler netConnectAcc
      = new FSMPatternEventHandler<ES, IS, R1TransferConnMsgs.ConnectAcc, BasicContentMsg>() {

        @Override
        public FSMStateName handle(FSMStateName state, ES es, IS is, R1TransferConnMsgs.ConnectAcc payload,
          BasicContentMsg container) throws FSMException {
          createDownloadComp.accept(es, is);
          sendCtrl(es, is, new R1TransferSeederEvents.Connected(is.torrentId, is.fileId, is.seederAdr));
          schedulePing(es, is);
          return States.ACTIVE;
        }
      };

    static FSMPatternEventHandler netDisconnect
      = new FSMPatternEventHandler<ES, IS, R1TransferConnMsgs.Disconnect, BasicContentMsg>() {

        @Override
        public FSMStateName handle(FSMStateName state, ES es, IS is, R1TransferConnMsgs.Disconnect payload,
          BasicContentMsg container) throws FSMException {
          killDownloadComp.accept(es, is);
          sendCtrl(es, is, new R1TransferSeederEvents.Disconnected(is.torrentId, is.fileId, is.seederAdr));
          cancelPing(es, is);
          return FSMBasicStateNames.FINAL;
        }
      };

    static FSMBasicEventHandler timerPing = new FSMBasicEventHandler<ES, IS, R1TransferSeederPing>() {
      @Override
      public FSMStateName handle(FSMStateName state, ES es, IS is, R1TransferSeederPing event) throws FSMException {
        if (is.pingTracker.healthy()) {
          R1TransferConnMsgs.Ping ping = new R1TransferConnMsgs.Ping(is.torrentId, is.fileId);
          msg(es, is, ping);
          is.pingTracker.ping();
          return state;
        } else {
          killDownloadComp.accept(es, is);
          sendCtrl(es, is, new R1TransferSeederEvents.Disconnected(is.torrentId, is.fileId, is.seederAdr));
          msg(es, is, new R1TransferConnMsgs.Disconnect(is.torrentId, is.fileId));
          cancelPing(es, is);
          return FSMBasicStateNames.FINAL;
        }
      }
    };

    static FSMPatternEventHandler netPong
      = new FSMPatternEventHandler<ES, IS, R1TransferConnMsgs.Pong, BasicContentMsg>() {

        @Override
        public FSMStateName handle(FSMStateName state, ES es, IS is, R1TransferConnMsgs.Pong payload,
          BasicContentMsg container) throws FSMException {
          is.pingTracker.pong();
          return state;
        }
      };

    static FSMBasicEventHandler disconnect = new FSMBasicEventHandler<ES, IS, R1TransferSeederEvents.Disconnect>() {

      @Override
      public FSMStateName handle(FSMStateName state, ES es, IS is, R1TransferSeederEvents.Disconnect req)
        throws FSMException {
        killDownloadComp.accept(es, is);
        sendCtrl(es, is, req.ack());
        msg(es, is, new R1TransferConnMsgs.Disconnect(is.torrentId, is.fileId));
        cancelPing(es, is);
        return FSMBasicStateNames.FINAL;
      }
    };

    static FSMPatternEventHandler msgTimeout
      = new FSMPatternEventHandler<ES, IS, BestEffortMsg.Timeout, BasicContentMsg>() {

        @Override
        public FSMStateName handle(FSMStateName state, ES es, IS is, BestEffortMsg.Timeout payload,
          BasicContentMsg container) throws FSMException {
          R1TransferSeederEvents.Disconnected stopped
          = new R1TransferSeederEvents.Disconnected(is.torrentId, is.fileId, is.seederAdr);
          sendCtrl(es, is, stopped);
          cancelPing(es, is);
          return FSMBasicStateNames.FINAL;
        }
      };

    private static void sendCtrl(ES es, IS is, R1FileDownload.ConnectEvent content) {
      es.proxy.trigger(content, es.ports.transferSeederCtrlNeg);
    }

    private static <C extends KompicsEvent & Identifiable> void bestEffortMsg(ES es, IS is, C content) {
      KHeader header = new BasicHeader(es.selfAdr, is.seederAdr, Transport.UDP);
      BestEffortMsg.Request wrap = new BestEffortMsg.Request(content, R1DownloadComp.HardCodedConfig.beRetries,
        R1DownloadComp.HardCodedConfig.beRetryInterval);
      KContentMsg msg = new BasicContentMsg(header, wrap);
      es.proxy.trigger(msg, es.ports.network);
    }

    private static <C extends KompicsEvent & Identifiable> void msg(ES es, IS is, C content) {
      KHeader header = new BasicHeader(es.selfAdr, is.seederAdr, Transport.UDP);
      KContentMsg msg = new BasicContentMsg(header, content);
      es.proxy.trigger(msg, es.ports.network);
    }

    private static void schedulePing(ES es, IS is) {
      SchedulePeriodicTimeout spt
        = new SchedulePeriodicTimeout(HardCodedConfig.pingTimerPeriod, HardCodedConfig.pingTimerPeriod);
      R1TransferSeederPing rt = new R1TransferSeederPing(spt, is.torrentId, is.fileId, is.seederAdr.getId());
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

    public static BiConsumer<ES, IS> createDownloadComp = new BiConsumer<ES, IS>() {

      @Override
      public void accept(ES es, IS is) {
        R1DownloadComp.Init init = new R1DownloadComp.Init(es.selfAdr, is.torrentId, is.fileId, is.seederAdr,
          is.fileMetadata);
        Component downloadComp = es.proxy.create(R1DownloadComp.class, init);
        Identifier downloadId = R1DownloadComp.baseId(is.torrentId, is.fileId, is.seederAdr.getId());
        es.proxy.connect(es.ports.timer, downloadComp.getNegative(Timer.class), Channel.TWO_WAY);
        es.ports.downloadC.addChannel(downloadId, downloadComp.getPositive(R1DownloadPort.class));
        es.ports.netDownloadC.addChannel(downloadId, downloadComp.getNegative(Network.class));
        es.proxy.trigger(Start.event, downloadComp.control());
        is.downloadComp = downloadComp;
      }
    };

    public static BiConsumer<ES, IS> killDownloadComp = new BiConsumer<ES, IS>() {
      @Override
      public void accept(ES es, IS is) {
        Identifier downloadId = R1DownloadComp.baseId(is.torrentId, is.fileId, is.seederAdr.getId());
        Component downloadComp = is.downloadComp;
        es.proxy.trigger(Kill.event, downloadComp.control());
        es.proxy.disconnect(es.ports.timer, downloadComp.getNegative(Timer.class));
        es.ports.downloadC.removeChannel(downloadId, downloadComp.getPositive(R1DownloadPort.class));
        es.ports.netDownloadC.removeChannel(downloadId, downloadComp.getNegative(Network.class));
        is.downloadComp = null;
      }
    };
  }
}
