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
import se.sics.silk.DefaultHandlers;
import se.sics.silk.event.SilkEvent;
import se.sics.silk.r2torrent.R2TorrentComp;
import se.sics.silk.r2torrent.R2TorrentES;
import se.sics.silk.r2torrent.torrent.R1FileUpload;
import se.sics.silk.r2torrent.torrent.util.R1FileMetadata;
import se.sics.silk.r2torrent.torrent.util.R1TorrentDetails;
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

  public static Identifier fsmBaseId(OverlayId torrentId, Identifier fileId, Identifier leecherId) {
    return new PairIdentifier(new PairIdentifier(torrentId, fileId), leecherId);
  }

  public static class IS implements FSMInternalState {

    private final FSMIdentifier fsmId;
    KAddress leecherAdr;
    OverlayId torrentId;
    Identifier fileId;
    PingTracker pingTracker = new PingTracker();
    Component uploadComp;
    UUID pingTimeoutId;

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
    R1TorrentDetails.Mngr torrentDetailsMngr;

    public ES(KAddress selfAdr, R1TorrentDetails.Mngr torrentDetailsMngr) {
      this.selfAdr = selfAdr;
      this.torrentDetailsMngr = torrentDetailsMngr;
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
        .positivePort(R1TransferLeecherCtrl.class)
        .basicEvent(R1TransferLeecherEvents.ConnectAcc.class)
        .subscribe(Handlers.connectAcc, States.CONNECT)
        .basicEvent(R1TransferLeecherEvents.ConnectRej.class)
        .subscribe(Handlers.connectRej, States.CONNECT)
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
          return Optional.of(fsmBaseId(e.torrentId(), e.fileId(), e.nodeId()));
        } else if (event instanceof BasicContentMsg) {
          BasicContentMsg msg = (BasicContentMsg) event;
          Msg payload = (Msg) msg.getContent();
          Identifier leecherId = msg.getSource().getId();
          return Optional.of(fsmBaseId(payload.torrentId(), payload.fileId(), leecherId));
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
          sendCtrl(es, is, new R1TransferLeecherEvents.ConnectReq(is.torrentId, is.fileId, is.leecherAdr));
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
    static FSMBasicEventHandler connectAcc
      = (FSMBasicEventHandler<ES, IS, R1TransferLeecherEvents.ConnectAcc>) (FSMStateName state,
        ES es, IS is, R1TransferLeecherEvents.ConnectAcc event) -> {
        Handlers.createUploadComp.accept(es, is);
        sendMsg(es, is, new R1TransferConnMsgs.ConnectAcc(is.torrentId, is.fileId));
        schedulePing(es, is);
        return States.ACTIVE;
      };

    static FSMBasicEventHandler connectRej
      = (FSMBasicEventHandler<ES, IS, R1TransferLeecherEvents.ConnectRej>) (FSMStateName state,
        ES es, IS is, R1TransferLeecherEvents.ConnectRej event) -> {
        throw new RuntimeException("not implemented");
      };

    static FSMBasicEventHandler timerPing = new FSMBasicEventHandler<ES, IS, R1TransferLeecherPing>() {
      @Override
      public FSMStateName handle(FSMStateName state, ES es, IS is, R1TransferLeecherPing event) throws FSMException {
        is.pingTracker.timerPing();
        if (!is.pingTracker.healthy()) {
          Handlers.killUploadComp.accept(es, is);
          sendCtrl(es, is, new R1TransferLeecherEvents.Disconnected(is.torrentId, is.fileId, is.leecherAdr.getId()));
          sendMsg(es, is, new R1TransferConnMsgs.Disconnect(is.torrentId, is.fileId));
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
          Handlers.killUploadComp.accept(es, is);
          sendMsg(es, is, new R1TransferConnMsgs.Disconnect(is.torrentId, is.fileId));
          cancelPing(es, is);
          return FSMBasicStateNames.FINAL;
        }
      };

    static FSMPatternEventHandler netDisconnect1
      = new FSMPatternEventHandler<ES, IS, R1TransferConnMsgs.Disconnect, BasicContentMsg>() {

        @Override
        public FSMStateName handle(FSMStateName state, ES es, IS is, R1TransferConnMsgs.Disconnect payload,
          BasicContentMsg container) throws FSMException {
          sendCtrl(es, is, new R1TransferLeecherEvents.Disconnected(is.torrentId, is.fileId, is.leecherAdr.getId()));
          return FSMBasicStateNames.FINAL;
        }
      };

    static FSMPatternEventHandler netDisconnect2
      = new FSMPatternEventHandler<ES, IS, R1TransferConnMsgs.Disconnect, BasicContentMsg>() {

        @Override
        public FSMStateName handle(FSMStateName state, ES es, IS is, R1TransferConnMsgs.Disconnect payload,
          BasicContentMsg container) throws FSMException {
          Handlers.killUploadComp.accept(es, is);
          sendCtrl(es, is, new R1TransferLeecherEvents.Disconnected(is.torrentId, is.fileId, is.leecherAdr.getId()));
          cancelPing(es, is);
          return FSMBasicStateNames.FINAL;
        }
      };

    private static <C extends KompicsEvent & Identifiable> void sendMsg(ES es, IS is, C content) {
      KHeader header = new BasicHeader(es.selfAdr, is.leecherAdr, Transport.UDP);
      KContentMsg msg = new BasicContentMsg(header, content);
      es.proxy.trigger(msg, es.ports.network());
    }

    private static void sendCtrl(ES es, IS is, R1FileUpload.ConnectEvent content) {
      es.proxy.trigger(content, es.ports.transferLeecherCtrlReq);
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

    static BiConsumer<ES, IS> createUploadComp = new BiConsumer<ES, IS>() {

      @Override
      public void accept(ES es, IS is) {
        Optional<R1TorrentDetails> torrent = es.torrentDetailsMngr.getTorrent(is.torrentId);
        if (!torrent.isPresent()) {
          throw new RuntimeException("ups");
        }
        Optional<R1FileMetadata> fileMetadata = torrent.get().getMetadata(is.fileId);
        if (!fileMetadata.isPresent()) {
          throw new RuntimeException("ups");
        }
        R1UploadComp.Init init = new R1UploadComp.Init(es.selfAdr, is.torrentId, is.fileId, is.leecherAdr,
          fileMetadata.get().defaultBlock);
        Component uploadComp = es.proxy.create(R1UploadComp.class, init);
        Identifier uploadId = R1UploadComp.baseId(is.torrentId, is.fileId, is.leecherAdr.getId());
        es.proxy.connect(es.ports.timer, uploadComp.getNegative(Timer.class), Channel.TWO_WAY);
        es.ports.transferUploadC.addChannel(uploadId, uploadComp.getNegative(R1UploadPort.class));
        es.ports.netTransferUploadC.addChannel(uploadId, uploadComp.getNegative(Network.class));
        es.proxy.trigger(Start.event, uploadComp.control());
        is.uploadComp = uploadComp;
      }
    };

    static BiConsumer<ES, IS> killUploadComp = new BiConsumer<ES, IS>() {
      @Override
      public void accept(ES es, IS is) {
        Identifier uploadId = R1UploadComp.baseId(is.torrentId, is.fileId, is.leecherAdr.getId());
        Component uploadComp = is.uploadComp;
        es.proxy.trigger(Kill.event, uploadComp.control());
        es.proxy.disconnect(es.ports.timer, uploadComp.getNegative(Timer.class));
        es.ports.transferUploadC.removeChannel(uploadId, uploadComp.getNegative(R1UploadPort.class));
        es.ports.netTransferUploadC.removeChannel(uploadId, uploadComp.getNegative(Network.class));
        is.uploadComp = null;
      }
    };
  }
}
