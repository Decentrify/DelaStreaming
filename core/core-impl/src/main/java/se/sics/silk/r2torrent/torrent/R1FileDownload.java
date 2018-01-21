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
package se.sics.silk.r2torrent.torrent;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.function.Consumer;
import org.javatuples.Pair;
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
import se.sics.kompics.fsm.id.FSMIdentifier;
import se.sics.kompics.fsm.id.FSMIdentifierFactory;
import se.sics.kompics.network.Network;
import se.sics.kompics.timer.Timer;
import se.sics.kompics.util.Identifiable;
import se.sics.kompics.util.Identifier;
import se.sics.ktoolbox.util.identifiable.BasicBuilders;
import se.sics.ktoolbox.util.identifiable.basic.IntIdFactory;
import se.sics.ktoolbox.util.identifiable.basic.PairIdentifier;
import se.sics.ktoolbox.util.identifiable.overlay.OverlayId;
import se.sics.ktoolbox.util.network.KAddress;
import se.sics.nstream.StreamId;
import se.sics.nstream.storage.durable.DStreamControlPort;
import se.sics.nstream.storage.durable.events.DStreamConnect;
import se.sics.nstream.storage.durable.events.DStreamDisconnect;
import se.sics.nstream.storage.durable.events.DStreamEvent;
import se.sics.nstream.util.BlockDetails;
import se.sics.silk.DefaultHandlers;
import se.sics.silk.SelfPort;
import se.sics.silk.event.SilkEvent;
import se.sics.silk.r2torrent.R2TorrentComp;
import se.sics.silk.r2torrent.R2TorrentES;
import static se.sics.silk.r2torrent.torrent.R2Torrent.HardCodedConfig.seed;
import se.sics.silk.r2torrent.torrent.event.R1FileDownloadEvents;
import se.sics.silk.r2torrent.torrent.state.R1DownloadFileState;
import se.sics.silk.r2torrent.torrent.util.R1FileMngr;
import se.sics.silk.r2torrent.transfer.R1DownloadComp;
import se.sics.silk.r2torrent.transfer.R1DownloadPort;
import se.sics.silk.r2torrent.transfer.R1TransferSeeder;
import se.sics.silk.r2torrent.transfer.events.R1TransferSeederEvents;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class R1FileDownload {

  private static final Logger LOG = LoggerFactory.getLogger(FSM.class);
  public static final String NAME = "dela-r1-torrent-file-download-fsm";

  public static enum States implements FSMStateName {

    STORAGE_PENDING,
    STORAGE_SUCC,
    ACTIVE,
    IDLE,
    CLOSE,
    COMPLETED
  }

  public static interface Event extends FSMEvent, Identifiable, SilkEvent.TorrentEvent, SilkEvent.FileEvent {
  }

  public static interface CtrlEvent extends Event {
  }

  public static interface DownloadEvent extends Event {
  }

  public static interface ConnectEvent extends Event {
  }

  public static Identifier fsmBaseId(OverlayId torrentId, Identifier fileId) {
    return new PairIdentifier(torrentId, fileId);
  }

  public static class HardCodedConfig {
  }

  public static class IS implements FSMInternalState {

    private final FSMIdentifier fsmId;
    StreamId streamId;
    OverlayId torrentId;
    Identifier fileId;
    R1DownloadFileState fileState;
    Map<Identifier, Component> comps = new HashMap<>();

    public IS(FSMIdentifier fsmId) {
      this.fsmId = fsmId;
    }

    @Override
    public FSMIdentifier getFSMId() {
      return fsmId;
    }

    public void init(R1FileDownloadEvents.Start req) {
      this.streamId = req.streamId;
      this.torrentId = req.torrentId;
      this.fileId = req.fileId;
      this.fileState = new R1DownloadFileState();
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
    IntIdFactory fileIdFactory;
    KAddress selfAdr;
    int blockSlots;
    BlockDetails defaultBlock;
    R1FileMngr fileMngr;

    public ES(KAddress selfAdr, int blockSlots, BlockDetails defaultBlock, R1FileMngr fileMngr) {
      this.selfAdr = selfAdr;
      this.fileIdFactory = new IntIdFactory(new Random(seed));
      this.blockSlots = blockSlots;
      this.defaultBlock = defaultBlock;
      this.fileMngr = fileMngr;
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
        .nextStates(States.STORAGE_PENDING)
        .buildTransition()
        .onState(States.STORAGE_PENDING)
        .nextStates(States.STORAGE_SUCC, States.CLOSE)
        .buildTransition()
        .onState(States.STORAGE_SUCC)
        .nextStates(States.ACTIVE, States.CLOSE)
        .buildTransition()
        .onState(States.ACTIVE)
        .nextStates(States.ACTIVE, States.IDLE, States.CLOSE)
        .buildTransition()
        .onState(States.IDLE)
        .nextStates(States.IDLE, States.ACTIVE, States.CLOSE)
        .buildTransition()
        .onState(States.CLOSE)
        .toFinal()
        .buildTransition();
    }

    private static FSMBuilder.SemanticDefinition semanticDef() throws FSMException {
      FSMBuilder.SemanticDefinition def = FSMBuilder.semanticDef()
        .defaultFallback(DefaultHandlers.basicDefault(), DefaultHandlers.patternDefault());
      def = def
        .positivePort(SelfPort.class)
        .basicEvent(R1FileDownloadEvents.Start.class)
        .subscribeOnStart(Handlers.fileStart)
        .basicEvent(R1FileDownloadEvents.Close.class)
        .subscribe(Handlers.fileClose0, States.STORAGE_PENDING, States.STORAGE_SUCC)
        .subscribe(Handlers.fileClose1, States.ACTIVE, States.IDLE)
        .basicEvent(R1FileDownloadEvents.Connect.class)
        .subscribe(Handlers.connect, States.STORAGE_SUCC, States.ACTIVE, States.IDLE)
        .basicEvent(R1FileDownloadEvents.Disconnect.class)
        .subscribe(Handlers.disconnectSeeder, States.ACTIVE)
        .basicEvent(R1TransferSeederEvents.Connected.class)
        .subscribe(Handlers.connected, States.ACTIVE)
        .basicEvent(R1TransferSeederEvents.Disconnected.class)
        .subscribe(Handlers.disconnected, States.ACTIVE)
        .buildEvents();

      def = def
        .positivePort(DStreamControlPort.class)
        .basicEvent(DStreamConnect.Success.class)
        .subscribe(Handlers.streamOpened, States.STORAGE_PENDING)
        .basicEvent(DStreamDisconnect.Success.class)
        .subscribe(Handlers.streamClosed, States.CLOSE)
        .buildEvents();
//      def = def
//        .positivePort(DownloadPort.class)
//        .basicEvent(DownloadEvents.Block.class)
//        .subscribe(Handlers.blockCompleted, States.ACTIVE, States.IDLE)
////        .basicEvent(DownloadEvents.Stopped.class)
////        .subscribe(Handlers.downloadStopped1, States.ACTIVE, States.IDLE)
////        .subscribe(Handlers.downloadStopped2, States.CLOSE)
//        .buildEvents();
      return def;
    }

    static BaseIdExtractor baseIdExtractor(ES es) {
      return (KompicsEvent event) -> {
        if (event instanceof Event) {
          Event e = (Event) event;
          return Optional.of(fsmBaseId(e.torrentId(), e.fileId()));
        } else if (event instanceof DStreamConnect.Success) {
          DStreamConnect.Success e = (DStreamConnect.Success) event;
          OverlayId torrentId = e.req.stream.getValue0().fileId.torrentId;
          Identifier fileId = es.fileIdFactory.
            id(new BasicBuilders.IntBuilder(e.req.stream.getValue0().fileId.fileNr));
          return Optional.of(fsmBaseId(torrentId, fileId));
        } else if (event instanceof DStreamDisconnect.Success) {
          DStreamDisconnect.Success e = (DStreamDisconnect.Success) event;
          OverlayId torrentId = e.req.streamId.fileId.torrentId;
          Identifier fileId = es.fileIdFactory.
            id(new BasicBuilders.IntBuilder(e.req.streamId.fileId.fileNr));
          return Optional.of(fsmBaseId(torrentId, fileId));
        } else {
          return Optional.empty();
        }
      };
    }

    public static MultiFSM multifsm(FSMIdentifierFactory fsmIdFactory, ES es, OnFSMExceptionAction oexa)
      throws FSMException {
      FSMInternalStateBuilder isb = new ISBuilder();
      return FSMBuilder.multiFSM(fsmIdFactory, NAME, structuralDef(), semanticDef(), es, isb, oexa, baseIdExtractor(es));
    }
  }

  public static class Handlers {

    static FSMBasicEventHandler fileStart = (FSMBasicEventHandler<ES, IS, R1FileDownloadEvents.Start>) (
      FSMStateName state, ES es, IS is, R1FileDownloadEvents.Start event) -> {
        is.init(event);
        sendStreamEvent(es, new DStreamConnect.Request(Pair.with(event.streamId, event.stream)));
        return States.STORAGE_PENDING;
      };

    static FSMBasicEventHandler fileClose0 = (FSMBasicEventHandler<ES, IS, R1FileDownloadEvents.Close>) (
      FSMStateName state, ES es, IS is, R1FileDownloadEvents.Close event) -> {
        sendStreamEvent(es, new DStreamDisconnect.Request(is.streamId));
        return States.CLOSE;
      };

    static FSMBasicEventHandler fileClose1 = (FSMBasicEventHandler<ES, IS, R1FileDownloadEvents.Close>) (
      FSMStateName state, ES es, IS is, R1FileDownloadEvents.Close event) -> {
        is.fileState.allConnected(disconnect(es, is)); //disconnect download comp
        is.fileState.connectedToPending();
        is.fileState.clearPending();
        //disconnect storage
        DStreamDisconnect.Request req = new DStreamDisconnect.Request(is.streamId);
        sendStreamEvent(es, req);
        //indicate closing
        R1FileDownloadEvents.Indication ind = new R1FileDownloadEvents.Indication(is.torrentId, is.fileId,
          States.CLOSE);
        sendCtrlEvent(es, ind);
        return States.CLOSE;
      };

    //*******************************************************STORAGE_PENDING****************************************************
    static FSMBasicEventHandler streamOpened = (FSMBasicEventHandler<ES, IS, DStreamConnect.Success>) (
      FSMStateName state, ES es, IS is, DStreamConnect.Success event) -> {
        R1FileDownloadEvents.Indication ind = new R1FileDownloadEvents.Indication(is.torrentId, is.fileId,
          States.STORAGE_SUCC);
        sendCtrlEvent(es, ind);
        return States.STORAGE_SUCC;
      };

    static FSMBasicEventHandler streamClosed = (FSMBasicEventHandler<ES, IS, DStreamDisconnect.Success>) (
      FSMStateName state, ES es, IS is, DStreamDisconnect.Success event) -> {
        return FSMBasicStateNames.FINAL;
      };

    //*****************************************************SEEDERS******************************************************
    static FSMBasicEventHandler connect = (FSMBasicEventHandler<ES, IS, R1FileDownloadEvents.Connect>) (
      FSMStateName state, ES es, IS is, R1FileDownloadEvents.Connect event) -> {
        is.fileState.pendingSeeder(event.seeder);
        sendConnectEvent(es, new R1TransferSeederEvents.Connect(is.torrentId, is.fileId, event.seeder));
        return States.ACTIVE;
      };

    static FSMBasicEventHandler disconnectSeeder = (FSMBasicEventHandler<ES, IS, R1FileDownloadEvents.Disconnect>) (
      FSMStateName state, ES es, IS is, R1FileDownloadEvents.Disconnect event) -> {
        is.fileState.disconnectedSeeder(event.seederId, disconnect(es, is));
        if(is.fileState.inactive()) {
          sendCtrlEvent(es, new R1FileDownloadEvents.Indication(is.torrentId, is.fileId, States.IDLE));
          return States.IDLE;
        }
        return state;
      };
    //*******************************************************CONNECT****************************************************
    static FSMBasicEventHandler connected = (FSMBasicEventHandler<ES, IS, R1TransferSeederEvents.Connected>) (
      FSMStateName state, ES es, IS is, R1TransferSeederEvents.Connected event) -> {
        is.fileState.connectedSeeder(event.seeder);
        connect(es, is).accept(event.seeder);
        return state;
      };

    static FSMBasicEventHandler disconnected = (FSMBasicEventHandler<ES, IS, R1TransferSeederEvents.Disconnected>) (
      FSMStateName state, ES es, IS is, R1TransferSeederEvents.Disconnected event) -> {
        is.fileState.disconnectedSeeder(event.seeder.getId(), disconnect(es, is));
        if(is.fileState.inactive()) {
          sendCtrlEvent(es, new R1FileDownloadEvents.Indication(is.torrentId, is.fileId, States.IDLE));
          return States.IDLE;
        }
        return state;
      };

    //******************************************************DOWNLOAD****************************************************
//    static FSMBasicEventHandler blockCompleted = (FSMBasicEventHandler<ES, IS, DownloadEvents.Block>) (
//      FSMStateName state, ES es, IS is, DownloadEvents.Block event) -> {
//        disconnect(es, is).accept(event.seeder);
//        return States.CLOSE;
//      };
//    static FSMBasicEventHandler downloadStopped1 = (FSMBasicEventHandler<ES, IS, DownloadEvents.Stopped>) (
//      FSMStateName state, ES es, IS is, DownloadEvents.Stopped event) -> {
//        R1FileDownloadEvents.Disconnected ind = new R1FileDownloadEvents.Disconnected(is.torrentId, is.fileId, event.nodeId);
//        sendCtrlEvent(es, ind);
//        killDownloadComp(es, is, event.seeder);
//        return state;
//      };
//
//    static FSMBasicEventHandler downloadStopped2 = (FSMBasicEventHandler<ES, IS, DownloadEvents.Stopped>) (
//      FSMStateName state, ES es, IS is, DownloadEvents.Stopped event) -> {
//        R1FileDownloadEvents.Disconnected ind = new R1FileDownloadEvents.Disconnected(is.torrentId, is.fileId, event.nodeId);
//        sendCtrlEvent(es, ind);
//        killDownloadComp(es, is, event.seeder);
//        if (is.comps.isEmpty()) {
//          R1FileDownloadEvents.Indication ind2 = new R1FileDownloadEvents.Indication(is.torrentId, is.fileId, FileStatus.COMPLETED);
//          sendCtrlEvent(es, ind2);
//          return FSMBasicStateNames.FINAL;
//        }
//        return state;
//      };
    private static Consumer<KAddress> connect(ES es, IS is) {
      return (KAddress seederAdr) -> {
        R1DownloadComp.Init init = new R1DownloadComp.Init(es.selfAdr, is.torrentId, is.fileId, seederAdr,
          es.blockSlots, es.defaultBlock);
        Component downloadComp = es.proxy.create(R1DownloadComp.class, init);
        Identifier downloadId = R1DownloadComp.baseId(is.torrentId, is.fileId, seederAdr.getId());
        es.proxy.connect(es.ports.timer, downloadComp.getNegative(Timer.class), Channel.TWO_WAY);
        es.ports.downloadC.addChannel(downloadId, downloadComp.getPositive(R1DownloadPort.class));
        es.ports.netDownloadC.addChannel(downloadId, downloadComp.getNegative(Network.class));
        es.proxy.trigger(Start.event, downloadComp.control());
        Identifier compId = R1DownloadComp.baseId(is.torrentId, is.fileId, seederAdr.getId());
        is.comps.put(compId, downloadComp);
      };
    }

    private static Consumer<KAddress> disconnect(ES es, IS is) {
      return (KAddress seeder) -> {
        Identifier compId = R1DownloadComp.baseId(is.torrentId, is.fileId, seeder.getId());
        if (is.comps.containsKey(compId)) {
          killDownloadComp(es, is, seeder);
          R1TransferSeederEvents.Disconnect disconnect
            = new R1TransferSeederEvents.Disconnect(is.torrentId, is.fileId, seeder.getId());
          sendConnectEvent(es, disconnect);
        }
        R1FileDownloadEvents.Disconnected ind = new R1FileDownloadEvents.Disconnected(is.torrentId, is.fileId, seeder.
          getId());
        sendCtrlEvent(es, ind);
      };
    }

    private static void killDownloadComp(ES es, IS is, KAddress seeder) {
      Identifier downloadId = R1DownloadComp.baseId(is.torrentId, is.fileId, seeder.getId());
      Component downloadComp = is.comps.remove(downloadId);
      es.proxy.trigger(Kill.event, downloadComp.control());
      es.proxy.disconnect(es.ports.timer, downloadComp.getNegative(Timer.class));
      es.ports.downloadC.removeChannel(downloadId, downloadComp.getPositive(R1DownloadPort.class));
      es.ports.netDownloadC.removeChannel(downloadId, downloadComp.getNegative(Network.class));
    }

    private static void sendStreamEvent(ES es, DStreamEvent e) {
      es.getProxy().trigger(e, es.ports.streamCtrl);
    }

    private static void sendConnectEvent(ES es, R1TransferSeeder.CtrlEvent e) {
      es.getProxy().trigger(e, es.ports.loopbackSend);
    }

    private static void sendCtrlEvent(ES es, KompicsEvent e) {
      es.getProxy().trigger(e, es.ports.loopbackSend);
    }

    private static void sendDownloadEvent(ES es, DownloadEvent e) {
      es.getProxy().trigger(e, es.ports.download);
    }
  }
}
