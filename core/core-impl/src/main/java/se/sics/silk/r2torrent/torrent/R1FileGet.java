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
import se.sics.silk.DefaultHandlers;
import se.sics.silk.SelfPort;
import se.sics.silk.event.SilkEvent;
import se.sics.silk.r2torrent.R2TorrentComp;
import se.sics.silk.r2torrent.R2TorrentES;
import static se.sics.silk.r2torrent.torrent.R2Torrent.HardCodedConfig.seed;
import se.sics.silk.r2torrent.torrent.event.R1FileGetEvents;
import se.sics.silk.r2torrent.torrent.state.FileState;
import se.sics.silk.r2torrent.torrent.state.FileStatus;
import se.sics.silk.r2torrent.transfer.DownloadPort;
import se.sics.silk.r2torrent.transfer.R1TransferSeederComp;
import se.sics.silk.r2torrent.transfer.events.DownloadEvents;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class R1FileGet {

  private static final Logger LOG = LoggerFactory.getLogger(FSM.class);
  public static final String NAME = "dela-r1-torrent-file-fsm";

  public static enum States implements FSMStateName {

    STORAGE,
    ACTIVE,
    PAUSED,
    CLOSE
  }

  public static interface Event extends FSMEvent, Identifiable, SilkEvent.TorrentEvent, SilkEvent.FileEvent {
  }

  public static interface CtrlEvent extends Event {
  }

  public static interface DownloadEvent extends Event {
  }

  public static Identifier fsmBaseId(OverlayId torrentId, Identifier fileId) {
    return new PairIdentifier(torrentId, fileId);
  }

  public static class IS implements FSMInternalState {

    private final FSMIdentifier fsmId;
    StreamId streamId;
    OverlayId torrentId;
    Identifier fileId;
    FileState fileState;
    Map<Identifier, Component> comps = new HashMap<>();

    public IS(FSMIdentifier fsmId) {
      this.fsmId = fsmId;
    }

    @Override
    public FSMIdentifier getFSMId() {
      return fsmId;
    }

    public void start(R1FileGetEvents.Start req) {
      this.streamId = req.streamId;
      this.torrentId = req.torrentId;
      this.fileId = req.fileId;
      this.fileState = new FileState();
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

    public ES(KAddress selfAdr) {
      this.selfAdr = selfAdr;
      this.fileIdFactory = new IntIdFactory(new Random(seed));
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
        .nextStates(States.STORAGE)
        .buildTransition()
        .onState(States.STORAGE)
        .nextStates(States.ACTIVE, States.CLOSE)
        .buildTransition()
        .onState(States.ACTIVE)
        .nextStates(States.PAUSED, States.CLOSE)
        .buildTransition()
        .onState(States.PAUSED)
        .nextStates(States.ACTIVE, States.CLOSE)
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
        .basicEvent(R1FileGetEvents.Start.class)
        .subscribeOnStart(Handlers.fileStart)
        .basicEvent(R1FileGetEvents.Close.class)
        .subscribe(Handlers.fileClose, States.STORAGE, States.ACTIVE)
        .basicEvent(R1FileGetEvents.Connect.class)
        .subscribe(Handlers.connectSeederActive, States.ACTIVE)
        .subscribe(Handlers.connectSeederPaused, States.PAUSED)
        .basicEvent(R1FileGetEvents.Disconnect.class)
        .subscribe(Handlers.disconnectSeeder, States.ACTIVE, States.PAUSED)
        .buildEvents();

      def = def
        .positivePort(DStreamControlPort.class)
        .basicEvent(DStreamConnect.Success.class)
        .subscribe(Handlers.streamOpened, States.STORAGE)
        .basicEvent(DStreamDisconnect.Success.class)
        .subscribe(Handlers.streamClosed, States.CLOSE)
        .buildEvents();

      def = def
        .positivePort(DownloadPort.class)
        .basicEvent(DownloadEvents.Block.class)
        .subscribe(Handlers.blockCompleted, States.ACTIVE, States.PAUSED)
//        .basicEvent(DownloadEvents.Stopped.class)
//        .subscribe(Handlers.downloadStopped1, States.ACTIVE, States.PAUSED)
//        .subscribe(Handlers.downloadStopped2, States.CLOSE)
        .buildEvents();
      return def;
    }

    static BaseIdExtractor baseIdExtractor(ES es) {
      return (KompicsEvent event) -> {
        if (event instanceof CtrlEvent) {
          CtrlEvent e = (CtrlEvent) event;
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

    static FSMBasicEventHandler fileStart = (FSMBasicEventHandler<ES, IS, R1FileGetEvents.Start>) (
      FSMStateName state, ES es, IS is, R1FileGetEvents.Start event) -> {
        is.start(event);
        DStreamConnect.Request req = new DStreamConnect.Request(Pair.with(event.streamId, event.stream));
        sendStreamEvent(es, req);
        return States.STORAGE;
      };

    static FSMBasicEventHandler fileClose = (FSMBasicEventHandler<ES, IS, R1FileGetEvents.Close>) (
      FSMStateName state, ES es, IS is, R1FileGetEvents.Close event) -> {
        pause(es, is);
        is.fileState.clearPending();
        DStreamDisconnect.Request req = new DStreamDisconnect.Request(is.streamId);
        sendStreamEvent(es, req);
        return States.CLOSE;
      };

    static FSMBasicEventHandler filePause = (FSMBasicEventHandler<ES, IS, R1FileGetEvents.Close>) (
      FSMStateName state, ES es, IS is, R1FileGetEvents.Close event) -> {
        pause(es, is);
        return States.PAUSED;
      };

    static FSMBasicEventHandler fileResume = (FSMBasicEventHandler<ES, IS, R1FileGetEvents.Close>) (
      FSMStateName state, ES es, IS is, R1FileGetEvents.Close event) -> {
        resume(es, is);
        return States.ACTIVE;
      };

    private static void pause(ES es, IS is) {
      is.fileState.allConnected(disconnect(es, is));
      is.fileState.connectedToPending();
      R1FileGetEvents.Indication ind = new R1FileGetEvents.Indication(is.torrentId, is.fileId, FileStatus.PAUSED);
      sendCtrlEvent(es, ind);
    }

    private static void resume(ES es, IS is) {
      is.fileState.allPending(connect(es, is));
      R1FileGetEvents.Indication ind = new R1FileGetEvents.Indication(is.torrentId, is.fileId, FileStatus.ACTIVE);
      sendCtrlEvent(es, ind);
    }

    //*****************************************************SEEDERS******************************************************
    static FSMBasicEventHandler connectSeederPaused = (FSMBasicEventHandler<ES, IS, R1FileGetEvents.Connect>) (
      FSMStateName state, ES es, IS is, R1FileGetEvents.Connect event) -> {
        is.fileState.seederConnect(event.seeder);
        return state;
      };

    static FSMBasicEventHandler connectSeederActive = (FSMBasicEventHandler<ES, IS, R1FileGetEvents.Connect>) (
      FSMStateName state, ES es, IS is, R1FileGetEvents.Connect event) -> {
        is.fileState.seederConnect(event.seeder);
        connect(es, is).accept(event.seeder);
        return state;
      };

    static FSMBasicEventHandler disconnectSeeder = (FSMBasicEventHandler<ES, IS, R1FileGetEvents.Disconnect>) (
      FSMStateName state, ES es, IS is, R1FileGetEvents.Disconnect event) -> {
        is.fileState.seederDisconnect(event.seederId, disconnect(es, is));
        return state;
      };

    //******************************************************DOWNLOAD****************************************************
    static FSMBasicEventHandler blockCompleted = (FSMBasicEventHandler<ES, IS, DownloadEvents.Block>) (
      FSMStateName state, ES es, IS is, DownloadEvents.Block event) -> {
        disconnect(es, is).accept(event.seeder);
        return States.CLOSE;
      };

//    static FSMBasicEventHandler downloadStopped1 = (FSMBasicEventHandler<ES, IS, DownloadEvents.Stopped>) (
//      FSMStateName state, ES es, IS is, DownloadEvents.Stopped event) -> {
//        R1FileGetEvents.Disconnected ind = new R1FileGetEvents.Disconnected(is.torrentId, is.fileId, event.nodeId);
//        sendCtrlEvent(es, ind);
//        killDownloadComp(es, is, event.seeder);
//        return state;
//      };
//
//    static FSMBasicEventHandler downloadStopped2 = (FSMBasicEventHandler<ES, IS, DownloadEvents.Stopped>) (
//      FSMStateName state, ES es, IS is, DownloadEvents.Stopped event) -> {
//        R1FileGetEvents.Disconnected ind = new R1FileGetEvents.Disconnected(is.torrentId, is.fileId, event.nodeId);
//        sendCtrlEvent(es, ind);
//        killDownloadComp(es, is, event.seeder);
//        if (is.comps.isEmpty()) {
//          R1FileGetEvents.Indication ind2 = new R1FileGetEvents.Indication(is.torrentId, is.fileId, FileStatus.COMPLETED);
//          sendCtrlEvent(es, ind2);
//          return FSMBasicStateNames.FINAL;
//        }
//        return state;
//      };

    private static Consumer<KAddress> connect(ES es, IS is) {
      return (KAddress seederAdr) -> {
        R1TransferSeederComp.Init init = new R1TransferSeederComp.Init(es.selfAdr, is.torrentId, is.fileId, seederAdr);
        Component downloadComp = es.proxy.create(R1TransferSeederComp.class, init);
        Identifier downloadId = R1TransferSeederComp.baseId(is.torrentId, is.fileId, seederAdr.getId());
        es.ports.downloadC.addChannel(downloadId, downloadComp.getNegative(DownloadPort.class));
        es.proxy.trigger(Start.event, downloadComp.control());
        Identifier compId = R1TransferSeederComp.baseId(is.torrentId, is.fileId, seederAdr.getId());
        is.comps.put(compId, downloadComp);
      };
    }

    private static Consumer<KAddress> disconnect(ES es, IS is) {
      return (KAddress seeder) -> {
        Identifier compId = R1TransferSeederComp.baseId(is.torrentId, is.fileId, seeder.getId());
        if (!is.comps.containsKey(compId)) {
          R1FileGetEvents.Disconnected ind = new R1FileGetEvents.Disconnected(is.torrentId, is.fileId, seeder.getId());
          sendCtrlEvent(es, ind);
        } else {
//          DownloadEvents.Stop req = new DownloadEvents.Stop(is.torrentId, is.fileId, seeder.getId());
//          es.proxy.trigger(req, es.ports.download);
        }
      };
    }

    private static void killDownloadComp(ES es, IS is, KAddress seeder) {
      Identifier compId = R1TransferSeederComp.baseId(is.torrentId, is.fileId, seeder.getId());
      Component downloadComp = is.comps.remove(compId);
      es.proxy.trigger(Kill.event, downloadComp.control());
      es.ports.downloadC.removeChannel(compId, downloadComp.getNegative(DownloadPort.class));
    }
    //*******************************************************STORAGE****************************************************
    static FSMBasicEventHandler streamOpened = (FSMBasicEventHandler<ES, IS, DStreamConnect.Success>) (
      FSMStateName state, ES es, IS is, DStreamConnect.Success event) -> {
        resume(es, is);
        return States.ACTIVE;
      };

    static FSMBasicEventHandler streamClosed = (FSMBasicEventHandler<ES, IS, DStreamDisconnect.Success>) (
      FSMStateName state, ES es, IS is, DStreamDisconnect.Success event) -> {
        R1FileGetEvents.Indication ind = new R1FileGetEvents.Indication(is.torrentId, is.fileId, FileStatus.CLOSED);
        sendCtrlEvent(es, ind);
        return FSMBasicStateNames.FINAL;
      };

    private static void sendStreamEvent(ES es, DStreamEvent e) {
      es.getProxy().trigger(e, es.ports.streamCtrl);
    }

    private static void sendCtrlEvent(ES es, KompicsEvent e) {
      es.getProxy().trigger(e, es.ports.loopbackSend);
    }

    private static void sendDownloadEvent(ES es, DownloadEvent e) {
      es.getProxy().trigger(e, es.ports.download);
    }
  }
}
