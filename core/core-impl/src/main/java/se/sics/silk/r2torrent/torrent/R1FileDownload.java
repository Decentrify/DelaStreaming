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
import java.util.Set;
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
import se.sics.nstream.storage.durable.DStoragePort;
import se.sics.nstream.storage.durable.DStreamControlPort;
import se.sics.nstream.storage.durable.events.DStorageWrite;
import se.sics.nstream.storage.durable.events.DStreamConnect;
import se.sics.nstream.storage.durable.events.DStreamDisconnect;
import se.sics.nstream.storage.durable.events.DStreamEvent;
import se.sics.silk.DefaultHandlers;
import se.sics.silk.SelfPort;
import se.sics.silk.event.SilkEvent;
import se.sics.silk.r2torrent.R2TorrentComp;
import se.sics.silk.r2torrent.R2TorrentES;
import se.sics.silk.r2torrent.storage.buffer.R1AsyncAppendBuffer;
import se.sics.silk.r2torrent.storage.buffer.R1AsyncBuffer;
import se.sics.silk.r2torrent.storage.buffer.R1AsyncCheckedBuffer;
import se.sics.silk.r2torrent.storage.sink.R1SinkWriteCallback;
import se.sics.silk.r2torrent.storage.sink.R1SinkWriter;
import static se.sics.silk.r2torrent.torrent.R1Torrent.HardCodedConfig.seed;
import se.sics.silk.r2torrent.torrent.event.R1FileDownloadEvents;
import se.sics.silk.r2torrent.torrent.state.R1FileDownloadSeederState;
import se.sics.silk.r2torrent.torrent.state.R1FileDownloadSeedersState;
import se.sics.silk.r2torrent.torrent.util.R1BlockHelper;
import se.sics.silk.r2torrent.torrent.util.R1FileDownloadTracker;
import se.sics.silk.r2torrent.torrent.util.R1FileMetadata;
import se.sics.silk.r2torrent.torrent.util.R1FileStorage;
import se.sics.silk.r2torrent.torrent.util.R1TorrentDetails;
import se.sics.silk.r2torrent.transfer.R1DownloadPort;
import se.sics.silk.r2torrent.transfer.R1TransferSeeder;
import se.sics.silk.r2torrent.transfer.R1TransferSeederCtrl;
import se.sics.silk.r2torrent.transfer.events.R1DownloadEvent;
import se.sics.silk.r2torrent.transfer.events.R1DownloadEvents;
import se.sics.silk.r2torrent.transfer.events.R1TransferSeederEvents;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class R1FileDownload {

  private static final Logger LOG = LoggerFactory.getLogger(R1FileDownload.class);
  public static final String NAME = "dela-r1-torrent-file-download-fsm";

  public static enum States implements FSMStateName {

    STORAGE_PENDING,
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

    public static int BLOCK_BATCH_REQUEST = 10;
    public static int DOWNLOAD_COMP_BUFFER_SIZE = 30;
  }

  public static class IS implements FSMInternalState {

    private final FSMIdentifier fsmId;
    public OverlayId torrentId;
    public Identifier fileId;
    public R1TorrentDetails torrentDetails;
    public R1FileMetadata fileMetadata;
    public R1FileStorage fileStorage;
    public R1FileDownloadSeedersState seedersState;
    public StreamActions streamActions;

    public IS(FSMIdentifier fsmId) {
      this.fsmId = fsmId;
    }

    @Override
    public FSMIdentifier getFSMId() {
      return fsmId;
    }

    public void init(R1TorrentDetails torrentDetails, R1FileDownloadEvents.Start req) {
      this.torrentId = req.torrentId;
      this.fileId = req.fileId;
      this.torrentDetails = torrentDetails;
      this.fileMetadata = torrentDetails.getMetadata(fileId).get();
      this.fileStorage = torrentDetails.getStorage(fileId).get();
    }

    public void startSeeders(ES es, DStreamConnect.Success resp) {
      streamActions = new StreamActions(es);
      R1AsyncBuffer bAux = new R1AsyncAppendBuffer(fileStorage.streamId, streamActions);
      R1AsyncCheckedBuffer buffer = new R1AsyncCheckedBuffer(bAux, torrentDetails.hashAlg);
      int startBlock = R1BlockHelper.blockNrFromPos(resp.streamPos, fileMetadata);
      if (startBlock == fileMetadata.nrBlocks) {
        throw new RuntimeException("fileCompleted");
      }
      R1FileDownloadTracker fileTracker = new R1FileDownloadTracker(startBlock, fileMetadata.nrBlocks);
      this.seedersState = new R1FileDownloadSeedersState(fileMetadata, fileTracker, buffer, new SeederActions(es, this));
    }
  }

  public static class ISBuilder implements FSMInternalStateBuilder {

    public ISBuilder() {
    }

    @Override
    public FSMInternalState newState(FSMIdentifier fsmId) {
      return new IS(fsmId);
    }
  }

  public static class ES implements R2TorrentES {

    public ComponentProxy proxy;
    public R2TorrentComp.Ports ports;
    public IntIdFactory fileIdFactory;
    public KAddress selfAdr;
    public R1TorrentDetails.Mngr torrentDetailsMngr;

    public ES(KAddress selfAdr, R1TorrentDetails.Mngr torrentDetailsMngr) {
      this.selfAdr = selfAdr;
      this.fileIdFactory = new IntIdFactory(new Random(seed));
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
        .nextStates(States.STORAGE_PENDING)
        .buildTransition()
        .onState(States.STORAGE_PENDING)
        .nextStates(States.IDLE, States.CLOSE)
        .buildTransition()
        .onState(States.IDLE)
        .nextStates(States.IDLE, States.ACTIVE, States.CLOSE, States.COMPLETED)
        .buildTransition()
        .onState(States.ACTIVE)
        .nextStates(States.ACTIVE, States.IDLE, States.CLOSE, States.COMPLETED)
        .buildTransition()
        .onState(States.COMPLETED)
        .nextStates(States.COMPLETED, States.CLOSE)
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
        .basicEvent(R1FileDownloadEvents.Close.class)
        .subscribe(Handlers.fileClose0, States.STORAGE_PENDING)
        .subscribe(Handlers.fileClose1, States.ACTIVE, States.IDLE)
        .basicEvent(R1FileDownloadEvents.Disconnect.class)
        .subscribe(Handlers.disconnectSeeder, States.ACTIVE)
        .buildEvents();
      def = def
        .negativePort(R1FileDownloadCtrl.class)
        .basicEvent(R1FileDownloadEvents.Start.class)
        .subscribeOnStart(Handlers.fileStart)
        .basicEvent(R1FileDownloadEvents.Connect.class)
        .subscribe(Handlers.connect, States.ACTIVE, States.IDLE)
        .buildEvents();
      def = def
        .positivePort(DStreamControlPort.class)
        .basicEvent(DStreamConnect.Success.class)
        .subscribe(Handlers.streamOpened, States.STORAGE_PENDING)
        .basicEvent(DStreamDisconnect.Success.class)
        .subscribe(Handlers.streamClosed, States.CLOSE)
        .buildEvents();
      def = def
        .positivePort(DStoragePort.class)
        .basicEvent(DStorageWrite.Response.class)
        .subscribe(StreamActions.written, States.ACTIVE, States.IDLE, States.COMPLETED)
        .buildEvents();
      def = def
        .positivePort(R1DownloadPort.class)
        .basicEvent(R1DownloadEvents.Completed.class)
        .subscribe(Handlers.completed, States.ACTIVE, States.IDLE)
        .buildEvents();
      def = def
        .positivePort(R1TransferSeederCtrl.class)
        .basicEvent(R1TransferSeederEvents.Connected.class)
        .subscribe(Handlers.transferConnected, States.ACTIVE)
        .basicEvent(R1TransferSeederEvents.Disconnected.class)
        .subscribe(Handlers.transferDisconnected1, States.ACTIVE)
        .subscribe(Handlers.transferDisconnected2, States.COMPLETED)
        .buildEvents();
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
        } else if (event instanceof DStorageWrite.Response) {
          DStorageWrite.Response e = (DStorageWrite.Response) event;
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
        Optional<R1TorrentDetails> torrentDetails = es.torrentDetailsMngr.getTorrent(event.torrentId);
        if(!torrentDetails.isPresent()) {
          throw new RuntimeException("ups");
        }
        if(!torrentDetails.get().getMetadata(event.fileId).isPresent() 
          || !torrentDetails.get().getStorage(event.fileId).isPresent()) {
          throw new RuntimeException("ups");
        }
        is.init(torrentDetails.get(), event);
        LOG.info("<{},{}>start", new Object[]{is.torrentId.baseId, is.fileId});
        sendStorageConnect(es, is);
        return States.STORAGE_PENDING;
      };

    static FSMBasicEventHandler fileClose0 = (FSMBasicEventHandler<ES, IS, R1FileDownloadEvents.Close>) (
      FSMStateName state, ES es, IS is, R1FileDownloadEvents.Close event) -> {
        is.seedersState.disconnectPending();
        sendStorageDisconnect(es, is);
        return States.CLOSE;
      };

    static FSMBasicEventHandler fileClose1 = (FSMBasicEventHandler<ES, IS, R1FileDownloadEvents.Close>) (
      FSMStateName state, ES es, IS is, R1FileDownloadEvents.Close event) -> {
        is.seedersState.disconnect();
        sendStorageDisconnect(es, is);
        sendCtrlIndication(es, is, States.CLOSE);
        return States.CLOSE;
      };

    //*******************************************************STORAGE_PENDING****************************************************
    static FSMBasicEventHandler streamOpened = (FSMBasicEventHandler<ES, IS, DStreamConnect.Success>) (
      FSMStateName state, ES es, IS is, DStreamConnect.Success resp) -> {
        LOG.debug("<{},{}>stream opened", new Object[]{is.torrentId.baseId, is.fileId});
        is.startSeeders(es, resp);
        sendCtrlIndication(es, is, States.IDLE);
        return States.IDLE;
      };

    static FSMBasicEventHandler streamClosed = (FSMBasicEventHandler<ES, IS, DStreamDisconnect.Success>) (
      FSMStateName state, ES es, IS is, DStreamDisconnect.Success event) -> {
        return FSMBasicStateNames.FINAL;
      };

    //*****************************************************SEEDERS******************************************************
    static FSMBasicEventHandler connect = (FSMBasicEventHandler<ES, IS, R1FileDownloadEvents.Connect>) (
      FSMStateName state, ES es, IS is, R1FileDownloadEvents.Connect event) -> {
        LOG.debug("<{},{}>connect:{}", new Object[]{is.torrentId.baseId, is.fileId, event.seeder});
        is.seedersState.pending(event.seeder);
        sendTransferEvent(es, new R1TransferSeederEvents.Connect(is.torrentId, is.fileId, event.seeder, is.fileMetadata));
        return States.ACTIVE;
      };

    static FSMBasicEventHandler disconnectSeeder = (FSMBasicEventHandler<ES, IS, R1FileDownloadEvents.Disconnect>) (
      FSMStateName state, ES es, IS is, R1FileDownloadEvents.Disconnect event) -> {
        is.seedersState.disconnect();
        if (is.seedersState.inactive()) {
          sendCtrlIndication(es, is, States.IDLE);
          return States.IDLE;
        }
        return state;
      };
    //*******************************************************CONNECT****************************************************
    static FSMBasicEventHandler transferConnected = (FSMBasicEventHandler<ES, IS, R1TransferSeederEvents.Connected>) (
      FSMStateName state, ES es, IS is, R1TransferSeederEvents.Connected event) -> {
        is.seedersState.connectPending(event.seeder);
        return state;
      };

    static FSMBasicEventHandler transferDisconnected1
      = (FSMBasicEventHandler<ES, IS, R1TransferSeederEvents.Disconnected>) (FSMStateName state, ES es, IS is,
        R1TransferSeederEvents.Disconnected event) -> {
        is.seedersState.disconnect(event.seeder.getId());
        if (is.seedersState.inactive()) {
          sendCtrlIndication(es, is, States.IDLE);
          return States.IDLE;
        }
        return state;
      };
    
    static FSMBasicEventHandler transferDisconnected2
      = (FSMBasicEventHandler<ES, IS, R1TransferSeederEvents.Disconnected>) (FSMStateName state, ES es, IS is,
        R1TransferSeederEvents.Disconnected event) -> {
        is.seedersState.disconnect(event.seeder.getId());
        if (is.seedersState.inactive()) {
          sendStorageDisconnect(es, is);
          return States.CLOSE;
        }
        return state;
      };

    //******************************************************DOWNLOAD****************************************************
    static FSMBasicEventHandler completed = (FSMBasicEventHandler<ES, IS, R1DownloadEvents.Completed>) (
      FSMStateName state, ES es, IS is, R1DownloadEvents.Completed resp) -> {
        Optional<R1FileDownloadSeederState> stateAux = is.seedersState.state(resp.nodeId);
        if (stateAux.isPresent()) {
          stateAux.get().complete(resp.block, resp.value, resp.hash);
          if (is.seedersState.fileTracker.isComplete()) {
            is.seedersState.completed();
            sendCtrlIndication(es, is, States.COMPLETED);
            return States.COMPLETED;
          }
        }
        return state;
      };

    private static void sendStorageConnect(ES es, IS is) {
      sendStorageEvent(es, new DStreamConnect.Request(is.fileStorage.streamId, is.fileStorage.stream));
    }

    private static void sendStorageDisconnect(ES es, IS is) {
      sendStorageEvent(es, new DStreamDisconnect.Request(is.fileStorage.streamId));
    }

    private static void sendCtrlIndication(ES es, IS is, States state) {
      sendCtrlEvent(es, new R1FileDownloadEvents.Indication(is.torrentId, is.fileId, state));
    }

    private static void sendStorageEvent(ES es, DStreamEvent e) {
      es.getProxy().trigger(e, es.ports.streamCtrl);
    }

    private static void sendTransferEvent(ES es, R1TransferSeeder.CtrlEvent e) {
      es.getProxy().trigger(e, es.ports.transferSeederCtrlReq);
    }

    static void sendCtrlEvent(ES es, R1Torrent.DownloadCtrl e) {
      es.getProxy().trigger(e, es.ports.fileDownloadCtrlProv);
    }

    private static void sendDownloadEvent(ES es, R1DownloadEvent e) {
      es.getProxy().trigger(e, es.ports.transferDownload);
    }
  }

  public static class StreamActions implements R1SinkWriter {

    private final ES es;
    private Map<Identifier, R1SinkWriteCallback> callbacks = new HashMap<>();

    public StreamActions(ES es) {
      this.es = es;
    }

    @Override
    public void write(StreamId streamId, long pos, byte[] value, R1SinkWriteCallback callback) {
      DStorageWrite.Request req = new DStorageWrite.Request(streamId, pos, value);
      es.proxy.trigger(req, es.ports.storage);
      callbacks.put(req.getId(), callback);
    }

    static FSMBasicEventHandler written = (FSMBasicEventHandler<ES, IS, DStorageWrite.Response>) (
      FSMStateName state, ES es, IS is, DStorageWrite.Response resp) -> {
        R1SinkWriteCallback callback = is.streamActions.callbacks.remove(resp.getId());
        if (callback == null) {
          throw new RuntimeException("logic issue");
        }
        callback.completed();
        return state;
      };
  }

  public static class SeederActions {

    final ES es;
    final IS is;

    public SeederActions(ES es, IS is) {
      this.es = es;
      this.is = is;
    }

    public void disconnected(KAddress seeder) {
      Handlers.sendCtrlEvent(es, new R1FileDownloadEvents.Disconnected(is.torrentId, is.fileId, seeder.getId()));
    }

    public void disconnect(KAddress seeder) {
      Handlers.sendTransferEvent(es, new R1TransferSeederEvents.Disconnect(is.torrentId, is.fileId, seeder));
    }

    private void connect(KAddress seeder) {
      Handlers.sendTransferEvent(es,
        new R1TransferSeederEvents.Connect(is.torrentId, is.fileId, seeder, is.fileMetadata));
    }

    public void download(KAddress seeder, Set<Integer> blocks) {
      Handlers.sendDownloadEvent(es, new R1DownloadEvents.GetBlocks(is.torrentId, is.fileId, seeder.getId(), blocks));
    }
  }
}
