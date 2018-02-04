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
import se.sics.nstream.FileId;
import se.sics.nstream.StreamId;
import se.sics.nstream.TorrentIds;
import se.sics.nstream.storage.durable.DStreamControlPort;
import se.sics.nstream.storage.durable.events.DStreamConnect;
import se.sics.nstream.storage.durable.events.DStreamDisconnect;
import se.sics.nstream.storage.durable.events.DStreamEvent;
import se.sics.nstream.storage.durable.util.MyStream;
import se.sics.nstream.util.BlockDetails;
import se.sics.silk.SelfPort;
import se.sics.silk.TorrentIdHelper;
import se.sics.silk.event.SilkEvent;
import se.sics.silk.r2torrent.R2TorrentComp;
import se.sics.silk.r2torrent.R2TorrentES;
import static se.sics.silk.r2torrent.torrent.R2Torrent.HardCodedConfig.seed;
import se.sics.silk.r2torrent.torrent.event.R1FileUploadEvents;
import se.sics.silk.r2torrent.torrent.state.R1FileUploadLeechersState;
import se.sics.silk.r2torrent.transfer.R1TransferLeecher;
import se.sics.silk.r2torrent.transfer.R1UploadComp;
import se.sics.silk.r2torrent.transfer.R1UploadPort;
import se.sics.silk.r2torrent.transfer.events.R1TransferLeecherEvents;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class R1FileUpload {

  private static final Logger LOG = LoggerFactory.getLogger(FSM.class);
  public static final String NAME = "dela-r1-torrent-file-upload-fsm";

  public static enum States implements FSMStateName {

    STORAGE_PENDING,
    ACTIVE,
    CLOSE,
  }

  public static interface Event extends FSMEvent, Identifiable, SilkEvent.TorrentEvent, SilkEvent.FileEvent {
  }

  public static interface UploadEvent extends Event {
  }

  public static interface ConnectEvent extends Event {
  }

  public static interface CtrlEvent extends Event {
  }

  public static Identifier fsmBaseId(OverlayId torrentId, Identifier fileId) {
    return new PairIdentifier(torrentId, fileId);
  }

  public static class HardCodedConfig {
  }

  public static class IS implements FSMInternalState {

    private final FSMIdentifier fsmId;
    OverlayId torrentId;
    Identifier fileId;
    R1FileUploadLeechersState fileState = new R1FileUploadLeechersState();
    Map<Identifier, Component> comps = new HashMap<>();

    public IS(FSMIdentifier fsmId) {
      this.fsmId = fsmId;
    }

    @Override
    public FSMIdentifier getFSMId() {
      return fsmId;
    }

    public void init(R1TransferLeecherEvents.ConnectReq req) {
      this.torrentId = req.torrentId;
      this.fileId = req.fileId;
    }
  }

  public static class ISBuilder implements FSMInternalStateBuilder {

    @Override
    public FSMInternalState newState(FSMIdentifier fsmId) {
      return new IS(fsmId);
    }
  }

  public static class R1FileMngr {
    public final Identifier endpointId;
    public final OverlayId torrentId;
    
    public R1FileMngr(Identifier endpointId, OverlayId torrentId) {
      this.endpointId = endpointId;
      this.torrentId = torrentId;
    }
    public boolean isComplete(Identifier fileId) {
      return true;
    }
    
    public StreamId streamId(Identifier fileId) {
      FileId fi = TorrentIdHelper.fileId(torrentId, endpointId);
      return TorrentIds.streamId(endpointId, fi);
    }
    
    public MyStream stream(StreamId streamId) {
      return null;
    }
  }
  
  public static class ES implements R2TorrentES {

    private ComponentProxy proxy;
    R2TorrentComp.Ports ports;
    KAddress selfAdr;
    IntIdFactory fileIdFactory;
    R1FileMngr fileMngr;
    BlockDetails defaultBlock;

    public ES(KAddress selfAdr, R1FileMngr fileMngr, BlockDetails defaultBlock) {
      this.selfAdr = selfAdr;
      this.fileIdFactory = new IntIdFactory(new Random(seed));
      this.fileMngr = fileMngr;
      this.defaultBlock = defaultBlock;
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
        .toFinal()
        .buildTransition()
        .onState(States.STORAGE_PENDING)
        .nextStates(States.STORAGE_PENDING, States.ACTIVE)
        .buildTransition()
        .onState(States.ACTIVE)
        .nextStates(States.ACTIVE, States.CLOSE)
        .buildTransition()
        .onState(States.CLOSE)
        .toFinal()
        .buildTransition();
    }

    private static FSMBuilder.SemanticDefinition semanticDef() throws FSMException {
      FSMBuilder.SemanticDefinition def = FSMBuilder.semanticDef();

      def = def
        .positivePort(SelfPort.class)
        .basicEvent(R1TransferLeecherEvents.ConnectReq.class)
        .subscribeOnStart(Handlers.connect0)
        .subscribe(Handlers.connect1, States.STORAGE_PENDING)
        .subscribe(Handlers.connect2, States.ACTIVE)
        .fallback(Handlers.connect3)
        .basicEvent(R1TransferLeecherEvents.Disconnected.class)
        .subscribe(Handlers.disconnected1, States.STORAGE_PENDING)
        .subscribe(Handlers.disconnected2, States.ACTIVE)
        .buildEvents();

      def = def
        .positivePort(DStreamControlPort.class)
        .basicEvent(DStreamConnect.Success.class)
        .subscribe(Handlers.streamOpened, States.STORAGE_PENDING)
        .basicEvent(DStreamDisconnect.Success.class)
        .subscribe(Handlers.streamClosed, States.CLOSE)
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

    //*******************************************************STORAGE_PENDING****************************************************
    static FSMBasicEventHandler streamOpened = (FSMBasicEventHandler<ES, IS, DStreamConnect.Success>) (
      FSMStateName state, ES es, IS is, DStreamConnect.Success event) -> {
        is.fileState.pendingConnected(createComp(es, is), sendConnectAcc(es, is));
        sendCtrlEvent(es, new R1FileUploadEvents.Indication(is.torrentId, is.fileId, States.ACTIVE));
        return States.ACTIVE;
      };

    static FSMBasicEventHandler streamClosed = (FSMBasicEventHandler<ES, IS, DStreamDisconnect.Success>) (
      FSMStateName state, ES es, IS is, DStreamDisconnect.Success event) -> {
        return FSMBasicStateNames.FINAL;
      };

    //*******************************************************CONNECT****************************************************
    static FSMBasicEventHandler connect0
      = (FSMBasicEventHandler<ES, IS, R1TransferLeecherEvents.ConnectReq>) (FSMStateName state, ES es, IS is,
        R1TransferLeecherEvents.ConnectReq req) -> {
        if (!es.fileMngr.isComplete(req.fileId)) {
          throw new RuntimeException("reject upload");
        }

        is.init(req);
        is.fileState.pendingLeecher(req);
        createStorageStream(es, is.fileId);
        return States.STORAGE_PENDING;
      };

    static FSMBasicEventHandler connect1
      = (FSMBasicEventHandler<ES, IS, R1TransferLeecherEvents.ConnectReq>) (FSMStateName state, ES es, IS is,
        R1TransferLeecherEvents.ConnectReq req) -> {
        is.fileState.pendingLeecher(req);
        return state;
      };

    static FSMBasicEventHandler connect2
      = (FSMBasicEventHandler<ES, IS, R1TransferLeecherEvents.ConnectReq>) (FSMStateName state, ES es, IS is,
        R1TransferLeecherEvents.ConnectReq req) -> {
        is.fileState.connected(req, createComp(es, is), sendConnectAcc(es, is));
        return state;
      };
    
    static FSMBasicEventHandler connect3
      = (FSMBasicEventHandler<ES, IS, R1TransferLeecherEvents.ConnectReq>) (FSMStateName state, ES es, IS is,
        R1TransferLeecherEvents.ConnectReq req) -> {
        sendConnectEvent(es, req.reject());
        return state;
      };

    static FSMBasicEventHandler disconnected1
      = (FSMBasicEventHandler<ES, IS, R1TransferLeecherEvents.Disconnected>) (FSMStateName state, ES es, IS is,
        R1TransferLeecherEvents.Disconnected req) -> {
        is.fileState.disconnected(req);
        if (is.fileState.empty()) {
          destroyStorageStream(es, is.fileId);
          return States.CLOSE;
        } else {
          return state;
        }
      };
    
    static FSMBasicEventHandler disconnected2
      = (FSMBasicEventHandler<ES, IS, R1TransferLeecherEvents.Disconnected>) (FSMStateName state, ES es, IS is,
        R1TransferLeecherEvents.Disconnected req) -> {
        is.fileState.disconnected(req, killComp(es, is));
        if (is.fileState.empty()) {
          destroyStorageStream(es, is.fileId);
          return States.CLOSE;
        } else {
          return state;
        }
      };

    private static Consumer<KAddress> createComp(ES es, IS is) {
      return (KAddress leecherAdr) -> {
        R1UploadComp.Init init = new R1UploadComp.Init(es.selfAdr, is.torrentId, is.fileId, leecherAdr, es.defaultBlock);
        Component uploadComp = es.proxy.create(R1UploadComp.class, init);
        Identifier uploadId = R1UploadComp.baseId(is.torrentId, is.fileId, leecherAdr.getId());
        es.proxy.connect(es.ports.timer, uploadComp.getNegative(Timer.class), Channel.TWO_WAY);
        es.ports.uploadC.addChannel(uploadId, uploadComp.getPositive(R1UploadPort.class));
        es.ports.netUploadC.addChannel(uploadId, uploadComp.getNegative(Network.class));
        es.proxy.trigger(Start.event, uploadComp.control());
        Identifier compId = R1UploadComp.baseId(is.torrentId, is.fileId, leecherAdr.getId());
        is.comps.put(compId, uploadComp);
      };
    }

    private static Consumer<KAddress> killComp(ES es, IS is) {
      return (KAddress leecher) -> {
        Identifier uploadId = R1UploadComp.baseId(is.torrentId, is.fileId, leecher.getId());
        Component uploadComp = is.comps.remove(uploadId);
        es.proxy.trigger(Kill.event, uploadComp.control());
        es.proxy.disconnect(es.ports.timer, uploadComp.getNegative(Timer.class));
        es.ports.uploadC.removeChannel(uploadId, uploadComp.getPositive(R1UploadPort.class));
        es.ports.netUploadC.removeChannel(uploadId, uploadComp.getNegative(Network.class));
      };
    }

    private static void createStorageStream(ES es, Identifier fileId) {
      StreamId streamId = es.fileMngr.streamId(fileId);
      MyStream stream = es.fileMngr.stream(streamId);
      sendStreamEvent(es, new DStreamConnect.Request(Pair.with(streamId, stream)));
    }
    
    private static void destroyStorageStream(ES es, Identifier fileId) {
      StreamId streamId = es.fileMngr.streamId(fileId);
      sendStreamEvent(es, new DStreamDisconnect.Request(streamId));
    }

    private static void sendCtrlEvent(ES es, KompicsEvent e) {
      es.getProxy().trigger(e, es.ports.loopbackPos);
    }

    private static void sendStreamEvent(ES es, DStreamEvent e) {
      es.getProxy().trigger(e, es.ports.streamCtrl);
    }

    private static void sendConnectEvent(ES es, R1TransferLeecher.CtrlEvent e) {
      es.getProxy().trigger(e, es.ports.loopbackPos);
    }

    private static Consumer<R1TransferLeecherEvents.ConnectReq> sendConnectAcc(ES es, IS is) {
      return (R1TransferLeecherEvents.ConnectReq req) -> {
        es.getProxy().trigger(req.accept(), es.ports.loopbackPos);
      };
    }
  }
}
