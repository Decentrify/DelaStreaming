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
package se.sics.nstream.hops.libmngr;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.javatuples.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.kompics.ComponentProxy;
import se.sics.kompics.Positive;
import se.sics.ktoolbox.nutil.fsm.FSMEventHandler;
import se.sics.ktoolbox.nutil.fsm.FSMException;
import se.sics.ktoolbox.nutil.fsm.FSMExternalState;
import se.sics.ktoolbox.nutil.fsm.FSMInternalState;
import se.sics.ktoolbox.nutil.fsm.FSMInternalStateBuilder;
import se.sics.ktoolbox.nutil.fsm.FSMInternalStateBuilders;
import se.sics.ktoolbox.nutil.fsm.FSMStateDef;
import se.sics.ktoolbox.nutil.fsm.FSMTransition;
import se.sics.ktoolbox.nutil.fsm.FSMTransitions;
import se.sics.ktoolbox.nutil.fsm.FSMachineDef;
import se.sics.ktoolbox.nutil.fsm.MultiFSM;
import se.sics.ktoolbox.nutil.fsm.ids.FSMDefId;
import se.sics.ktoolbox.nutil.fsm.ids.FSMId;
import se.sics.ktoolbox.nutil.fsm.ids.FSMStateDefId;
import se.sics.ktoolbox.util.Either;
import se.sics.ktoolbox.util.identifiable.Identifier;
import se.sics.ktoolbox.util.identifiable.overlay.OverlayId;
import se.sics.ktoolbox.util.network.KAddress;
import se.sics.ktoolbox.util.result.Result;
import se.sics.nstream.FileId;
import se.sics.nstream.StreamId;
import se.sics.nstream.TorrentIds;
import se.sics.nstream.hops.library.HopsTorrentPort;
import se.sics.nstream.hops.library.event.core.HopsTorrentDownloadEvent;
import se.sics.nstream.hops.library.event.core.HopsTorrentStopEvent;
import se.sics.nstream.hops.library.event.core.HopsTorrentUploadEvent;
import se.sics.nstream.hops.manifest.DiskHelper;
import se.sics.nstream.hops.manifest.ManifestJSON;
import se.sics.nstream.library.Library;
import se.sics.nstream.library.endpointmngr.EndpointIdRegistry;
import se.sics.nstream.storage.durable.DEndpointCtrlPort;
import se.sics.nstream.storage.durable.DurableStorageProvider;
import se.sics.nstream.storage.durable.disk.DiskComp;
import se.sics.nstream.storage.durable.disk.DiskEndpoint;
import se.sics.nstream.storage.durable.disk.DiskFED;
import se.sics.nstream.storage.durable.disk.DiskResource;
import se.sics.nstream.storage.durable.events.DEndpointConnect;
import se.sics.nstream.storage.durable.events.DEndpointDisconnect;
import se.sics.nstream.storage.durable.util.FileExtendedDetails;
import se.sics.nstream.storage.durable.util.MyStream;
import se.sics.nstream.storage.durable.util.StreamResource;
import se.sics.nstream.torrent.TorrentMngrPort;
import se.sics.nstream.torrent.event.StartTorrent;
import se.sics.nstream.torrent.event.StopTorrent;
import se.sics.nstream.torrent.transfer.TransferCtrlPort;
import se.sics.nstream.torrent.transfer.event.ctrl.SetupTransfer;
import se.sics.nstream.transfer.MyTorrent;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class LocalLibTorrentFSM {

  private static final Logger LOG = LoggerFactory.getLogger(LocalLibTorrentFSM.class);
  public static final String NAME = "local-dela-torrent-library-fsm";

  public static enum Transition implements FSMTransition {

    DOWNLOAD_ACCEPT,
    UPLOAD_PREPARE_STORAGE,
    UPLOAD_CONTINUE_PREPARE_STORAGE,
    UPLOAD_PREPARE_TRANSFER1,
    UPLOAD_PREPARE_TRANSFER2,
    UPLOAD_ADVANCE_TRANSFER,
    UPLOADING,
    ENDPOINT_CLEAN1,
    ENDPOINT_CLEAN2,
    ENDPOINT_CONTINUE_CLEAN,
    TRANSFER_CLEAN1,
    TRANSFER_CLEAN2,
    TRANSFER_CLEAN3,
  }

  public static MultiFSM getFSM(LibTExternal es) {
    try {
      Map<FSMDefId, FSMachineDef> fsmds = new HashMap<>();
      FSMachineDef torrentFSM = build();
      fsmds.put(torrentFSM.id, torrentFSM);

      FSMInternalStateBuilders builders = new FSMInternalStateBuilders();
      builders.register(torrentFSM.id, new LibTInternalBuilder());

      List<Pair<Class, List<Class>>> positivePorts = new LinkedList<>();
      //
      Class endpointPort = DEndpointCtrlPort.class;
      List<Class> endpointEvents = new LinkedList<>();
      endpointEvents.add(DEndpointConnect.Success.class);
      endpointEvents.add(DEndpointDisconnect.Success.class);
      positivePorts.add(Pair.with(endpointPort, endpointEvents));
      //
      Class torrentMngrPort = TorrentMngrPort.class;
      List<Class> torrentMngrEvents = new LinkedList<>();
      torrentMngrEvents.add(StartTorrent.Response.class);
      torrentMngrEvents.add(StopTorrent.Response.class);
      positivePorts.add(Pair.with(torrentMngrPort, torrentMngrEvents));
      //
      Class transferCtrlPort = TransferCtrlPort.class;
      List<Class> transferEvents = new LinkedList<>();
      transferEvents.add(SetupTransfer.Response.class);
      positivePorts.add(Pair.with(transferCtrlPort, transferEvents));

      List<Pair<Class, List<Class>>> negativePorts = new LinkedList<>();
      Class hopsTorrentPort = HopsTorrentPort.class;
      List<Class> hopsTorrentEvents = new LinkedList<>();
      hopsTorrentEvents.add(HopsTorrentDownloadEvent.StartRequest.class);
      hopsTorrentEvents.add(HopsTorrentUploadEvent.Request.class);
      hopsTorrentEvents.add(HopsTorrentStopEvent.Request.class);
      negativePorts.add(Pair.with(hopsTorrentPort, hopsTorrentEvents));

      MultiFSM fsm = new MultiFSM(fsmds, es, builders, positivePorts, negativePorts);
      return fsm;
    } catch (FSMException ex) {
      throw new RuntimeException(ex);
    }
  }

  public static FSMachineDef build() throws FSMException {
    FSMachineDef fsm = FSMachineDef.instance(NAME);
    FSMStateDefId init_id = fsm.registerInitState(initState());
    FSMStateDefId u_ps_id = fsm.registerState(uploadPrepareStorageState());
    FSMStateDefId u_pt_id = fsm.registerState(uploadPrepareTransferState());
    FSMStateDefId u_at_id = fsm.registerState(uploadAdvanceTransferState());
    FSMStateDefId u_id = fsm.registerState(uploadingState());
    FSMStateDefId ec_id = fsm.registerState(endpointCleaningState());
    FSMStateDefId tc_id = fsm.registerState(transferCleaningState());
    FSMStateDefId s_id = fsm.registerState(stoppedState());
    fsm.register(Transition.DOWNLOAD_ACCEPT, init_id, init_id);
    //upload transitions
    fsm.register(Transition.UPLOAD_PREPARE_STORAGE, init_id, u_ps_id);
    fsm.register(Transition.UPLOAD_PREPARE_TRANSFER1, init_id, u_pt_id);
    fsm.register(Transition.UPLOAD_CONTINUE_PREPARE_STORAGE, u_ps_id, u_ps_id);
    fsm.register(Transition.UPLOAD_PREPARE_TRANSFER2, u_ps_id, u_pt_id);
    fsm.register(Transition.UPLOAD_ADVANCE_TRANSFER, u_pt_id, u_at_id);
    fsm.register(Transition.UPLOADING, u_at_id, u_id);
    fsm.register(Transition.ENDPOINT_CLEAN1, u_ps_id, ec_id);
    fsm.register(Transition.TRANSFER_CLEAN1, u_pt_id, tc_id);
    fsm.register(Transition.TRANSFER_CLEAN2, u_at_id, tc_id);
    fsm.register(Transition.TRANSFER_CLEAN3, u_id, tc_id);
    fsm.register(Transition.ENDPOINT_CLEAN2, tc_id, ec_id);
    fsm.register(Transition.ENDPOINT_CONTINUE_CLEAN, ec_id, ec_id);
    fsm.seal();
    return fsm;
  }

  private static FSMStateDef initState() throws FSMException {
    FSMStateDef state = new FSMStateDef();
    state.register(HopsTorrentDownloadEvent.StartRequest.class, initDownload);
    state.register(HopsTorrentUploadEvent.Request.class, initUpload);
    state.seal();
    return state;
  }

  static FSMEventHandler initDownload
    = new FSMEventHandler<LibTExternal, LibTInternal, HopsTorrentDownloadEvent.StartRequest>() {
      @Override
      public FSMTransition handle(LibTExternal es, LibTInternal is, HopsTorrentDownloadEvent.StartRequest req) {
        LOG.info("{}accepting new download:{}", req.getBaseId(), req.torrentName);
        return Transition.DOWNLOAD_ACCEPT;
      }
    };

  static FSMEventHandler initUpload
    = new FSMEventHandler<LibTExternal, LibTInternal, HopsTorrentUploadEvent.Request>() {
      @Override
      public FSMTransition handle(LibTExternal es, LibTInternal is, HopsTorrentUploadEvent.Request req) {
        if (es.library.containsTorrent(req.torrentId)) {
          throw new RuntimeException("library and fsm do not agree - cannot fix it while running - logic error");
        }
        LOG.info("<{}>accepting new upload:{}", req.getBaseId(), req.torrentName);
        es.library.prepareUpload(req.projectId, req.torrentId, req.torrentName);
        is.setInit(req);
        is.setTorrentBuilder(new TorrentBuilder());
        setupStorageEndpoints(es, is);
        setupManifestStream(is, req);
        if (is.endpointRegistration.isComplete()) {
          is.torrentBuilder.setEndpoints(is.endpointRegistration.getSetup());
          setupTransfer(es, is);
          return Transition.UPLOAD_PREPARE_TRANSFER1;
        } else {
          return Transition.UPLOAD_PREPARE_STORAGE;
        }
      }
    };

  private static FSMStateDef uploadPrepareStorageState() throws FSMException {
    FSMStateDef state = new FSMStateDef();
    state.register(DEndpointConnect.Success.class, uploadPrepareStorage);
    state.register(HopsTorrentStopEvent.Request.class, uploadPrepareStorageStop);
    state.seal();
    return state;
  }

  static FSMEventHandler uploadPrepareStorage
    = new FSMEventHandler<LibTExternal, LibTInternal, DEndpointConnect.Success>() {
      @Override
      public FSMTransition handle(LibTExternal es, LibTInternal is, DEndpointConnect.Success resp) {
        LOG.debug("<{}>endpoint:{} prepared", resp.getBaseId(), resp.req.endpointProvider.getName());
        is.endpointRegistration.connected(resp.req.endpointId);
        if (is.endpointRegistration.isComplete()) {
          is.torrentBuilder.setEndpoints(is.endpointRegistration.getSetup());
          setupTransfer(es, is);
          return Transition.UPLOAD_PREPARE_TRANSFER2;
        } else {
          return Transition.UPLOAD_CONTINUE_PREPARE_STORAGE;
        }
      }
    };

  static FSMEventHandler uploadPrepareStorageStop
    = new FSMEventHandler<LibTExternal, LibTInternal, HopsTorrentStopEvent.Request>() {
      @Override
      public FSMTransition handle(LibTExternal es, LibTInternal is, HopsTorrentStopEvent.Request req) {
        LOG.info("<{}>stop received during endpoint prepare", req.getBaseId());
        es.proxy.answer(is.uploadReq, is.uploadReq.failed(Result.logicalFail("concurrent stop event:" + req.getId())));
        is.setStopReq(req);
        cleanStorageEndpoints(es, is, is.endpointRegistration.selfCleaning());
        return Transition.ENDPOINT_CLEAN1;
      }
    };

  private static FSMStateDef uploadPrepareTransferState() throws FSMException {
    FSMStateDef state = new FSMStateDef();
    state.register(StartTorrent.Response.class, uploadPrepareTransfer);
    state.register(HopsTorrentStopEvent.Request.class, uploadPrepareTransferStop);
    state.seal();
    return state;
  }

  static FSMEventHandler uploadPrepareTransfer
    = new FSMEventHandler<LibTExternal, LibTInternal, StartTorrent.Response>() {
      @Override
      public FSMTransition handle(LibTExternal es, LibTInternal is, StartTorrent.Response resp) {
        if (resp.result.isSuccess()) {
          LOG.debug("<{}>torrent:{} - started", resp.getBaseId(), resp.overlayId());
          prepareManifest(is);
          prepareDetails(is);
          is.setTorrent(is.getTorrentBuilder().getTorrent());
          advanceTransfer(es, is);
          return Transition.UPLOAD_ADVANCE_TRANSFER;
        } else {
          LOG.warn("<{}>torrent:{} - start failed", resp.getBaseId(), resp.overlayId());
          throw new RuntimeException("todo deal with failure");
        }
      }
    };

  static FSMEventHandler uploadPrepareTransferStop
    = new FSMEventHandler<LibTExternal, LibTInternal, HopsTorrentStopEvent.Request>() {
      @Override
      public FSMTransition handle(LibTExternal es, LibTInternal is, HopsTorrentStopEvent.Request req) {
        LOG.info("<{}>stop received during transfer prepare", req.getBaseId());
        es.proxy.answer(is.uploadReq, is.uploadReq.failed(Result.logicalFail("concurrent stop event:" + req.getId())));
        is.setStopReq(req);
        cleanTransfer(es, is);
        return Transition.TRANSFER_CLEAN1;
      }
    };

  private static FSMStateDef uploadAdvanceTransferState() throws FSMException {
    FSMStateDef state = new FSMStateDef();
    state.register(SetupTransfer.Response.class, uploadAdvanceTransfer);
    state.register(HopsTorrentStopEvent.Request.class, uploadAdvanceTransferStop);
    state.seal();
    return state;
  }

  static FSMEventHandler uploadAdvanceTransfer
    = new FSMEventHandler<LibTExternal, LibTInternal, SetupTransfer.Response>() {
      @Override
      public FSMTransition handle(LibTExternal es, LibTInternal is, SetupTransfer.Response resp) {
        if (resp.result.isSuccess()) {
          LOG.debug("<{}>torrent:{} - transfer - set up", resp.getBaseId(), resp.overlayId());
          if (!es.library.containsTorrent(resp.overlayId())) {
            throw new RuntimeException("mismatch between library and fsm - critical logical error");
          }
          es.library.upload(resp.overlayId(), is.torrentBuilder.getManifestStream().getValue1());
          es.getProxy().answer(is.getUploadReq(), is.getUploadReq().success(Result.success(true)));
          return Transition.UPLOADING;
        } else {
          LOG.debug("<{}>torrent:{} - transfer - set up failed", resp.getBaseId(), resp.overlayId());
          throw new RuntimeException("todo deal with failure");
        }
      }
    };

  static FSMEventHandler uploadAdvanceTransferStop
    = new FSMEventHandler<LibTExternal, LibTInternal, HopsTorrentStopEvent.Request>() {
      @Override
      public FSMTransition handle(LibTExternal es, LibTInternal is, HopsTorrentStopEvent.Request req) {
        LOG.info("<{}>stop received during transfer advance", req.getBaseId());
        es.proxy.answer(is.uploadReq, is.uploadReq.failed(Result.logicalFail("concurrent stop event:" + req.getId())));
        is.setStopReq(req);
        cleanTransfer(es, is);
        return Transition.TRANSFER_CLEAN2;
      }
    };

  private static FSMStateDef uploadingState() throws FSMException {
    FSMStateDef state = new FSMStateDef();
    state.register(HopsTorrentStopEvent.Request.class, uploadingStop);
    state.seal();
    return state;
  }

  static FSMEventHandler uploadingStop
    = new FSMEventHandler<LibTExternal, LibTInternal, HopsTorrentStopEvent.Request>() {
      @Override
      public FSMTransition handle(LibTExternal es, LibTInternal is, HopsTorrentStopEvent.Request req) {
        LOG.info("<{}>stop received during uploading", req.getBaseId());
        is.setStopReq(req);
        cleanTransfer(es, is);
        return Transition.TRANSFER_CLEAN3;
      }
    };

  private static FSMStateDef endpointCleaningState() throws FSMException {
    FSMStateDef state = new FSMStateDef();
    state.register(DEndpointDisconnect.Success.class, endpointCleaning);
    state.seal();
    return state;
  }

  static FSMEventHandler endpointCleaning
    = new FSMEventHandler<LibTExternal, LibTInternal, DEndpointDisconnect.Success>() {
      @Override
      public FSMTransition handle(LibTExternal es, LibTInternal is, DEndpointDisconnect.Success resp) {
        LOG.debug("<{}>endpoint:{} cleaned", resp.getBaseId(), resp.req.endpointId);
        is.endpointRegistration.cleaned(resp.req.endpointId);
        if (is.endpointRegistration.cleaningComplete()) {
          es.getProxy().answer(is.stopReq, is.stopReq.success());
          es.library.remove(is.torrentId);
          return FSMTransitions.KILL;
        }
        return Transition.ENDPOINT_CONTINUE_CLEAN;
      }
    };

  private static FSMStateDef transferCleaningState() throws FSMException {
    FSMStateDef state = new FSMStateDef();
    state.register(StopTorrent.Response.class, transferCleaning);
    state.seal();
    return state;
  }

  static FSMEventHandler transferCleaning
    = new FSMEventHandler<LibTExternal, LibTInternal, StopTorrent.Response>() {
      @Override
      public FSMTransition handle(LibTExternal es, LibTInternal is, StopTorrent.Response resp) {
        LOG.debug("<{}>torrent:{} cleaned", resp.getBaseId(), resp.torrentId);
        if (!resp.result.isSuccess()) {
          throw new RuntimeException("TODO Alex - what do you do when the cleaning operation fails");
        }
        cleanStorageEndpoints(es, is, is.endpointRegistration.selfCleaning());
        return Transition.ENDPOINT_CLEAN2;
      }
    };

  private static FSMStateDef stoppedState() throws FSMException {
    FSMStateDef state = new FSMStateDef();
    state.seal();
    return state;
  }

  private static DurableStorageProvider getStorageProvider(Identifier selfId) {
    return new DiskComp.StorageProvider(selfId);
  }

  private static void setupManifestStream(LibTInternal is, HopsTorrentUploadEvent.Request req) {
    MyStream manifestStream = new MyStream(new DiskEndpoint(), new DiskResource(req.manifestResource.dirPath,
      req.manifestResource.fileName));
    Identifier manifestEndpointId = is.endpointRegistration.nameToId(manifestStream.endpoint.getEndpointName());
    FileId manifestFileId = TorrentIds.fileId(req.torrentId, MyTorrent.MANIFEST_ID);
    StreamId manifestStreamId = TorrentIds.streamId(manifestEndpointId, manifestFileId);
    is.getTorrentBuilder().setManifestStream(manifestStreamId, manifestStream);
  }

  private static void setupStorageEndpoints(LibTExternal es, LibTInternal is) {

    List<DurableStorageProvider> storageProviders = new ArrayList<>();
    DurableStorageProvider storageProvider = getStorageProvider(es.selfAdr.getId());
    storageProviders.add(storageProvider);

    for (DurableStorageProvider dsp : storageProviders) {
      String endpointName = dsp.getName();
      Identifier endpointId;
      if (es.endpointIdRegistry.registered(endpointName)) {
        endpointId = es.endpointIdRegistry.lookup(endpointName);
      } else {
        endpointId = es.endpointIdRegistry.register(endpointName);
      }
      is.endpointRegistration.addWaiting(endpointName, endpointId, dsp.getEndpoint());
      es.proxy.trigger(new DEndpointConnect.Request(endpointId, dsp, is.fsmId, is.fsmName), es.endpointPort());
    }
  }

  private static void cleanStorageEndpoints(LibTExternal es, LibTInternal is, Set<Identifier> endpoints) {
    for (Identifier endpointId : endpoints) {
      es.proxy.trigger(new DEndpointDisconnect.Request(endpointId, is.fsmId, is.fsmName), es.endpointPort());
    }
  }

  private static void prepareManifest(LibTInternal is) {
    StreamResource manifestResource = is.getTorrentBuilder().getManifestStream().getValue1().resource;
    Result<ManifestJSON> manifestJSON = DiskHelper.readManifest((DiskResource) manifestResource);
    if (!manifestJSON.isSuccess()) {
      throw new RuntimeException("todo deal with failure");
    }
    Either<MyTorrent.Manifest, ManifestJSON> manifestResult = Either.right(manifestJSON.getValue());
    is.getTorrentBuilder().setManifest(is.getTorrentId(), manifestResult);
  }

  private static void prepareDetails(LibTInternal is) {
    Map<FileId, FileExtendedDetails> extendedDetails = new HashMap<>();
    Pair<StreamId, MyStream> manifestStream = is.getTorrentBuilder().getManifestStream();
    DiskResource manifestResource = (DiskResource) manifestStream.getValue1().resource;
    for (Map.Entry<String, FileId> file : is.getTorrentBuilder().getFiles().entrySet()) {
      StreamId fileStreamId = manifestStream.getValue0().withFile(file.getValue());
      DiskResource fileResource = manifestResource.withFile(file.getKey());
      MyStream fileStream = manifestStream.getValue1().withResource(fileResource);
      extendedDetails.put(file.getValue(), new DiskFED(fileStreamId, fileStream));
    }
    is.getTorrentBuilder().setExtendedDetails(extendedDetails);
  }

  private static void setupTransfer(LibTExternal es, LibTInternal is) {
    StartTorrent.Request req = new StartTorrent.Request(is.getTorrentId(), is.getPartners(), is.fsmId, is.fsmName);
    es.getProxy().trigger(req, es.torrentMngrPort());
  }
  
  private static void cleanTransfer(LibTExternal es, LibTInternal is) {
    StopTorrent.Request req = new StopTorrent.Request(is.getTorrentId(), is.fsmId, is.fsmName);
    es.getProxy().trigger(req, es.torrentMngrPort());
  }

  private static void advanceTransfer(LibTExternal es, LibTInternal is) {
    SetupTransfer.Request req = new SetupTransfer.Request(is.getTorrentId(), is.getTorrent(), is.fsmId, is.fsmName);
    es.getProxy().trigger(req, es.transferCtrlPort());
  }

  public static class LibTInternal implements FSMInternalState {

    public final FSMId fsmId;
    public final String fsmName;
    private HopsTorrentUploadEvent.Request uploadReq;
    private HopsTorrentStopEvent.Request stopReq;
    private OverlayId torrentId;
    private List<KAddress> partners;
    private final EndpointRegistration endpointRegistration = new EndpointRegistration();
    private TorrentBuilder torrentBuilder;
    private MyTorrent torrent;

    public LibTInternal(FSMId fsmId) {
      this.fsmId = fsmId;
      this.fsmName = LocalLibTorrentFSM.NAME;
    }

    public void setInit(HopsTorrentUploadEvent.Request req) {
      this.uploadReq = req;
      this.torrentId = req.torrentId;
      this.partners = new LinkedList<>();
    }

    public HopsTorrentUploadEvent.Request getUploadReq() {
      return uploadReq;
    }

    public HopsTorrentStopEvent.Request getStopReq() {
      return stopReq;
    }

    public void setStopReq(HopsTorrentStopEvent.Request stopReq) {
      this.stopReq = stopReq;
    }

    public OverlayId getTorrentId() {
      return torrentId;
    }

    public List<KAddress> getPartners() {
      return partners;
    }

    public TorrentBuilder getTorrentBuilder() {
      return torrentBuilder;
    }

    public void setTorrentBuilder(TorrentBuilder torrentBuilder) {
      this.torrentBuilder = torrentBuilder;
    }

    public MyTorrent getTorrent() {
      return torrent;
    }

    public void setTorrent(MyTorrent torrent) {
      this.torrent = torrent;
    }
  }

  public static class LibTInternalBuilder implements FSMInternalStateBuilder {

    @Override
    public FSMInternalState newState(FSMId fsmId) {
      return new LibTInternal(fsmId);
    }
  }

  public static class LibTExternal implements FSMExternalState {

    private ComponentProxy proxy;
    public final KAddress selfAdr;
    public final Library library;
    public final EndpointIdRegistry endpointIdRegistry;

    public LibTExternal(KAddress selfAdr, Library library, EndpointIdRegistry endpointIdRegistry) {
      this.selfAdr = selfAdr;
      this.library = library;
      this.endpointIdRegistry = endpointIdRegistry;
    }

    @Override
    public void setProxy(ComponentProxy proxy) {
      this.proxy = proxy;
    }

    @Override
    public ComponentProxy getProxy() {
      return proxy;
    }

    public Positive endpointPort() {
      return proxy.getNegative(DEndpointCtrlPort.class).getPair();
    }

    public Positive torrentMngrPort() {
      return proxy.getNegative(TorrentMngrPort.class).getPair();
    }

    public Positive transferCtrlPort() {
      return proxy.getNegative(TransferCtrlPort.class).getPair();
    }
  }
}
