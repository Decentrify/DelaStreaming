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

import com.google.common.base.Optional;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.javatuples.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.kompics.ComponentProxy;
import se.sics.kompics.Positive;
import se.sics.ktoolbox.nutil.fsm.FSMEvent;
import se.sics.ktoolbox.nutil.fsm.FSMEventHandler;
import se.sics.ktoolbox.nutil.fsm.FSMException;
import se.sics.ktoolbox.nutil.fsm.FSMExternalState;
import se.sics.ktoolbox.nutil.fsm.FSMIdExtractor;
import se.sics.ktoolbox.nutil.fsm.FSMInternalState;
import se.sics.ktoolbox.nutil.fsm.FSMInternalStateBuilder;
import se.sics.ktoolbox.nutil.fsm.FSMInternalStateBuilders;
import se.sics.ktoolbox.nutil.fsm.FSMStateDef;
import se.sics.ktoolbox.nutil.fsm.FSMTransition;
import se.sics.ktoolbox.nutil.fsm.FSMTransitions;
import se.sics.ktoolbox.nutil.fsm.FSMachineDef;
import se.sics.ktoolbox.nutil.fsm.MultiFSM;
import se.sics.ktoolbox.nutil.fsm.genericsetup.OnFSMExceptionAction;
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
import se.sics.nstream.hops.hdfs.HDFSResource;
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
import se.sics.nstream.storage.durable.events.DEndpoint;
import se.sics.nstream.storage.durable.util.FileExtendedDetails;
import se.sics.nstream.storage.durable.util.MyStream;
import se.sics.nstream.storage.durable.util.StreamResource;
import se.sics.nstream.torrent.TorrentMngrPort;
import se.sics.nstream.torrent.event.StartTorrent;
import se.sics.nstream.torrent.event.StopTorrent;
import se.sics.nstream.torrent.status.event.DownloadSummaryEvent;
import se.sics.nstream.torrent.tracking.TorrentStatusPort;
import se.sics.nstream.torrent.transfer.TransferCtrlPort;
import se.sics.nstream.torrent.transfer.event.ctrl.GetRawTorrent;
import se.sics.nstream.torrent.transfer.event.ctrl.SetupTransfer;
import se.sics.nstream.transfer.MyTorrent;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class LocalLibTorrentFSM {

  private static final Logger LOG = LoggerFactory.getLogger(LocalLibTorrentFSM.class);
  public static final String NAME = "dela-torrent-library-fsm";

  public static enum Transition implements FSMTransition {

    D_PREPARE_STORAGE,
    D_CONTINUE_PREPARE_STORAGE,
    D_PREPARE_TRANSFER1,
    D_PREPARE_TRANSFER2,
    D_GET_MANIFEST,
    D_ADVANCE_TRANSFER1,
    D_ADVANCE_TRANSFER2,
    DOWNLOADING,
    UPLOADING2,
    D_ENDPOINT_CLEAN,
    D_TRANSFER_CLEAN1,
    D_MANIFEST_CLEAN,
    D_TRANSFER_CLEAN2,
    D_CLEAN,

    U_PREPARE_STORAGE,
    U_CONTINUE_PREPARE_STORAGE,
    U_PREPARE_TRANSFER1,
    U_PREPARE_TRANSFER2,
    U_ADVANCE_TRANSFER,
    UPLOADING,
    U_ENDPOINT_CLEAN,
    U_TRANSFER_CLEAN1,
    U_TRANSFER_CLEAN2,
    U_CLEAN,
    ENDPOINT_CLEAN,
    ENDPOINT_CONTINUE_CLEAN,
  }

  public static MultiFSM getFSM(LibTExternal es, OnFSMExceptionAction oexa) {
    try {
      Map<FSMDefId, FSMachineDef> fsmds = new HashMap<>();
      final FSMachineDef torrentFSM = build();
      fsmds.put(torrentFSM.id, torrentFSM);

      FSMInternalStateBuilders builders = new FSMInternalStateBuilders();
      builders.register(torrentFSM.id, new LibTInternalBuilder());

      List<Pair<Class, List<Class>>> positivePorts = new LinkedList<>();
      //
      Class endpointPort = DEndpointCtrlPort.class;
      List<Class> endpointEvents = new LinkedList<>();
      endpointEvents.add(DEndpoint.Success.class);
      endpointEvents.add(DEndpoint.Failed.class);
      endpointEvents.add(DEndpoint.Disconnected.class);
      positivePorts.add(Pair.with(endpointPort, endpointEvents));
      //
      Class torrentMngrPort = TorrentMngrPort.class;
      List<Class> torrentMngrEvents = new LinkedList<>();
      torrentMngrEvents.add(StartTorrent.Response.class);
      torrentMngrEvents.add(StopTorrent.Response.class);
      positivePorts.add(Pair.with(torrentMngrPort, torrentMngrEvents));
      //
      Class transferCtrlPort = TransferCtrlPort.class;
      List<Class> transferCtrlEvents = new LinkedList<>();
      transferCtrlEvents.add(SetupTransfer.Response.class);
      transferCtrlEvents.add(GetRawTorrent.Response.class);
      positivePorts.add(Pair.with(transferCtrlPort, transferCtrlEvents));
      //
      Class torrentStatusPort = TorrentStatusPort.class;
      List<Class> torrentStatusEvents = new LinkedList<>();
      torrentStatusEvents.add(DownloadSummaryEvent.class);
      positivePorts.add(Pair.with(torrentStatusPort, torrentStatusEvents));

      List<Pair<Class, List<Class>>> negativePorts = new LinkedList<>();
      Class hopsTorrentPort = HopsTorrentPort.class;
      List<Class> hopsTorrentEvents = new LinkedList<>();
      hopsTorrentEvents.add(HopsTorrentDownloadEvent.StartRequest.class);
      hopsTorrentEvents.add(HopsTorrentUploadEvent.Request.class);
      hopsTorrentEvents.add(HopsTorrentStopEvent.Request.class);
      negativePorts.add(Pair.with(hopsTorrentPort, hopsTorrentEvents));

      FSMIdExtractor fsmIdExtractor = new FSMIdExtractor() {
        private final Set<Class> fsmEvents = new HashSet<>();

        {
          fsmEvents.add(DEndpoint.Success.class);
          fsmEvents.add(DEndpoint.Failed.class);
          fsmEvents.add(DEndpoint.Disconnected.class);
          fsmEvents.add(StartTorrent.Response.class);
          fsmEvents.add(StopTorrent.Response.class);
          fsmEvents.add(SetupTransfer.Response.class);
          fsmEvents.add(GetRawTorrent.Response.class);
          fsmEvents.add(DownloadSummaryEvent.class);
          fsmEvents.add(HopsTorrentDownloadEvent.StartRequest.class);
          fsmEvents.add(HopsTorrentUploadEvent.Request.class);
          fsmEvents.add(HopsTorrentStopEvent.Request.class);
        }

        @Override
        public Optional<FSMId> fromEvent(FSMEvent event) throws FSMException {
          if (fsmEvents.contains(event.getClass())) {
            return Optional.of(torrentFSM.id.getFSMId(event.getBaseId()));
          }
          return Optional.absent();
        }
      };
      MultiFSM fsm = new MultiFSM(oexa, fsmIdExtractor, fsmds, es, builders, positivePorts, negativePorts);
      return fsm;
    } catch (FSMException ex) {
      throw new RuntimeException(ex);
    }
  }

  public static FSMachineDef build() throws FSMException {
    FSMachineDef fsm = FSMachineDef.instance(NAME);
    FSMStateDefId init_id = fsm.registerInitState(initState());

    FSMStateDefId d_ps_id = fsm.registerState(downloadPrepareStorageState());
    FSMStateDefId d_pt_id = fsm.registerState(downloadPrepareTransferState());
    FSMStateDefId d_gm_id = fsm.registerState(downloadGetManifestState());
    FSMStateDefId d_at_id = fsm.registerState(downloadAdvanceTransferState());
    FSMStateDefId d_id = fsm.registerState(downloadingState());

    FSMStateDefId u_ps_id = fsm.registerState(uploadPrepareStorageState());
    FSMStateDefId u_pt_id = fsm.registerState(uploadPrepareTransferState());
    FSMStateDefId u_at_id = fsm.registerState(uploadAdvanceTransferState());
    FSMStateDefId u_id = fsm.registerState(uploadingState());

    FSMStateDefId ec_id = fsm.registerState(endpointCleaningState());
    FSMStateDefId tc_id = fsm.registerState(transferCleaningState());

    fsm.register(Transition.D_PREPARE_STORAGE, init_id, d_ps_id);
    fsm.register(Transition.D_PREPARE_TRANSFER1, init_id, d_pt_id);
    fsm.register(Transition.D_CONTINUE_PREPARE_STORAGE, d_ps_id, d_ps_id);
    fsm.register(Transition.D_PREPARE_TRANSFER2, d_ps_id, d_pt_id);
    fsm.register(Transition.D_ENDPOINT_CLEAN, d_ps_id, ec_id);
    fsm.register(Transition.D_GET_MANIFEST, d_pt_id, d_gm_id);
    fsm.register(Transition.D_ADVANCE_TRANSFER1, d_pt_id, d_at_id);
    fsm.register(Transition.D_TRANSFER_CLEAN1, d_pt_id, tc_id);
    fsm.register(Transition.D_ADVANCE_TRANSFER2, d_gm_id, d_at_id);
    fsm.register(Transition.D_MANIFEST_CLEAN, d_gm_id, tc_id);
    fsm.register(Transition.DOWNLOADING, d_at_id, d_id);
    fsm.register(Transition.D_TRANSFER_CLEAN2, d_at_id, tc_id);
    fsm.register(Transition.UPLOADING2, d_id, u_id);
    fsm.register(Transition.D_CLEAN, d_id, tc_id);
    //upload transitions
    fsm.register(Transition.U_PREPARE_STORAGE, init_id, u_ps_id);
    fsm.register(Transition.U_PREPARE_TRANSFER1, init_id, u_pt_id);
    fsm.register(Transition.U_CONTINUE_PREPARE_STORAGE, u_ps_id, u_ps_id);
    fsm.register(Transition.U_PREPARE_TRANSFER2, u_ps_id, u_pt_id);
    fsm.register(Transition.U_ENDPOINT_CLEAN, u_ps_id, ec_id);
    fsm.register(Transition.U_ADVANCE_TRANSFER, u_pt_id, u_at_id);
    fsm.register(Transition.U_TRANSFER_CLEAN1, u_pt_id, tc_id);
    fsm.register(Transition.UPLOADING, u_at_id, u_id);
    fsm.register(Transition.U_TRANSFER_CLEAN2, u_at_id, tc_id);
    fsm.register(Transition.U_CLEAN, u_id, tc_id);
    //
    fsm.register(Transition.ENDPOINT_CLEAN, tc_id, ec_id);
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

  //**** DOWNLOAD ****
  static FSMEventHandler initDownload
    = new FSMEventHandler<LibTExternal, LibTInternal, HopsTorrentDownloadEvent.StartRequest>() {
      @Override
      public FSMTransition handle(LibTExternal es, LibTInternal is, HopsTorrentDownloadEvent.StartRequest req) {
        if (es.library.containsTorrent(req.torrentId)) {
          throw new RuntimeException("library and fsm do not agree - cannot fix it while running - logic error");
        }
        LOG.info("{}accepting new download:{}", req.getBaseId(), req.torrentName);
        es.library.prepareDownload(req.projectId, req.torrentId, req.torrentName);
        is.setDownloadInit(req);
        is.setTorrentBuilder(new TorrentBuilder());
        setupStorageEndpoints(es, is);
        setupManifestStream(is, req.manifest);
        if (is.endpointRegistration.isComplete()) {
          is.torrentBuilder.setEndpoints(is.endpointRegistration.getSetup());
          setupTransfer(es, is);
          return Transition.D_PREPARE_TRANSFER1;
        } else {
          return Transition.D_PREPARE_STORAGE;
        }
      }
    };

  private static FSMEventHandler downloadStop1(final Transition t) {
    return new FSMEventHandler<LibTExternal, LibTInternal, HopsTorrentStopEvent.Request>() {
      @Override
      public FSMTransition handle(LibTExternal es, LibTInternal is, HopsTorrentStopEvent.Request req) {
        LOG.info("<{}>stop received during download:{} - move to cleaning endpoints", req.getBaseId(), req.torrentId);
        downloadReqFailed(es, is);
        is.setStopReq(req);
        cleanStorageEndpoints(es, is, is.endpointRegistration.selfCleaning());
        return t;
      }
    };
  }

  private static FSMEventHandler downloadStop2(final Transition t) {
    return new FSMEventHandler<LibTExternal, LibTInternal, HopsTorrentStopEvent.Request>() {
      @Override
      public FSMTransition handle(LibTExternal es, LibTInternal is, HopsTorrentStopEvent.Request req) {
        LOG.info("<{}>stop received during download:{} - move to cleaning transfer", req.getBaseId(), is.torrentId);
        downloadReqFailed(es, is);
        is.setStopReq(req);
        cleanTransfer(es, is);
        return t;
      }
    };
  }

  private static FSMEventHandler downloadStop3(final Transition t) {
    return new FSMEventHandler<LibTExternal, LibTInternal, HopsTorrentStopEvent.Request>() {
      @Override
      public FSMTransition handle(LibTExternal es, LibTInternal is, HopsTorrentStopEvent.Request req) {
        LOG.info("<{}>stop received during download:{} - move to cleaning transfer", req.getBaseId(), is.torrentId);
        is.setStopReq(req);
        cleanTransfer(es, is);
        return t;
      }
    };
  }

  private static FSMStateDef downloadPrepareStorageState() throws FSMException {
    FSMStateDef state = new FSMStateDef();
    state.register(DEndpoint.Success.class, downloadPrepareStorage);
    state.register(HopsTorrentStopEvent.Request.class, downloadStop1(Transition.D_ENDPOINT_CLEAN));
    state.seal();
    return state;
  }

  static FSMEventHandler downloadPrepareStorage
    = new FSMEventHandler<LibTExternal, LibTInternal, DEndpoint.Success>() {
      @Override
      public FSMTransition handle(LibTExternal es, LibTInternal is, DEndpoint.Success resp) {
        LOG.debug("<{}>endpoint:{} prepared", resp.getBaseId(), resp.req.endpointProvider.getName());
        is.endpointRegistration.connected(resp.req.endpointId);
        if (is.endpointRegistration.isComplete()) {
          is.torrentBuilder.setEndpoints(is.endpointRegistration.getSetup());
          setupTransfer(es, is);
          return Transition.D_PREPARE_TRANSFER2;
        } else {
          return Transition.D_CONTINUE_PREPARE_STORAGE;
        }
      }
    };

  private static FSMStateDef downloadPrepareTransferState() throws FSMException {
    FSMStateDef state = new FSMStateDef();
    state.register(StartTorrent.Response.class, downloadPrepareTransfer);
    state.register(HopsTorrentStopEvent.Request.class, downloadStop2(Transition.D_TRANSFER_CLEAN1));
    state.seal();
    return state;
  }

  static FSMEventHandler downloadPrepareTransfer
    = new FSMEventHandler<LibTExternal, LibTInternal, StartTorrent.Response>() {
      @Override
      public FSMTransition handle(LibTExternal es, LibTInternal is, StartTorrent.Response resp) {
        if (resp.result.isSuccess()) {
          LOG.debug("<{}>torrent:{} - prepared", resp.getBaseId(), resp.overlayId());
          if (is.downloadReq.isPresent()) {
            getManifest(es, is);
            return Transition.D_GET_MANIFEST;
          } else {
            //on restart I already have the manifest
            readManifest(is);
            prepareDetails(is);
            is.setTorrent(is.getTorrentBuilder().getTorrent());
            advanceTransfer(es, is);
            return Transition.D_ADVANCE_TRANSFER1;
          }
        } else {
          LOG.warn("<{}>torrent:{} - start failed", resp.getBaseId(), resp.overlayId());
          throw new RuntimeException("todo deal with failure");
        }
      }
    };

  private static FSMStateDef downloadGetManifestState() throws FSMException {
    FSMStateDef state = new FSMStateDef();
    state.register(GetRawTorrent.Response.class, downloadGetManifest);
    state.register(HopsTorrentStopEvent.Request.class, downloadStop2(Transition.D_MANIFEST_CLEAN));
    state.seal();
    return state;
  }

  static FSMEventHandler downloadGetManifest
    = new FSMEventHandler<LibTExternal, LibTInternal, GetRawTorrent.Response>() {
      @Override
      public FSMTransition handle(LibTExternal es, LibTInternal is, GetRawTorrent.Response resp) {
        if (resp.result.isSuccess()) {
          LOG.debug("<{}>torrent:{} - advanced", resp.getBaseId(), resp.overlayId());
          readManifest2(is, resp.result.getValue());
          prepareDetails(is);
          is.setTorrent(is.getTorrentBuilder().getTorrent());
          advanceTransfer(es, is);
          return Transition.D_ADVANCE_TRANSFER2;
        } else {
          LOG.warn("<{}>torrent:{} - start failed", resp.getBaseId(), resp.overlayId());
          throw new RuntimeException("todo deal with failure");
        }
      }
    };

  private static FSMStateDef downloadAdvanceTransferState() throws FSMException {
    FSMStateDef state = new FSMStateDef();
    state.register(SetupTransfer.Response.class, downloadAdvanceTransfer);
    state.register(HopsTorrentStopEvent.Request.class, downloadStop2(Transition.D_TRANSFER_CLEAN2));
    state.seal();
    return state;
  }

  static FSMEventHandler downloadAdvanceTransfer
    = new FSMEventHandler<LibTExternal, LibTInternal, SetupTransfer.Response>() {
      @Override
      public FSMTransition handle(LibTExternal es, LibTInternal is, SetupTransfer.Response resp) {
        if (resp.result.isSuccess()) {
          LOG.debug("<{}>torrent:{} - transfer - set up", resp.getBaseId(), resp.overlayId());
          if (!es.library.containsTorrent(resp.overlayId())) {
            throw new RuntimeException("mismatch between library and fsm - critical logical error");
          }
          downloadReqSuccess(es, is);
          return Transition.DOWNLOADING;
        } else {
          LOG.debug("<{}>torrent:{} - transfer - set up failed", resp.getBaseId(), resp.overlayId());
          throw new RuntimeException("todo deal with failure");
        }
      }
    };

  private static FSMStateDef downloadingState() throws FSMException {
    FSMStateDef state = new FSMStateDef();
    state.register(DownloadSummaryEvent.class, downloadCompleted);
    state.register(HopsTorrentStopEvent.Request.class, downloadStop3(Transition.D_CLEAN));
    state.seal();
    return state;
  }

  static FSMEventHandler downloadCompleted
    = new FSMEventHandler<LibTExternal, LibTInternal, DownloadSummaryEvent>() {
      @Override
      public FSMTransition handle(LibTExternal es, LibTInternal is, DownloadSummaryEvent event) {
        LOG.debug("<{}>torrent:{} - download completed", event.getBaseId(), event.torrentId);
        if (!es.library.containsTorrent(event.torrentId)) {
          throw new RuntimeException("mismatch between library and fsm - critical logical error");
        }
        es.library.finishDownload(event.torrentId);
        return Transition.UPLOADING2;
      }
    };

  //**** UPLOAD ****
  static FSMEventHandler initUpload
    = new FSMEventHandler<LibTExternal, LibTInternal, HopsTorrentUploadEvent.Request>() {
      @Override
      public FSMTransition handle(LibTExternal es, LibTInternal is, HopsTorrentUploadEvent.Request req) {
        if (es.library.containsTorrent(req.torrentId)) {
          throw new RuntimeException("library and fsm do not agree - cannot fix it while running - logic error");
        }
        LOG.info("<{}>accepting new upload:{}", req.getBaseId(), req.torrentName);
        es.library.prepareUpload(req.projectId, req.torrentId, req.torrentName);
        is.setUploadInit(req);
        is.setTorrentBuilder(new TorrentBuilder());
        setupStorageEndpoints(es, is);
        setupManifestStream(is, req.manifestResource);
        if (is.endpointRegistration.isComplete()) {
          is.torrentBuilder.setEndpoints(is.endpointRegistration.getSetup());
          setupTransfer(es, is);
          return Transition.U_PREPARE_TRANSFER1;
        } else {
          return Transition.U_PREPARE_STORAGE;
        }
      }
    };

  private static FSMEventHandler uploadStop1(final Transition t) {
    return new FSMEventHandler<LibTExternal, LibTInternal, HopsTorrentStopEvent.Request>() {
      @Override
      public FSMTransition handle(LibTExternal es, LibTInternal is, HopsTorrentStopEvent.Request req) {
        LOG.info("<{}>stop received during upload:{} - move to cleaning endpoints", req.getBaseId(), req.torrentId);
        uploadReqFailed(es, is);
        is.setStopReq(req);
        cleanStorageEndpoints(es, is, is.endpointRegistration.selfCleaning());
        return t;
      }
    };
  }

  private static FSMEventHandler uploadStop2(final Transition t) {
    return new FSMEventHandler<LibTExternal, LibTInternal, HopsTorrentStopEvent.Request>() {
      @Override
      public FSMTransition handle(LibTExternal es, LibTInternal is, HopsTorrentStopEvent.Request req) {
        LOG.info("<{}>stop received during upload:{} - move to cleaning endpoints", req.getBaseId(), req.torrentId);
        uploadReqFailed(es, is);
        is.setStopReq(req);
        cleanTransfer(es, is);
        return t;
      }
    };
  }

  private static FSMEventHandler uploadStop3(final Transition t) {
    return new FSMEventHandler<LibTExternal, LibTInternal, HopsTorrentStopEvent.Request>() {
      @Override
      public FSMTransition handle(LibTExternal es, LibTInternal is, HopsTorrentStopEvent.Request req) {
        LOG.info("<{}>stop received during seeding:{} - move to cleaning endpoints", req.getBaseId(), req.torrentId);
        is.setStopReq(req);
        cleanTransfer(es, is);
        return t;
      }
    };
  }

  private static FSMStateDef uploadPrepareStorageState() throws FSMException {
    FSMStateDef state = new FSMStateDef();
    state.register(DEndpoint.Success.class, uploadPrepareStorage);
    state.register(HopsTorrentStopEvent.Request.class, uploadStop1(Transition.U_ENDPOINT_CLEAN));
    state.seal();
    return state;
  }

  static FSMEventHandler uploadPrepareStorage
    = new FSMEventHandler<LibTExternal, LibTInternal, DEndpoint.Success>() {
      @Override
      public FSMTransition handle(LibTExternal es, LibTInternal is, DEndpoint.Success resp) {
        LOG.debug("<{}>endpoint:{} prepared", resp.getBaseId(), resp.req.endpointProvider.getName());
        is.endpointRegistration.connected(resp.req.endpointId);
        if (is.endpointRegistration.isComplete()) {
          is.torrentBuilder.setEndpoints(is.endpointRegistration.getSetup());
          setupTransfer(es, is);
          return Transition.U_PREPARE_TRANSFER2;
        } else {
          return Transition.U_CONTINUE_PREPARE_STORAGE;
        }
      }
    };

  private static FSMStateDef uploadPrepareTransferState() throws FSMException {
    FSMStateDef state = new FSMStateDef();
    state.register(StartTorrent.Response.class, uploadPrepareTransfer);
    state.register(HopsTorrentStopEvent.Request.class, uploadStop2(Transition.U_TRANSFER_CLEAN1));
    state.seal();
    return state;
  }

  static FSMEventHandler uploadPrepareTransfer
    = new FSMEventHandler<LibTExternal, LibTInternal, StartTorrent.Response>() {
      @Override
      public FSMTransition handle(LibTExternal es, LibTInternal is, StartTorrent.Response resp) {
        if (resp.result.isSuccess()) {
          LOG.debug("<{}>torrent:{} - prepared", resp.getBaseId(), resp.overlayId());
          readManifest(is);
          prepareDetails(is);
          is.setTorrent(is.getTorrentBuilder().getTorrent());
          advanceTransfer(es, is);
          return Transition.U_ADVANCE_TRANSFER;
        } else {
          LOG.warn("<{}>torrent:{} - start failed", resp.getBaseId(), resp.overlayId());
          throw new RuntimeException("todo deal with failure");
        }
      }
    };

  private static FSMStateDef uploadAdvanceTransferState() throws FSMException {
    FSMStateDef state = new FSMStateDef();
    state.register(SetupTransfer.Response.class, uploadAdvanceTransfer);
    state.register(HopsTorrentStopEvent.Request.class, uploadStop2(Transition.U_TRANSFER_CLEAN2));
    state.seal();
    return state;
  }

  static FSMEventHandler uploadAdvanceTransfer
    = new FSMEventHandler<LibTExternal, LibTInternal, SetupTransfer.Response>() {
      @Override
      public FSMTransition handle(LibTExternal es, LibTInternal is, SetupTransfer.Response resp) {
        if (resp.result.isSuccess()) {
          LOG.debug("<{}>torrent:{} - transfer - set up", resp.getBaseId(), resp.overlayId());
          if (!es.library.containsTorrent(is.torrentId)) {
            throw new RuntimeException("mismatch between library and fsm - critical logical error");
          }
          uploadReqSuccess(es, is);
          return Transition.UPLOADING;
        } else {
          LOG.debug("<{}>torrent:{} - transfer - set up failed", resp.getBaseId(), resp.overlayId());
          throw new RuntimeException("todo deal with failure");
        }
      }
    };

  private static FSMStateDef uploadingState() throws FSMException {
    FSMStateDef state = new FSMStateDef();
    state.register(HopsTorrentStopEvent.Request.class, uploadStop3(Transition.U_CLEAN));
    state.seal();
    return state;
  }

  //********************************************************************************************************************
//  private static FSMEventHandler storageError1(Transition t) {
//    return new FSMEventHandler<LibTExternal, LibTInternal, DEndpoint.Failed>() {
//      @Override
//      public FSMTransition handle(LibTExternal es, LibTInternal is, DEndpoint.Failed resp) {
//        LOG.info("<{}>endpoint error received for torrent:{} - move to cleaning endpoints", resp.getBaseId(),
//          resp.req.torrentId);
//        if (is.downloadReq.isPresent()) {
//          es.proxy.answer(is.downloadReq.get(), is.downloadReq.get().failed(Result.internalFailure(
//            (Exception) resp.cause)));
//          is.finishDownloadReq();
//        }
//        if (is.uploadReq.isPresent()) {
//          es.proxy.answer(is.uploadReq.get(), is.uploadReq.get().failed(Result.internalFailure((Exception) resp.cause)));
//          is.finishUploadReq();
//        }
//        cleanStorageEndpoints(es, is, is.endpointRegistration.selfCleaning());
//        return t;
//      }
//    };
//  }
//
//  private static FSMEventHandler storageError2() {
//    return new FSMEventHandler<LibTExternal, LibTInternal, DEndpoint.Failed>() {
//      @Override
//      public FSMTransition handle(LibTExternal es, LibTInternal is, DEndpoint.Failed resp) {
//        LOG.info("<{}>endpoint error received for torrent:{} - move to cleaning transfer", resp.getBaseId(),
//          resp.req.torrentId);
//        if (is.downloadReq.isPresent()) {
//          es.proxy.answer(is.downloadReq.get(), is.downloadReq.get().failed(Result.internalFailure(
//            (Exception) resp.cause)));
//          is.finishDownloadReq();
//        }
//        if (is.uploadReq.isPresent()) {
//          es.proxy.answer(is.uploadReq.get(), is.uploadReq.get().failed(Result.internalFailure((Exception) resp.cause)));
//          is.finishUploadReq();
//        }
//        cleanTransfer(es, is);
//        return Transition.;
//      }
//    };
//  }
  private static FSMStateDef endpointCleaningState() throws FSMException {
    FSMStateDef state = new FSMStateDef();
    state.register(DEndpoint.Disconnected.class, endpointCleaning);
    state.seal();
    return state;
  }

  static FSMEventHandler endpointCleaning
    = new FSMEventHandler<LibTExternal, LibTInternal, DEndpoint.Disconnected>() {
      @Override
      public FSMTransition handle(LibTExternal es, LibTInternal is, DEndpoint.Disconnected resp) {
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
        return Transition.ENDPOINT_CLEAN;
      }
    };

  private static DurableStorageProvider getStorageProvider(Identifier selfId) {
    return new DiskComp.StorageProvider(selfId);
  }

  private static void setupManifestStream(LibTInternal is, HDFSResource manifestResource) {
    MyStream manifestStream = new MyStream(new DiskEndpoint(), new DiskResource(manifestResource.dirPath,
      manifestResource.fileName));
    Identifier manifestEndpointId = is.endpointRegistration.nameToId(manifestStream.endpoint.getEndpointName());
    FileId manifestFileId = TorrentIds.fileId(is.torrentId, MyTorrent.MANIFEST_ID);
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
      es.proxy.trigger(new DEndpoint.Connect(is.torrentId, endpointId, storageProvider), es.endpointPort());
    }
  }

  private static void cleanStorageEndpoints(LibTExternal es, LibTInternal is, Set<Identifier> endpoints) {
    for (Identifier endpointId : endpoints) {
      es.proxy.trigger(new DEndpoint.Disconnect(is.torrentId, endpointId), es.endpointPort());
    }
  }

  private static void readManifest(LibTInternal is) {
    StreamResource manifestResource = is.getTorrentBuilder().getManifestStream().getValue1().resource;
    Result<ManifestJSON> manifestJSON = DiskHelper.readManifest((DiskResource) manifestResource);
    if (!manifestJSON.isSuccess()) {
      throw new RuntimeException("todo deal with failure");
    }
    Either<MyTorrent.Manifest, ManifestJSON> manifestResult = Either.right(manifestJSON.getValue());
    is.getTorrentBuilder().setManifest(is.getTorrentId(), manifestResult);
  }

  private static void readManifest2(LibTInternal is, MyTorrent.Manifest manifest) {
    Either<MyTorrent.Manifest, ManifestJSON> manifestResult = Either.left(manifest);
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
    StartTorrent.Request req = new StartTorrent.Request(is.getTorrentId(), is.getPartners());
    es.getProxy().trigger(req, es.torrentMngrPort());
  }

  private static void cleanTransfer(LibTExternal es, LibTInternal is) {
    StopTorrent.Request req = new StopTorrent.Request(is.getTorrentId());
    es.getProxy().trigger(req, es.torrentMngrPort());
  }

  private static void getManifest(LibTExternal es, LibTInternal is) {
    GetRawTorrent.Request req = new GetRawTorrent.Request(is.torrentId);
    es.getProxy().trigger(req, es.transferCtrlPort());
  }

  private static void advanceTransfer(LibTExternal es, LibTInternal is) {
    SetupTransfer.Request req = new SetupTransfer.Request(is.getTorrentId(), is.getTorrent());
    es.getProxy().trigger(req, es.transferCtrlPort());
  }

  private static void downloadReqFailed(LibTExternal es, LibTInternal is) {
    if (is.downloadReq.isPresent()) {
      es.proxy.answer(is.downloadReq.get(), is.downloadReq.get().
        failed(Result.logicalFail("concurrent stop event with download:" + is.torrentId)));
    } else {
      is.dRestartReq.get().failed();
    }
    is.finishDownloadReq();
  }

  private static void downloadReqSuccess(LibTExternal es, LibTInternal is) {
    es.library.download(is.torrentId, is.torrentBuilder.getManifestStream().getValue1());
    if (is.downloadReq.isPresent()) {
      es.getProxy().answer(is.getDownloadReq(), is.getDownloadReq().success(Result.success(true)));
    } else {
      is.dRestartReq.get().success();
    }
    is.finishDownloadReq();
  }

  private static void uploadReqFailed(LibTExternal es, LibTInternal is) {
    if (is.uploadReq.isPresent()) {
      es.proxy.answer(is.uploadReq.get(), is.uploadReq.get().
        failed(Result.logicalFail("concurrent stop event with upload:" + is.torrentId)));
    } else {
      is.uRestartReq.get().failed();
    }
    is.finishUploadReq();
  }

  private static void uploadReqSuccess(LibTExternal es, LibTInternal is) {
    es.library.upload(is.torrentId, is.torrentBuilder.getManifestStream().getValue1());
    if (is.uploadReq.isPresent()) {
      es.getProxy().answer(is.getUploadReq(), is.getUploadReq().success(Result.success(true)));
    } else {
      is.uRestartReq.get().success();
    }
    is.finishUploadReq();
  }

  public static class LibTInternal implements FSMInternalState {

    public final FSMId fsmId;
    public final String fsmName;
    //either
    private Optional<HopsTorrentUploadEvent.Request> uploadReq = Optional.absent();
    private Optional<TorrentRestart.DiskUpldReq> uRestartReq = Optional.absent();
    private Optional<HopsTorrentDownloadEvent.StartRequest> downloadReq = Optional.absent();
    private Optional<TorrentRestart.DiskDwldReq> dRestartReq = Optional.absent();
    //
    private HopsTorrentStopEvent.Request stopReq;
    //
    private OverlayId torrentId;
    private List<KAddress> partners;
    private final EndpointRegistration endpointRegistration = new EndpointRegistration();
    private TorrentBuilder torrentBuilder;
    private MyTorrent torrent;

    public LibTInternal(FSMId fsmId) {
      this.fsmId = fsmId;
      this.fsmName = LocalLibTorrentFSM.NAME;
    }

    public void setUploadInit(HopsTorrentUploadEvent.Request req) {
      this.uploadReq = Optional.of(req);
      this.torrentId = req.torrentId;
      this.partners = new LinkedList<>();
    }

    public void setUploadRestartInit(TorrentRestart.DiskUpldReq req) {
      this.uRestartReq = Optional.of(req);
      this.torrentId = req.torrentId;
      this.partners = req.partners;
    }

    public void setDownloadInit(HopsTorrentDownloadEvent.StartRequest req) {
      this.downloadReq = Optional.of(req);
      this.torrentId = req.torrentId;
      this.partners = req.partners;
    }

    public void setDownloadRestartInit(TorrentRestart.DiskDwldReq req) {
      this.dRestartReq = Optional.of(req);
      this.torrentId = req.torrentId;
      this.partners = req.partners;
    }

    public HopsTorrentUploadEvent.Request getUploadReq() {
      return uploadReq.get();
    }

    public void finishUploadReq() {
      uploadReq = Optional.absent();
    }

    public HopsTorrentDownloadEvent.StartRequest getDownloadReq() {
      return downloadReq.get();
    }

    public void finishDownloadReq() {
      downloadReq = Optional.absent();
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

    public Positive torrentStatusPort() {
      return proxy.getNegative(TorrentStatusPort.class).getPair();
    }
  }
}
