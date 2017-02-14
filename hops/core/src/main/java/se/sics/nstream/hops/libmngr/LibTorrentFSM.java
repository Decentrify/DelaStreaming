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
import org.apache.hadoop.security.UserGroupInformation;
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
import se.sics.ktoolbox.nutil.fsm.FSMOnWrongStateAction;
import se.sics.ktoolbox.nutil.fsm.FSMStateDef;
import se.sics.ktoolbox.nutil.fsm.FSMTransition;
import se.sics.ktoolbox.nutil.fsm.FSMTransitions;
import se.sics.ktoolbox.nutil.fsm.FSMachineDef;
import se.sics.ktoolbox.nutil.fsm.MultiFSM;
import se.sics.ktoolbox.nutil.fsm.genericsetup.OnFSMExceptionAction;
import se.sics.ktoolbox.nutil.fsm.ids.FSMDefId;
import se.sics.ktoolbox.nutil.fsm.ids.FSMId;
import se.sics.ktoolbox.nutil.fsm.ids.FSMStateDefId;
import se.sics.ktoolbox.nutil.fsm.ids.FSMStateId;
import se.sics.ktoolbox.util.Either;
import se.sics.ktoolbox.util.identifiable.Identifier;
import se.sics.ktoolbox.util.identifiable.overlay.OverlayId;
import se.sics.ktoolbox.util.network.KAddress;
import se.sics.ktoolbox.util.result.Result;
import se.sics.nstream.FileId;
import se.sics.nstream.StreamId;
import se.sics.nstream.TorrentIds;
import se.sics.nstream.hops.HopsFED;
import se.sics.nstream.hops.hdfs.HDFSComp;
import se.sics.nstream.hops.hdfs.HDFSEndpoint;
import se.sics.nstream.hops.hdfs.HDFSHelper;
import se.sics.nstream.hops.hdfs.HDFSResource;
import se.sics.nstream.hops.library.Details;
import se.sics.nstream.hops.library.HopsTorrentPort;
import se.sics.nstream.hops.library.event.core.HopsTorrentDownloadEvent;
import se.sics.nstream.hops.library.event.core.HopsTorrentStopEvent;
import se.sics.nstream.hops.library.event.core.HopsTorrentUploadEvent;
import se.sics.nstream.hops.manifest.DiskHelper;
import se.sics.nstream.hops.manifest.ManifestHelper;
import se.sics.nstream.hops.manifest.ManifestJSON;
import se.sics.nstream.library.Library;
import se.sics.nstream.library.endpointmngr.EndpointIdRegistry;
import se.sics.nstream.library.restart.TorrentRestart;
import se.sics.nstream.library.restart.TorrentRestartPort;
import se.sics.nstream.storage.durable.DEndpointCtrlPort;
import se.sics.nstream.storage.durable.DurableStorageProvider;
import se.sics.nstream.storage.durable.disk.DiskComp;
import se.sics.nstream.storage.durable.disk.DiskEndpoint;
import se.sics.nstream.storage.durable.disk.DiskFED;
import se.sics.nstream.storage.durable.disk.DiskResource;
import se.sics.nstream.storage.durable.events.DEndpoint;
import se.sics.nstream.storage.durable.util.FileExtendedDetails;
import se.sics.nstream.storage.durable.util.MyStream;
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
public class LibTorrentFSM {

  private static final Logger LOG = LoggerFactory.getLogger(LibTorrentFSM.class);
  public static final String NAME = "dela-torrent-library-fsm";

  public static enum Transition implements FSMTransition {

    PREPARE_STORAGE_D,
    PREPARE_STORAGE_U,
    PREPARE_TRANSFER_D,
    PREPARE_TRANSFER_U,
    PREPARE_STORAGE_CONTINUE,
    PREPARE_TRANSFER,
    DOWNLOAD_MANIFEST,
    EXTENDED_DETAILS,
    ADVANCE_TRANSFER1,
    ADVANCE_TRANSFER_DM,
    ADVANCE_TRANSFER_EX,
    DOWNLOADING,
    UPLOADING,
    UPLOADING2,
    ENDPOINT_CLEAN,
    ENDPOINT_CLEAN2,
    ENDPOINT_CONTINUE_CLEAN,
    TRANSFER_CLEAN1,
    TRANSFER_CLEAN2,
    TRANSFER_CLEAN3,
    TRANSFER_CLEAN4,
    TRANSFER_CLEAN5,
    TRANSFER_CLEAN_EX
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
      hopsTorrentEvents.add(HopsTorrentDownloadEvent.AdvanceRequest.class);
      hopsTorrentEvents.add(HopsTorrentUploadEvent.Request.class);
      hopsTorrentEvents.add(HopsTorrentStopEvent.Request.class);
      negativePorts.add(Pair.with(hopsTorrentPort, hopsTorrentEvents));
      //
      Class torrentRestartPort = TorrentRestartPort.class;
      List<Class> torrentRestartEvents = new LinkedList<>();
      torrentRestartEvents.add(TorrentRestart.DwldReq.class);
      torrentRestartEvents.add(TorrentRestart.UpldReq.class);
      negativePorts.add(Pair.with(torrentRestartPort, torrentRestartEvents));

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
          fsmEvents.add(HopsTorrentDownloadEvent.AdvanceRequest.class);
          fsmEvents.add(HopsTorrentUploadEvent.Request.class);
          fsmEvents.add(HopsTorrentStopEvent.Request.class);
          fsmEvents.add(TorrentRestart.DwldReq.class);
          fsmEvents.add(TorrentRestart.UpldReq.class);
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
    FSMOnWrongStateAction owsa = new FSMOnWrongStateAction<LibTExternal, LibTInternal>() {
      @Override
      public void handle(FSMStateId state, FSMEvent event, LibTExternal es, LibTInternal is) {
        if (event instanceof HopsTorrentDownloadEvent.StartRequest) {
          HopsTorrentDownloadEvent.StartRequest req = (HopsTorrentDownloadEvent.StartRequest) event;
          es.proxy.answer(req, req.failed(Result.logicalFail("torrent:" + is.torrentId + "is active already")));
        } else if (event instanceof HopsTorrentDownloadEvent.AdvanceRequest) {
          HopsTorrentDownloadEvent.AdvanceRequest req = (HopsTorrentDownloadEvent.AdvanceRequest) event;
          es.proxy.answer(req, req.fail(Result.logicalFail("torrent:" + is.torrentId + "is active already")));
        } else if (event instanceof HopsTorrentUploadEvent.Request) {
          HopsTorrentUploadEvent.Request req = (HopsTorrentUploadEvent.Request) event;
          es.proxy.answer(req, req.failed(Result.logicalFail("torrent:" + is.torrentId + "is active already")));
        } else {
          LOG.warn("state:{} does not handle event:{} and does not register owsa behaviour", state, event);
        }
      }
    };
    FSMachineDef fsm = FSMachineDef.instance(NAME);
    FSMStateDefId init_id = fsm.registerInitState(initState(owsa));
    FSMStateDefId ps_id = fsm.registerState(prepareStorageState(owsa));
    FSMStateDefId pt_id = fsm.registerState(prepareTransferState(owsa));
    FSMStateDefId dm_id = fsm.registerState(downloadManifestState(owsa));
    FSMStateDefId ex_id = fsm.registerState(extendedDetailsState(owsa));
    FSMStateDefId at_id = fsm.registerState(advanceTransferState(owsa));
    FSMStateDefId d_id = fsm.registerState(downloadingState(owsa));
    FSMStateDefId u_id = fsm.registerState(uploadingState(owsa));
    FSMStateDefId ec_id = fsm.registerState(endpointCleaningState(owsa));
    FSMStateDefId tc_id = fsm.registerState(transferCleaningState(owsa));

    fsm.register(Transition.PREPARE_STORAGE_D, init_id, ps_id);
    fsm.register(Transition.PREPARE_STORAGE_U, init_id, ps_id);
    fsm.register(Transition.PREPARE_TRANSFER_D, init_id, pt_id);
    fsm.register(Transition.PREPARE_TRANSFER_U, init_id, pt_id);
    fsm.register(Transition.PREPARE_STORAGE_CONTINUE, ps_id, ps_id);
    fsm.register(Transition.PREPARE_TRANSFER, ps_id, pt_id);
    fsm.register(Transition.ENDPOINT_CLEAN2, ps_id, ec_id);
    fsm.register(Transition.DOWNLOAD_MANIFEST, pt_id, dm_id);
    fsm.register(Transition.TRANSFER_CLEAN1, pt_id, tc_id);
    fsm.register(Transition.ADVANCE_TRANSFER1, pt_id, at_id);
    fsm.register(Transition.ADVANCE_TRANSFER_DM, dm_id, at_id);
    fsm.register(Transition.EXTENDED_DETAILS, dm_id, ex_id);
    fsm.register(Transition.TRANSFER_CLEAN2, dm_id, tc_id);
    fsm.register(Transition.ADVANCE_TRANSFER_EX, ex_id, at_id);
    fsm.register(Transition.TRANSFER_CLEAN_EX, ex_id, tc_id);
    fsm.register(Transition.DOWNLOADING, at_id, d_id);
    fsm.register(Transition.TRANSFER_CLEAN3, at_id, tc_id);
    fsm.register(Transition.UPLOADING, at_id, u_id);
    fsm.register(Transition.UPLOADING2, d_id, u_id);
    fsm.register(Transition.TRANSFER_CLEAN4, d_id, tc_id);
    fsm.register(Transition.TRANSFER_CLEAN5, u_id, tc_id);
    //
    fsm.register(Transition.ENDPOINT_CLEAN, tc_id, ec_id);
    fsm.register(Transition.ENDPOINT_CONTINUE_CLEAN, ec_id, ec_id);
    fsm.seal();
    return fsm;
  }

  private static FSMStateDef initState(FSMOnWrongStateAction owsa) throws FSMException {
    FSMStateDef state = new FSMStateDef();
    state.setOnWrongStateAction(owsa);
    state.register(HopsTorrentDownloadEvent.StartRequest.class, initDownload);
    state.register(TorrentRestart.DwldReq.class, initDownloadRestart);
    state.register(HopsTorrentUploadEvent.Request.class, initUpload);
    state.register(TorrentRestart.UpldReq.class, initUploadRestart);
    state.seal();
    return state;
  }

  static FSMEventHandler initDownload
    = new FSMEventHandler<LibTExternal, LibTInternal, HopsTorrentDownloadEvent.StartRequest>() {
      @Override
      public FSMTransition handle(LibTExternal es, LibTInternal is, HopsTorrentDownloadEvent.StartRequest req) {
        LOG.info("{}accepting new download:{}", req.getBaseId(), req.torrentName);
        is.setDownloadInit(req);
        MyStream manifestStream = manifestStreamSetup(es, is, req.hdfsEndpoint, req.manifest);
        return downloadInit(es, is, req.torrentId, req.torrentName, req.projectId, manifestStream);
      }
    };

  static FSMEventHandler initDownloadRestart
    = new FSMEventHandler<LibTExternal, LibTInternal, TorrentRestart.DwldReq>() {
      @Override
      public FSMTransition handle(LibTExternal es, LibTInternal is, TorrentRestart.DwldReq req) {
        LOG.info("{}restarting download:{}", req.getBaseId(), req.torrentName);
        is.setDownloadRestartInit(req);
        return downloadInit(es, is, req.torrentId, req.torrentName, req.projectId, req.manifestStream);
      }
    };

  private static Transition downloadInit(LibTExternal es, LibTInternal is, OverlayId torrentId, String torrentName,
    Integer projectId, MyStream manifestStream) {
    if (es.library.containsTorrent(torrentId)) {
      throw new RuntimeException("library and fsm do not agree - cannot fix it while running - logic error");
    }
    es.library.prepareDownload(projectId, torrentId, torrentName);
    is.setTorrentBuilder(new TorrentBuilder());
    setupStorageEndpoints(es, is, manifestStream);
    setManifestStream(is, manifestStream);
    if (is.endpointRegistration.isComplete()) {
      is.torrentBuilder.setEndpoints(is.endpointRegistration.getSetup());
      setupTransfer(es, is);
      return Transition.PREPARE_TRANSFER_D;
    } else {
      return Transition.PREPARE_STORAGE_D;
    }
  }

  static FSMEventHandler initUpload
    = new FSMEventHandler<LibTExternal, LibTInternal, HopsTorrentUploadEvent.Request>() {
      @Override
      public FSMTransition handle(LibTExternal es, LibTInternal is, HopsTorrentUploadEvent.Request req) {
        LOG.info("<{}>accepting new upload:{}", req.getBaseId(), req.torrentName);
        is.setUploadInit(req);
        MyStream manifestStream = manifestStreamSetup(es, is, req.hdfsEndpoint, req.manifestResource);
        return initUpload(es, is, req.torrentId, req.torrentName, req.projectId, manifestStream);
      }
    };

  static FSMEventHandler initUploadRestart
    = new FSMEventHandler<LibTExternal, LibTInternal, TorrentRestart.UpldReq>() {
      @Override
      public FSMTransition handle(LibTExternal es, LibTInternal is, TorrentRestart.UpldReq req) {
        LOG.info("<{}>accepting new upload:{}", req.getBaseId(), req.torrentName);
        is.setUploadRestartInit(req);
        return initUpload(es, is, req.torrentId, req.torrentName, req.projectId, req.manifestStream);
      }
    };

  private static Transition initUpload(LibTExternal es, LibTInternal is, OverlayId torrentId, String torrentName,
    Integer projectId, MyStream manifestStream) {
    if (es.library.containsTorrent(torrentId)) {
      throw new RuntimeException("library and fsm do not agree - cannot fix it while running - logic error");
    }
    es.library.prepareUpload(projectId, torrentId, torrentName);
    is.setTorrentBuilder(new TorrentBuilder());
    setupStorageEndpoints(es, is, manifestStream);
    setManifestStream(is, manifestStream);
    if (is.endpointRegistration.isComplete()) {
      is.torrentBuilder.setEndpoints(is.endpointRegistration.getSetup());
      setupTransfer(es, is);
      return Transition.PREPARE_TRANSFER_U;
    } else {
      return Transition.PREPARE_STORAGE_U;
    }
  }

  private static FSMEventHandler stop1(final Transition t) {
    return new FSMEventHandler<LibTExternal, LibTInternal, HopsTorrentStopEvent.Request>() {
      @Override
      public FSMTransition handle(LibTExternal es, LibTInternal is, HopsTorrentStopEvent.Request req) {
        LOG.info("<{}>stop received for torrent:{} - move to cleaning endpoints", req.getBaseId(), req.torrentId);
        reqFailed(es, is);
        is.setStopReq(req);
        cleanStorageEndpoints(es, is, is.endpointRegistration.selfCleaning());
        return t;
      }
    };
  }

  private static FSMEventHandler stop2(final Transition t) {
    return new FSMEventHandler<LibTExternal, LibTInternal, HopsTorrentStopEvent.Request>() {
      @Override
      public FSMTransition handle(LibTExternal es, LibTInternal is, HopsTorrentStopEvent.Request req) {
        LOG.info("<{}>stop received for torrent:{} - move to cleaning transfer", req.getBaseId(), req.torrentId);
        reqFailed(es, is);
        is.setStopReq(req);
        cleanTransfer(es, is);
        return t;
      }
    };
  }

  private static FSMEventHandler stop3(final Transition t) {
    return new FSMEventHandler<LibTExternal, LibTInternal, HopsTorrentStopEvent.Request>() {
      @Override
      public FSMTransition handle(LibTExternal es, LibTInternal is, HopsTorrentStopEvent.Request req) {
        LOG.info("<{}>stop received for torrent:{} - move to cleaning transfer", req.getBaseId(), is.torrentId);
        is.setStopReq(req);
        cleanTransfer(es, is);
        return t;
      }
    };
  }

  private static FSMStateDef prepareStorageState(FSMOnWrongStateAction owsa) throws FSMException {
    FSMStateDef state = new FSMStateDef();
    state.setOnWrongStateAction(owsa);
    state.register(DEndpoint.Success.class, prepareStorage);
    state.register(HopsTorrentStopEvent.Request.class, stop1(Transition.ENDPOINT_CLEAN2));
    state.seal();
    return state;
  }

  static FSMEventHandler prepareStorage
    = new FSMEventHandler<LibTExternal, LibTInternal, DEndpoint.Success>() {
      @Override
      public FSMTransition handle(LibTExternal es, LibTInternal is, DEndpoint.Success resp) {
        LOG.debug("<{}>endpoint:{} prepared", resp.getBaseId(), resp.req.endpointProvider.getName());
        is.endpointRegistration.connected(resp.req.endpointId);
        if (is.endpointRegistration.isComplete()) {
          is.torrentBuilder.setEndpoints(is.endpointRegistration.getSetup());
          setupTransfer(es, is);
          return Transition.PREPARE_TRANSFER;
        } else {
          return Transition.PREPARE_STORAGE_CONTINUE;
        }
      }
    };

  private static FSMStateDef prepareTransferState(FSMOnWrongStateAction owsa) throws FSMException {
    FSMStateDef state = new FSMStateDef();
    state.setOnWrongStateAction(owsa);
    state.register(StartTorrent.Response.class, prepareTransfer);
    state.register(HopsTorrentStopEvent.Request.class, stop2(Transition.TRANSFER_CLEAN1));
    state.seal();
    return state;
  }

  static FSMEventHandler prepareTransfer
    = new FSMEventHandler<LibTExternal, LibTInternal, StartTorrent.Response>() {
      @Override
      public FSMTransition handle(LibTExternal es, LibTInternal is, StartTorrent.Response resp) {
        if (resp.result.isSuccess()) {
          LOG.debug("<{}>torrent:{} - prepared", resp.getBaseId(), resp.overlayId());
          if (is.downloadReq.isPresent()) {
            getManifest(es, is);
            return Transition.DOWNLOAD_MANIFEST;
          } else {
            //on download restart the manifest is present on endpoint
            //on upload req/restart the manifest is present on endpoint
            readManifest(es, is);
            prepareDetails(es, is);
            is.setTorrent(is.getTorrentBuilder().getTorrent());
            advanceTransfer(es, is);
            return Transition.ADVANCE_TRANSFER1;
          }
        } else {
          LOG.warn("<{}>torrent:{} - start failed", resp.getBaseId(), resp.overlayId());
          throw new RuntimeException("todo deal with failure");
        }
      }
    };

  private static FSMStateDef downloadManifestState(FSMOnWrongStateAction owsa) throws FSMException {
    FSMStateDef state = new FSMStateDef();
    state.setOnWrongStateAction(owsa);
    state.register(GetRawTorrent.Response.class, downloadManifest);
    state.register(HopsTorrentStopEvent.Request.class, stop2(Transition.TRANSFER_CLEAN2));
    state.seal();
    return state;
  }

  static FSMEventHandler downloadManifest
    = new FSMEventHandler<LibTExternal, LibTInternal, GetRawTorrent.Response>() {
      @Override
      public FSMTransition handle(LibTExternal es, LibTInternal is, GetRawTorrent.Response resp) {
        if (resp.result.isSuccess()) {
          LOG.debug("<{}>torrent:{} - advanced", resp.getBaseId(), resp.overlayId());
          writeManifest(es, is, resp.result.getValue());
          if (withExtendedDetails(es)) {
            getExtendedDetails(es, is);
            return Transition.EXTENDED_DETAILS;
          } else {
            prepareDetails(es, is);
            is.setTorrent(is.getTorrentBuilder().getTorrent());
            advanceTransfer(es, is);
            return Transition.ADVANCE_TRANSFER_DM;
          }

        } else {
          LOG.warn("<{}>torrent:{} - start failed", resp.getBaseId(), resp.overlayId());
          throw new RuntimeException("todo deal with failure");
        }
      }
    };

  private static FSMStateDef extendedDetailsState(FSMOnWrongStateAction owsa) throws FSMException {
    FSMStateDef state = new FSMStateDef();
    state.setOnWrongStateAction(owsa);
    state.register(HopsTorrentDownloadEvent.AdvanceRequest.class, extendedDetails);
    state.register(HopsTorrentStopEvent.Request.class, stop2(Transition.TRANSFER_CLEAN_EX));
    state.seal();
    return state;
  }

  static FSMEventHandler extendedDetails
    = new FSMEventHandler<LibTExternal, LibTInternal, HopsTorrentDownloadEvent.AdvanceRequest>() {
      @Override
      public FSMTransition handle(LibTExternal es, LibTInternal is, HopsTorrentDownloadEvent.AdvanceRequest req) {
        if (req.result.isSuccess()) {
          LOG.debug("<{}>torrent:{} - extended details", req.getBaseId(), req.torrentId);
          is.setDownloadAdvance(req);
          prepareDetails(es, is);
          is.setTorrent(is.getTorrentBuilder().getTorrent());
          advanceTransfer(es, is);
          return Transition.ADVANCE_TRANSFER_EX;
        } else {
          LOG.warn("<{}>torrent:{} - start failed", req.getBaseId(), req.torrentId);
          throw new RuntimeException("todo deal with failure");
        }
      }
    };

  private static FSMStateDef advanceTransferState(FSMOnWrongStateAction owsa) throws FSMException {
    FSMStateDef state = new FSMStateDef();
    state.setOnWrongStateAction(owsa);
    state.register(SetupTransfer.Response.class, advanceTransfer);
    state.register(HopsTorrentStopEvent.Request.class, stop2(Transition.TRANSFER_CLEAN3));
    state.seal();
    return state;
  }

  static FSMEventHandler advanceTransfer
    = new FSMEventHandler<LibTExternal, LibTInternal, SetupTransfer.Response>() {
      @Override
      public FSMTransition handle(LibTExternal es, LibTInternal is, SetupTransfer.Response resp) {
        if (resp.result.isSuccess()) {
          LOG.debug("<{}>torrent:{} - transfer - set up", resp.getBaseId(), resp.overlayId());
          if (!es.library.containsTorrent(resp.overlayId())) {
            throw new RuntimeException("mismatch between library and fsm - critical logical error");
          }
          return reqSuccess(es, is);
        } else {
          LOG.debug("<{}>torrent:{} - transfer - set up failed", resp.getBaseId(), resp.overlayId());
          throw new RuntimeException("todo deal with failure");
        }
      }
    };

  private static FSMStateDef downloadingState(FSMOnWrongStateAction owsa) throws FSMException {
    FSMStateDef state = new FSMStateDef();
    state.setOnWrongStateAction(owsa);
    state.register(DownloadSummaryEvent.class, downloadCompleted);
    state.register(HopsTorrentStopEvent.Request.class, stop3(Transition.TRANSFER_CLEAN4));
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

  private static FSMStateDef uploadingState(FSMOnWrongStateAction owsa) throws FSMException {
    FSMStateDef state = new FSMStateDef();
    state.setOnWrongStateAction(owsa);
    state.register(HopsTorrentStopEvent.Request.class, stop3(Transition.TRANSFER_CLEAN5));
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
  private static FSMStateDef endpointCleaningState(FSMOnWrongStateAction owsa) throws FSMException {
    FSMStateDef state = new FSMStateDef();
    state.setOnWrongStateAction(owsa);
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

  private static FSMStateDef transferCleaningState(FSMOnWrongStateAction owsa) throws FSMException {
    FSMStateDef state = new FSMStateDef();
    state.setOnWrongStateAction(owsa);
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

  private static void setManifestStream(LibTInternal is, MyStream manifestStream) {
    Identifier manifestEndpointId = is.endpointRegistration.nameToId(manifestStream.endpoint.getEndpointName());
    FileId manifestFileId = TorrentIds.fileId(is.torrentId, MyTorrent.MANIFEST_ID);
    StreamId manifestStreamId = TorrentIds.streamId(manifestEndpointId, manifestFileId);
    is.getTorrentBuilder().setManifestStream(manifestStreamId, manifestStream);
  }

  private static void setupStorageEndpoints(LibTExternal es, LibTInternal is, MyStream manifestStream) {

    List<DurableStorageProvider> storageProviders = new ArrayList<>();
    DurableStorageProvider storageProvider = getStorageProvider(es, manifestStream);
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

  private static void readManifest(LibTExternal es, LibTInternal is) {
    MyStream manifestStream = is.getTorrentBuilder().getManifestStream().getValue1();
    Result<ManifestJSON> manifestJSON = readManifestFromStorage(es, manifestStream);
    if (!manifestJSON.isSuccess()) {
      throw new RuntimeException("todo deal with failure");
    }
    Either<MyTorrent.Manifest, ManifestJSON> manifestResult = Either.right(manifestJSON.getValue());
    is.getTorrentBuilder().setManifest(is.getTorrentId(), manifestResult);
  }

  private static void writeManifest(LibTExternal es, LibTInternal is, MyTorrent.Manifest manifest) {
    writeManifestToStorage(es, is, manifest);
    Either<MyTorrent.Manifest, ManifestJSON> manifestResult = Either.left(manifest);
    is.getTorrentBuilder().setManifest(is.getTorrentId(), manifestResult);
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

  private static void getExtendedDetails(LibTExternal es, LibTInternal is) {
    es.getProxy().answer(is.downloadReq.get(), is.downloadReq.get().success(Result.success(true)));
    is.downloadReq = Optional.absent();
  }

  private static void advanceTransfer(LibTExternal es, LibTInternal is) {
    SetupTransfer.Request req = new SetupTransfer.Request(is.getTorrentId(), is.getTorrent());
    es.getProxy().trigger(req, es.transferCtrlPort());
  }

  private static void reqFailed(LibTExternal es, LibTInternal is) {
    if (is.downloadReq.isPresent()) {
      es.proxy.answer(is.downloadReq.get(), is.downloadReq.get().
        failed(Result.logicalFail("concurrent stop event with download:" + is.torrentId)));
      is.downloadReq = Optional.absent();
    } else if (is.advanceReq.isPresent()) {
      es.proxy.answer(is.advanceReq.get(), is.advanceReq.get().
        fail(Result.logicalFail("concurrent stop event with download:" + is.torrentId)));
      is.advanceReq = Optional.absent();
    } else if (is.dRestartReq.isPresent()) {
      is.dRestartReq.get().failed();
      is.dRestartReq = Optional.absent();
    } else if (is.uploadReq.isPresent()) {
      es.proxy.answer(is.uploadReq.get(), is.uploadReq.get().
        failed(Result.logicalFail("concurrent stop event with upload:" + is.torrentId)));
      is.uploadReq = Optional.absent();
    } else {
      is.uRestartReq.get().failed();
      is.uRestartReq = Optional.absent();
    }
  }

  private static Transition reqSuccess(LibTExternal es, LibTInternal is) {
    if (is.downloadReq.isPresent()) {
      es.getProxy().answer(is.downloadReq.get(), is.downloadReq.get().success(Result.success(true)));
      es.library.download(is.torrentId, is.torrentBuilder.getManifestStream().getValue1());
      is.downloadReq = Optional.absent();
      return Transition.DOWNLOADING;
    } else if (is.advanceReq.isPresent()) {
      es.getProxy().answer(is.advanceReq.get(), is.advanceReq.get().success(Result.success(true)));
      es.library.download(is.torrentId, is.torrentBuilder.getManifestStream().getValue1());
      is.advanceReq = Optional.absent();
      return Transition.DOWNLOADING;
    } else if (is.dRestartReq.isPresent()) {
      is.dRestartReq.get().success();
      es.library.download(is.torrentId, is.torrentBuilder.getManifestStream().getValue1());
      is.dRestartReq = Optional.absent();
      return Transition.DOWNLOADING;
    }
    if (is.uploadReq.isPresent()) {
      es.getProxy().answer(is.getUploadReq(), is.getUploadReq().success(Result.success(true)));
      es.library.upload(is.torrentId, is.torrentBuilder.getManifestStream().getValue1());
      is.uploadReq = Optional.absent();
      return Transition.UPLOADING;
    } else {
      is.uRestartReq.get().success();
      es.library.upload(is.torrentId, is.torrentBuilder.getManifestStream().getValue1());
      is.uRestartReq = Optional.absent();
      return Transition.UPLOADING;
    }
  }

  //DISK/HDFS hack
  private static DurableStorageProvider getStorageProvider(LibTExternal es, MyStream manifestStream) {
    if (es.fsmType.equals(Details.Types.DISK)) {
      return new DiskComp.StorageProvider(es.selfAdr.getId());
    } else {
      return new HDFSComp.StorageProvider(es.selfAdr.getId(), (HDFSEndpoint) (manifestStream.endpoint));
    }
  }

  private static MyStream manifestStreamSetup(LibTExternal es, LibTInternal is, HDFSEndpoint endpoint,
    HDFSResource manifestResource) {
    if (es.fsmType.equals(Details.Types.DISK)) {
      return new MyStream(new DiskEndpoint(), new DiskResource(manifestResource.dirPath, manifestResource.fileName));
    } else {
      return new MyStream(endpoint, manifestResource);
    }
  }

  private static Result<ManifestJSON> readManifestFromStorage(LibTExternal es, MyStream manifestStream) {
    if (es.fsmType.equals(Details.Types.DISK)) {
      return DiskHelper.readManifest((DiskResource) manifestStream.resource);
    } else {
      HDFSEndpoint hdfsEndpoint = (HDFSEndpoint) manifestStream.endpoint;
      //TODO Alex - creating too many ugi's
      UserGroupInformation ugi = UserGroupInformation.createRemoteUser(
        hdfsEndpoint.user);
      return HDFSHelper.readManifest(ugi, hdfsEndpoint, (HDFSResource) manifestStream.resource);
    }
  }

  private static Result<Boolean> writeManifestToStorage(LibTExternal es, LibTInternal is,
    MyTorrent.Manifest manifest) {
    MyStream manifestStream = is.getTorrentBuilder().getManifestStream().getValue1();
    if (es.fsmType.equals(Details.Types.DISK)) {
      ManifestJSON manifestJSON = ManifestHelper.getManifestJSON(manifest.manifestByte);
      return DiskHelper.writeManifest((DiskResource) manifestStream.resource, manifestJSON);
    } else {
      ManifestJSON manifestJSON = ManifestHelper.getManifestJSON(manifest.manifestByte);
      HDFSEndpoint hdfsEndpoint = (HDFSEndpoint) manifestStream.endpoint;
      HDFSResource hdfsResource = (HDFSResource) manifestStream.resource;
      //TODO Alex - creating too many ugi's
      UserGroupInformation ugi = UserGroupInformation.createRemoteUser(hdfsEndpoint.user);
      return HDFSHelper.writeManifest(ugi, hdfsEndpoint, hdfsResource, manifestJSON);
    }
  }

  private static void prepareDetails(LibTExternal es, LibTInternal is) {
    Map<FileId, FileExtendedDetails> extendedDetails = new HashMap<>();
    Pair<StreamId, MyStream> manifestStream = is.getTorrentBuilder().getManifestStream();
    if (es.fsmType.equals(Details.Types.DISK)) {
      DiskResource manifestResource = (DiskResource) manifestStream.getValue1().resource;
      for (Map.Entry<String, FileId> file : is.getTorrentBuilder().getFiles().entrySet()) {
        StreamId fileStreamId = manifestStream.getValue0().withFile(file.getValue());
        DiskResource fileResource = manifestResource.withFile(file.getKey());
        MyStream fileStream = manifestStream.getValue1().withResource(fileResource);
        extendedDetails.put(file.getValue(), new DiskFED(fileStreamId, fileStream));
      }
    } else {
      HDFSResource manifestResource = (HDFSResource) manifestStream.getValue1().resource;
      for (Map.Entry<String, FileId> file : is.getTorrentBuilder().getFiles().entrySet()) {
        StreamId fileStreamId = manifestStream.getValue0().withFile(file.getValue());
        HDFSResource fileResource = manifestResource.withFile(file.getKey());
        MyStream fileStream = manifestStream.getValue1().withResource(fileResource);
        //TODO Alex critical - kafka
        Optional<Pair<StreamId, MyStream>> kafkaStream = Optional.absent();
        extendedDetails.put(file.getValue(), new HopsFED(Pair.with(fileStreamId, fileStream), kafkaStream));
      }
    }
    is.getTorrentBuilder().setExtendedDetails(extendedDetails);
  }

  private static boolean withExtendedDetails(LibTExternal es) {
    if (es.fsmType.equals(Details.Types.DISK)) {
      return false;
    } else {
      return true;
    }
  }

  public static class LibTInternal implements FSMInternalState {

    public final FSMId fsmId;
    public final String fsmName;
    //either
    private Optional<HopsTorrentUploadEvent.Request> uploadReq = Optional.absent();
    private Optional<TorrentRestart.UpldReq> uRestartReq = Optional.absent();
    private Optional<HopsTorrentDownloadEvent.StartRequest> downloadReq = Optional.absent();
    private Optional<HopsTorrentDownloadEvent.AdvanceRequest> advanceReq = Optional.absent();
    private Optional<TorrentRestart.DwldReq> dRestartReq = Optional.absent();
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
      this.fsmName = LibTorrentFSM.NAME;
    }

    public void setUploadInit(HopsTorrentUploadEvent.Request req) {
      this.uploadReq = Optional.of(req);
      this.torrentId = req.torrentId;
      this.partners = new LinkedList<>();
    }

    public void setUploadRestartInit(TorrentRestart.UpldReq req) {
      this.uRestartReq = Optional.of(req);
      this.torrentId = req.torrentId;
      this.partners = req.partners;
    }

    public void setDownloadInit(HopsTorrentDownloadEvent.StartRequest req) {
      this.downloadReq = Optional.of(req);
      this.torrentId = req.torrentId;
      this.partners = req.partners;
    }

    public void setDownloadAdvance(HopsTorrentDownloadEvent.AdvanceRequest req) {
      this.advanceReq = Optional.of(req);
    }

    public void setDownloadRestartInit(TorrentRestart.DwldReq req) {
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
    public final Details.Types fsmType;

    public LibTExternal(KAddress selfAdr, Library library, EndpointIdRegistry endpointIdRegistry,
      Details.Types fsmType) {
      this.selfAdr = selfAdr;
      this.library = library;
      this.endpointIdRegistry = endpointIdRegistry;
      this.fsmType = fsmType;
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
