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
package se.sics.nstream.hops.libmngr.fsm;

import com.google.common.base.Optional;
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
import se.sics.kompics.Promise;
import se.sics.kompics.fsm.FSMBasicStateNames;
import se.sics.kompics.fsm.FSMException;
import se.sics.kompics.fsm.FSMStateName;
import se.sics.kompics.fsm.handler.FSMBasicEventHandler;
import se.sics.kompics.util.Identifier;
import se.sics.ktoolbox.util.Either;
import se.sics.ktoolbox.util.identifiable.overlay.OverlayId;
import se.sics.ktoolbox.util.network.KAddress;
import se.sics.ktoolbox.util.result.Result;
import se.sics.nstream.FileId;
import se.sics.nstream.StreamId;
import se.sics.nstream.TorrentIds;
import se.sics.nstream.hops.HopsFED;
import se.sics.nstream.hops.hdfs.HDFSComp;
import se.sics.nstream.hops.hdfs.HDFSHelper;
import se.sics.nstream.hops.hdfs.disk.DiskComp;
import se.sics.nstream.hops.hdfs.disk.DiskFED;
import se.sics.nstream.hops.kafka.KafkaComp;
import se.sics.nstream.hops.kafka.KafkaEndpoint;
import se.sics.nstream.hops.kafka.KafkaResource;
import se.sics.nstream.hops.library.Details;
import se.sics.nstream.hops.library.event.core.HopsTorrentDownloadEvent;
import se.sics.nstream.hops.library.event.core.HopsTorrentStopEvent;
import se.sics.nstream.hops.library.event.core.HopsTorrentUploadEvent;
import se.sics.nstream.hops.manifest.DiskHelper;
import se.sics.nstream.hops.manifest.ManifestHelper;
import se.sics.nstream.hops.manifest.ManifestJSON;
import se.sics.nstream.hops.storage.disk.DiskEndpoint;
import se.sics.nstream.hops.storage.disk.DiskResource;
import se.sics.nstream.hops.storage.hdfs.HDFSEndpoint;
import se.sics.nstream.hops.storage.hdfs.HDFSResource;
import se.sics.nstream.library.event.torrent.TorrentExtendedStatusEvent;
import se.sics.nstream.library.restart.TorrentRestart;
import se.sics.nstream.storage.durable.DurableStorageProvider;
import se.sics.nstream.storage.durable.events.DEndpoint;
import se.sics.nstream.storage.durable.util.FileExtendedDetails;
import se.sics.nstream.storage.durable.util.MyStream;
import se.sics.nstream.storage.durable.util.StreamEndpoint;
import se.sics.nstream.torrent.event.StartTorrent;
import se.sics.nstream.torrent.event.StopTorrent;
import se.sics.nstream.torrent.status.event.DownloadSummaryEvent;
import se.sics.nstream.torrent.tracking.event.StatusSummaryEvent;
import se.sics.nstream.torrent.transfer.event.ctrl.GetRawTorrent;
import se.sics.nstream.torrent.transfer.event.ctrl.SetupTransfer;
import se.sics.nstream.transfer.MyTorrent;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class LibTHandlers {

  private static final Logger LOG = LoggerFactory.getLogger(LibTFSM.class);

  static FSMBasicEventHandler fallbackDownloadStart
    = new FSMBasicEventHandler<LibTExternal, LibTInternal, HopsTorrentDownloadEvent.StartRequest>() {
      @Override
      public FSMStateName handle(FSMStateName state, LibTExternal es, LibTInternal is,
        HopsTorrentDownloadEvent.StartRequest req) {
        es.getProxy().answer(req, req.fail(Result.logicalFail("torrent:" + is.getTorrentId() + "is active already")));
        return state;
      }
    };

  static FSMBasicEventHandler fallbackUploadStart
    = new FSMBasicEventHandler<LibTExternal, LibTInternal, HopsTorrentUploadEvent.Request>() {
      @Override
      public FSMStateName handle(FSMStateName state, LibTExternal es, LibTInternal is,
        HopsTorrentUploadEvent.Request req) {
        es.getProxy().answer(req, req.fail(Result.logicalFail("torrent:" + is.getTorrentId() + "is active already")));
        return state;
      }
    };

  static FSMBasicEventHandler stop0 
    = new FSMBasicEventHandler<LibTExternal, LibTInternal, HopsTorrentStopEvent.Request>() {
    @Override
    public FSMStateName handle(FSMStateName state, LibTExternal es, LibTInternal is, HopsTorrentStopEvent.Request req)
      throws FSMException {
      LOG.info("<{}>stop received for torrent:{} - stopping", req.getLibTFSMId(), req.torrentId);
      stop(is, es.getProxy(), req);
      success(is, es.getProxy(), Result.success(true));
      es.library.killed(is.getTorrentId());
      return FSMBasicStateNames.FINAL;
    }
  };

  static FSMBasicEventHandler stop1 
    = new FSMBasicEventHandler<LibTExternal, LibTInternal, HopsTorrentStopEvent.Request>() {
    @Override
    public FSMStateName handle(FSMStateName state, LibTExternal es, LibTInternal is, HopsTorrentStopEvent.Request req)
      throws FSMException {
      LOG.info("<{}>stop received for torrent:{} - move to cleaning endpoints", req.getLibTFSMId(), req.torrentId);
      stop(is, es.getProxy(), req);
      cleanStorage(es, is);
      es.library.killing(is.getTorrentId());
      return LibTStates.CLEAN_STORAGE;
    }
  };

  static FSMBasicEventHandler stop2 
    = new FSMBasicEventHandler<LibTExternal, LibTInternal, HopsTorrentStopEvent.Request>() {
    @Override
    public FSMStateName handle(FSMStateName state, LibTExternal es, LibTInternal is, HopsTorrentStopEvent.Request req)
      throws FSMException {
      LOG.info("<{}>stop received for torrent:{} - move to cleaning transfer", req.getLibTFSMId(), req.torrentId);
      stop(is, es.getProxy(), req);
      cleanTransfer(es, is);
      es.library.killing(is.getTorrentId());
      return LibTStates.CLEAN_TRANSFER;
    }
  };

  static FSMBasicEventHandler stop3 
    = new FSMBasicEventHandler<LibTExternal, LibTInternal, HopsTorrentStopEvent.Request>() {
    @Override
    public FSMStateName handle(FSMStateName state, LibTExternal es, LibTInternal is, HopsTorrentStopEvent.Request req)
      throws FSMException {
      LOG.info("<{}>stop received for torrent:{} - move to cleaning transfer", req.getLibTFSMId(), is.getTorrentId());
      stop(is, es.getProxy(), req);
      cleanTransfer(es, is);
      es.library.killing(is.getTorrentId());
      return LibTStates.CLEAN_TRANSFER;
    }
  };

  static FSMBasicEventHandler stop4 
    = new FSMBasicEventHandler<LibTExternal, LibTInternal, HopsTorrentStopEvent.Request>() {
    @Override
    public FSMStateName handle(FSMStateName state, LibTExternal es, LibTInternal is, HopsTorrentStopEvent.Request req)
      throws FSMException {
      LOG.info("<{}>stop received for torrent:{} - working on cleaning", req.getLibTFSMId(), is.getTorrentId());
      stop(is, es.getProxy(), req);
      return state;
    }
  };

  static FSMBasicEventHandler status
    = new FSMBasicEventHandler<LibTExternal, LibTInternal, TorrentExtendedStatusEvent.Request>() {

      @Override
      public FSMStateName handle(FSMStateName state, LibTExternal es, LibTInternal is,
        TorrentExtendedStatusEvent.Request event) throws FSMException {
        is.statusReq = Optional.of(event);
        es.getProxy().trigger(new StatusSummaryEvent.Request(is.getTorrentId()), es.torrentStatusPort());
        return state;
      }
    };

  static FSMBasicEventHandler statusReport
    = new FSMBasicEventHandler<LibTExternal, LibTInternal, StatusSummaryEvent.Response>() {
      @Override
      public FSMStateName handle(FSMStateName state, LibTExternal es, LibTInternal is, StatusSummaryEvent.Response event)
      throws FSMException {
        if(is.statusReq.isPresent()) {
          es.getProxy().answer(is.statusReq.get(), is.statusReq.get().succes(event.result));
        }
        return state;
      }
    };

  static FSMBasicEventHandler initDownload
    = new FSMBasicEventHandler<LibTExternal, LibTInternal, HopsTorrentDownloadEvent.StartRequest>() {
      @Override
      public FSMStateName handle(FSMStateName state, LibTExternal es, LibTInternal is,
        HopsTorrentDownloadEvent.StartRequest req) throws FSMException {
        LOG.info("{}accepting new download:{}", req.getLibTFSMId(), req.torrentName);
        is.setDownload(req);
        MyStream manifestStream = manifestStreamSetup(es, is, req.hdfsEndpoint, req.manifest);
        return downloadInit(es, is, req.torrentId, req.torrentName, req.projectId, req.datasetId, manifestStream,
          req.partners);
      }
    };

  static FSMBasicEventHandler initDownloadRestart
    = new FSMBasicEventHandler<LibTExternal, LibTInternal, TorrentRestart.DwldReq>() {
      @Override
      public FSMStateName handle(FSMStateName state, LibTExternal es, LibTInternal is, TorrentRestart.DwldReq req)
      throws FSMException {
        LOG.info("{}restarting download:{}", req.getLibTFSMId(), req.torrentName);
        is.setDownloadRestart(req);
        return downloadInit(es, is, req.torrentId, req.torrentName, req.projectId, req.datasetId, req.manifestStream,
          req.partners);
      }
    };

  private static LibTStates downloadInit(LibTExternal es, LibTInternal is, OverlayId torrentId, String torrentName,
    Integer projectId, Integer datasetId, MyStream manifestStream, List<KAddress> partners) {
    if (es.library.containsTorrent(torrentId)) {
      throw new RuntimeException("library and fsm do not agree - cannot fix it while running - logic error");
    }
    es.library.prepareDownload(torrentId, projectId, datasetId, torrentName, partners);
    Set<String> preRegisteredEndpoints = saveManifestStream(es, is, manifestStream);
    setupManifestStorageEndpoint(es, is, preRegisteredEndpoints);
    if (is.storageRegistry.isComplete()) {
      is.getSetupState().storageSetupComplete(is.storageRegistry.getSetup());
      setupTransfer(es, is);
      return LibTStates.PREPARE_TRANSFER;
    } else {
      return LibTStates.PREPARE_MANIFEST_STORAGE;
    }
  }

  static FSMBasicEventHandler initUpload
    = new FSMBasicEventHandler<LibTExternal, LibTInternal, HopsTorrentUploadEvent.Request>() {
      @Override
      public FSMStateName handle(FSMStateName state, LibTExternal es, LibTInternal is,
        HopsTorrentUploadEvent.Request req) throws FSMException {
        LOG.info("<{}>accepting new upload:{}", req.getLibTFSMId(), req.torrentName);
        is.setUpload(req);
        MyStream manifestStream = manifestStreamSetup(es, is, req.hdfsEndpoint, req.manifestResource);
        return initUpload(es, is, req.torrentId, req.torrentName, req.projectId, req.datasetId, manifestStream);
      }
    };

  static FSMBasicEventHandler initUploadRestart
    = new FSMBasicEventHandler<LibTExternal, LibTInternal, TorrentRestart.UpldReq>() {
      @Override
      public FSMStateName handle(FSMStateName state, LibTExternal es, LibTInternal is, TorrentRestart.UpldReq req)
      throws FSMException {
        LOG.info("<{}>accepting new upload:{}", req.getLibTFSMId(), req.torrentName);
        is.setUploadRestart(req);
        return initUpload(es, is, req.torrentId, req.torrentName, req.projectId, req.datasetId, req.manifestStream);
      }
    };

  private static LibTStates initUpload(LibTExternal es, LibTInternal is, OverlayId torrentId, String torrentName,
    Integer projectId, Integer datasetId, MyStream manifestStream) {
    if (es.library.containsTorrent(torrentId)) {
      throw new RuntimeException("library and fsm do not agree - cannot fix it while running - logic error");
    }
    es.library.prepareUpload(torrentId, projectId, datasetId, torrentName);
    Set<String> preRegisteredEndpoints = saveManifestStream(es, is, manifestStream);
    setupManifestStorageEndpoint(es, is, preRegisteredEndpoints);
    if (is.storageRegistry.isComplete()) {
      is.getSetupState().storageSetupComplete(is.storageRegistry.getSetup());
      setupTransfer(es, is);
      return LibTStates.PREPARE_TRANSFER;
    } else {
      return LibTStates.PREPARE_MANIFEST_STORAGE;
    }
  }

  static FSMBasicEventHandler prepareManifestStorage
    = new FSMBasicEventHandler<LibTExternal, LibTInternal, DEndpoint.Success>() {
      @Override
      public FSMStateName handle(FSMStateName stateName, LibTExternal es, LibTInternal is, DEndpoint.Success resp) {
        LOG.debug("<{}>endpoint:{} prepared", resp.getLibTFSMId(), resp.req.endpointProvider.getName());
        is.storageRegistry.connected(resp.req.endpointId);
        if (is.storageRegistry.isComplete()) {
          is.getSetupState().storageSetupComplete(is.storageRegistry.getSetup());
          setupTransfer(es, is);
          return LibTStates.PREPARE_TRANSFER;
        } else {
          return LibTStates.PREPARE_MANIFEST_STORAGE;
        }
      }
    };

  static FSMBasicEventHandler prepareTransfer
    = new FSMBasicEventHandler<LibTExternal, LibTInternal, StartTorrent.Response>() {
      @Override
      public FSMStateName handle(FSMStateName state, LibTExternal es, LibTInternal is, StartTorrent.Response resp) {
        if (resp.result.isSuccess()) {
          LOG.debug("<{}>torrent:{} - prepared", resp.getLibTFSMId(), resp.overlayId());
          if (is.getSetupState().isDownloadSetup()) {
            getManifest(es, is);
            return LibTStates.DOWNLOAD_MANIFEST;
          } else {
            //on download restart the manifest is present on endpoint
            //on upload req/restart the manifest is present on endpoint
            readManifest(es, is);
            prepareBasicDetails(es, is);
            is.advanceTransfer();
            advanceTransfer(es, is);
            return LibTStates.ADVANCE_TRANSFER;
          }
        } else {
          LOG.warn("<{}>torrent:{} - start failed", resp.getLibTFSMId(), resp.overlayId());
          throw new RuntimeException("todo deal with failure");
        }
      }
    };

  static FSMBasicEventHandler downloadManifest
    = new FSMBasicEventHandler<LibTExternal, LibTInternal, GetRawTorrent.Response>() {
      @Override
      public FSMStateName handle(FSMStateName state, LibTExternal es, LibTInternal is, GetRawTorrent.Response resp)
      throws FSMException {
        if (resp.result.isSuccess()) {
          LOG.debug("<{}>torrent:{} - advanced", resp.getLibTFSMId(), resp.overlayId());
          writeManifest(es, is, resp.result.getValue());
          if (withExtendedDetails(es)) {
            success(is, es.getProxy(), Result.success(true));
            return LibTStates.EXTENDED_DETAILS;
          } else {
            prepareBasicDetails(es, is);
            is.advanceTransfer();
            advanceTransfer(es, is);
            return LibTStates.ADVANCE_TRANSFER;
          }

        } else {
          LOG.warn("<{}>torrent:{} - start failed", resp.getLibTFSMId(), resp.overlayId());
          throw new RuntimeException("todo deal with failure");
        }
      }
    };

  static FSMBasicEventHandler extendedDetails
    = new FSMBasicEventHandler<LibTExternal, LibTInternal, HopsTorrentDownloadEvent.AdvanceRequest>() {
      @Override
      public FSMStateName handle(FSMStateName state, LibTExternal es, LibTInternal is,
        HopsTorrentDownloadEvent.AdvanceRequest req) throws FSMException {
        if (req.result.isSuccess()) {
          LOG.debug("<{}>torrent:{} - extended details", req.getLibTFSMId(), req.torrentId);
          is.setDownloadAdvance(req);
          if (setupFileStorageEndpoints(es, is, req.kafkaEndpoint)) {
            return LibTStates.PREPARE_FILES_STORAGE;
          } else {
            prepareBasicDetails(es, is);
            is.advanceTransfer();
            advanceTransfer(es, is);
            return LibTStates.ADVANCE_TRANSFER;
          }
        } else {
          LOG.warn("<{}>torrent:{} - start failed", req.getLibTFSMId(), req.torrentId);
          throw new RuntimeException("todo deal with failure");
        }
      }
    };

  static FSMBasicEventHandler prepareFilesStorage
    = new FSMBasicEventHandler<LibTExternal, LibTInternal, DEndpoint.Success>() {
      @Override
      public FSMStateName handle(FSMStateName stateName, LibTExternal es, LibTInternal is, DEndpoint.Success resp) {
        LOG.debug("<{}>endpoint:{} prepared", resp.getLibTFSMId(), resp.req.endpointProvider.getName());
        is.storageRegistry.connected(resp.req.endpointId);
        if (is.storageRegistry.isComplete()) {
          prepareExtendedDetails(es, is);
          is.getSetupState().storageSetupComplete(is.storageRegistry.getSetup());
          is.advanceTransfer();
          advanceTransfer(es, is);
          return LibTStates.ADVANCE_TRANSFER;
        } else {
          return LibTStates.PREPARE_FILES_STORAGE;
        }
      }
    };

  static FSMBasicEventHandler advanceTransfer
    = new FSMBasicEventHandler<LibTExternal, LibTInternal, SetupTransfer.Response>() {
      @Override
      public FSMStateName handle(FSMStateName stateName, LibTExternal es, LibTInternal is, SetupTransfer.Response resp)
      throws FSMException {
        if (resp.result.isSuccess()) {
          LOG.debug("<{}>torrent:{} - transfer - set up", resp.getLibTFSMId(), resp.overlayId());
          if (!es.library.containsTorrent(resp.overlayId())) {
            throw new RuntimeException("mismatch between library and fsm - broken");
          }
          success(is, es.getProxy(), Result.success(true));
          LibTInternal.TComplete state = is.getCompleteState();
          if (state.download) {
            es.library.download(is.getTorrentId(), state.manifestStream);
            return LibTStates.DOWNLOADING;
          } else {
            es.library.upload(is.getTorrentId(), state.manifestStream);
            return LibTStates.UPLOADING;
          }
        } else {
          LOG.debug("<{}>torrent:{} - transfer - set up failed", resp.getLibTFSMId(), resp.overlayId());
          throw new RuntimeException("todo deal with failure");
        }
      }
    };

  static FSMBasicEventHandler downloadCompleted
    = new FSMBasicEventHandler<LibTExternal, LibTInternal, DownloadSummaryEvent>() {
      @Override
      public FSMStateName handle(FSMStateName state, LibTExternal es, LibTInternal is, DownloadSummaryEvent event) {
        LOG.debug("<{}>torrent:{} - download completed", event.getLibTFSMId(), event.torrentId);
        if (!es.library.containsTorrent(event.torrentId)) {
          throw new RuntimeException("mismatch between library and fsm - critical logical error");
        }
        es.library.finishDownload(event.torrentId);
        return LibTStates.UPLOADING;
      }
    };

  static FSMBasicEventHandler endpointCleaning
    = new FSMBasicEventHandler<LibTExternal, LibTInternal, DEndpoint.Disconnected>() {
      @Override
      public FSMStateName handle(FSMStateName state, LibTExternal es, LibTInternal is, DEndpoint.Disconnected resp)
      throws FSMException {
        LOG.debug("<{}>endpoint:{} cleaned",
          new Object[]{resp.getLibTFSMId(), resp.req.endpointId});
        is.storageRegistry.cleaned(resp.req.endpointId);
        es.endpointIdRegistry.release(resp.req.endpointId);
        LOG.debug("<{}> active:{} endpoints:{}",
          new Object[]{resp.getLibTFSMId(), is.storageRegistry.endpointView().size(), is.storageRegistry.
            endpointView()});
        if (is.storageRegistry.cleaningComplete()) {
          success(is, es.getProxy(), Result.success(true));
          es.library.killed(is.getTorrentId());
          LOG.info("<{}>terminating fsm", resp.getLibTFSMId());
          return FSMBasicStateNames.FINAL;
        }
        return LibTStates.CLEAN_STORAGE;
      }
    };

  static FSMBasicEventHandler transferCleaning
    = new FSMBasicEventHandler<LibTExternal, LibTInternal, StopTorrent.Response>() {
      @Override
      public FSMStateName handle(FSMStateName state, LibTExternal es, LibTInternal is, StopTorrent.Response resp) {
        LOG.debug("<{}>torrent:{} cleaned", resp.getLibTFSMId(), resp.torrentId);
        if (!resp.result.isSuccess()) {
          throw new RuntimeException("TODO Alex - what do you do when the cleaning operation fails");
        }
        cleanStorage(es, is);
        return LibTStates.CLEAN_STORAGE;
      }
    };

  private static Set<String> saveManifestStream(LibTExternal es, LibTInternal is, MyStream manifestStream) {
    Set<String> preRegisteredEndpoints = new HashSet<>();
    Identifier manifestEndpointId = es.endpointIdRegistry.lookup(manifestStream.endpoint.getEndpointName());
    if (manifestEndpointId == null) {
      preRegisteredEndpoints.add(manifestStream.endpoint.getEndpointName());
      manifestEndpointId = es.endpointIdRegistry.register(manifestStream.endpoint.getEndpointName());
    }
    FileId manifestFileId = TorrentIds.fileId(is.getTorrentId(), MyTorrent.MANIFEST_ID);
    StreamId manifestStreamId = TorrentIds.streamId(manifestEndpointId, manifestFileId);
    is.getSetupState().setManifestStream(manifestStreamId, manifestStream);
    return preRegisteredEndpoints;
  }

  private static void setupManifestStorageEndpoint(LibTExternal es, LibTInternal is,
    Set<String> preRegisteredEndpoints) {
    List<DurableStorageProvider> storageProviders = new LinkedList<>();
    storageProviders.add(getManifestStorageProvider(es, is));
    setupStorageEndpoints(es, is, storageProviders, preRegisteredEndpoints);
  }

  private static boolean setupFileStorageEndpoints(LibTExternal es, LibTInternal is,
    Optional<KafkaEndpoint> kafkaEndpoint) {
    List<DurableStorageProvider> storageProviders = getFilesStorageProvider(es, is, kafkaEndpoint);
    return setupStorageEndpoints(es, is, storageProviders, new HashSet<String>());
  }

  private static boolean setupStorageEndpoints(LibTExternal es, LibTInternal is,
    List<DurableStorageProvider> providers,
    Set<String> preRegisteredEndpoints) {
    boolean waiting = false;
    for (DurableStorageProvider provider : providers) {
      String endpointName = provider.getName();
      Identifier endpointId = es.endpointIdRegistry.lookup(endpointName);
      if (endpointId == null) {
        endpointId = es.endpointIdRegistry.register(endpointName);
      }
      is.storageRegistry.addWaiting(endpointName, endpointId, provider.getEndpoint());
      es.endpointIdRegistry.use(endpointId);
      es.getProxy().trigger(new DEndpoint.Connect(is.getTorrentId(), endpointId, provider), es.endpointPort());
      waiting = true;
    }
    return waiting;
  }

  private static void cleanStorage(LibTExternal es, LibTInternal is) {
    Set<Identifier> endpoints = is.storageRegistry.selfCleaning();
    for (Identifier endpointId : endpoints) {
      es.getProxy().trigger(new DEndpoint.Disconnect(is.getTorrentId(), endpointId), es.endpointPort());
    }
  }

  private static void readManifest(LibTExternal es, LibTInternal is) {
    MyStream manifestStream = is.getSetupState().getManifestStream();
    Result<ManifestJSON> manifestJSON = readManifestFromStorage(es, manifestStream);
    if (!manifestJSON.isSuccess()) {
      throw new RuntimeException("todo deal with failure");
    }
    Either<MyTorrent.Manifest, ManifestJSON> manifestResult = Either.right(manifestJSON.getValue());
    is.getSetupState().setManifest(manifestResult);
  }

  private static void writeManifest(LibTExternal es, LibTInternal is, MyTorrent.Manifest manifest) {
    writeManifestToStorage(es, is, manifest);
    Either<MyTorrent.Manifest, ManifestJSON> manifestResult = Either.left(manifest);
    is.getSetupState().setManifest(manifestResult);
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
    GetRawTorrent.Request req = new GetRawTorrent.Request(is.getTorrentId());
    es.getProxy().trigger(req, es.transferCtrlPort());
  }

  private static void advanceTransfer(LibTExternal es, LibTInternal is) {
    SetupTransfer.Request req = new SetupTransfer.Request(is.getTorrentId(), is.getCompleteState().torrent);
    es.getProxy().trigger(req, es.transferCtrlPort());
  }

  //DISK/HDFS hack
  private static DurableStorageProvider getManifestStorageProvider(LibTExternal es, LibTInternal is) {
    if (es.fsmType.equals(Details.Types.DISK)) {
      return new DiskComp.StorageProvider(es.selfAdr.getId());
    } else {
      HDFSEndpoint manifestEndpoint = (HDFSEndpoint) is.getSetupState().getManifestStream().endpoint;
      return new HDFSComp.StorageProvider(es.selfAdr.getId(), manifestEndpoint);
    }
  }

  private static List<DurableStorageProvider> getFilesStorageProvider(LibTExternal es, LibTInternal is,
    Optional<KafkaEndpoint> kafka) {
    List<DurableStorageProvider> providers = new LinkedList<>();
    if (es.fsmType.equals(Details.Types.DISK)) {
      providers.add(new DiskComp.StorageProvider(es.selfAdr.getId()));
    } else {
      HDFSEndpoint manifestEndpoint = (HDFSEndpoint) is.getSetupState().getManifestStream().endpoint;
      providers.add(new HDFSComp.StorageProvider(es.selfAdr.getId(), manifestEndpoint));
      if (kafka.isPresent()) {
        providers.add(new KafkaComp.StorageProvider(es.selfAdr.getId(), kafka.get()));
      }
    }
    return providers;
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
    MyStream manifestStream = is.getSetupState().getManifestStream();
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

  private static void prepareBasicDetails(LibTExternal es, LibTInternal is) {
    Map<FileId, FileExtendedDetails> basicDetails = new HashMap<>();
    StreamId manifestStreamId = is.getSetupState().getManifestStreamId();
    MyStream manifestStream = is.getSetupState().getManifestStream();
    if (es.fsmType.equals(Details.Types.DISK)) {
      DiskResource manifestResource = (DiskResource) manifestStream.resource;
      for (Map.Entry<String, FileId> file : is.getSetupState().getFiles().entrySet()) {
        StreamId fileStreamId = manifestStreamId.withFile(file.getValue());
        DiskResource fileResource = manifestResource.withFile(file.getKey());
        MyStream fileStream = manifestStream.withResource(fileResource);
        basicDetails.put(file.getValue(), new DiskFED(fileStreamId, fileStream));
      }
    } else {
      HDFSResource manifestResource = (HDFSResource) manifestStream.resource;
      for (Map.Entry<String, FileId> file : is.getSetupState().getFiles().entrySet()) {
        StreamId fileStreamId = manifestStreamId.withFile(file.getValue());
        HDFSResource fileResource = manifestResource.withFile(file.getKey());
        MyStream fileStream = manifestStream.withResource(fileResource);

        //KAFKA - basic details do not have kafka details
        Optional<Pair<StreamId, MyStream>> kafkaStream = Optional.absent();
        basicDetails.put(file.getValue(), new HopsFED(Pair.with(fileStreamId, fileStream), kafkaStream));
      }
    }
    is.getSetupState().setDetails(basicDetails);
  }

  private static boolean withExtendedDetails(LibTExternal es) {
    if (es.fsmType.equals(Details.Types.DISK)) {
      return false;
    } else {
      return true;
    }
  }

  //only for hops - extended is hdfs + kafka
  private static void prepareExtendedDetails(LibTExternal es, LibTInternal is) {
    Map<FileId, FileExtendedDetails> extDetails = new HashMap<>();
    StreamId manifestStreamId = is.getSetupState().getManifestStreamId();
    MyStream manifestStream = is.getSetupState().getManifestStream();
    HDFSResource manifestResource = (HDFSResource) manifestStream.resource;
    LOG.debug("kafka files:{}", is.auxState.getKafkaDetails().size());
    for (Map.Entry<String, FileId> file : is.getSetupState().getFiles().entrySet()) {
      LOG.debug("preparing file:{}", file.getKey());
      StreamId fileStreamId = manifestStreamId.withFile(file.getValue());
      HDFSResource fileResource = manifestResource.withFile(file.getKey());
      MyStream fileStream = manifestStream.withResource(fileResource);

      //KAFKA
      Optional<Pair<StreamId, MyStream>> kafkaStream = Optional.absent();
      KafkaResource kafkaResource = is.auxState.getKafkaDetails().get(file.getKey());
      if (kafkaResource != null) {
        LOG.debug("preparing kafka:{}", kafkaResource.getSinkName());
        Identifier kafkaEndpointId = is.storageRegistry.nameToId(is.auxState.getKafkaEndpointName());
        StreamEndpoint kafkaEndpoint = is.storageRegistry.getEndpoint(kafkaEndpointId);
        MyStream ks = new MyStream(kafkaEndpoint, kafkaResource);
        StreamId kafkaStreamId = TorrentIds.streamId(kafkaEndpointId, file.getValue());
        kafkaStream = Optional.of(Pair.with(kafkaStreamId, ks));
      }
      extDetails.put(file.getValue(), new HopsFED(Pair.with(fileStreamId, fileStream), kafkaStream));
    }
    is.getSetupState().setDetails(extDetails);
  }

  public static void failed(LibTInternal is, ComponentProxy proxy, Result r) throws FSMException {
    Optional<Promise> p = is.ar.active();
    if (!p.isPresent()) {
      throw new FSMException("no active req");
    }
    proxy.answer(p.get(), p.get().fail(r));
    is.ar.reset();
  }

  public static void success(LibTInternal is, ComponentProxy proxy, Result r) throws FSMException {
    Optional<Promise> p = is.ar.active();
    if (!p.isPresent()) {
      throw new FSMException("no active req");
    }
    proxy.answer(p.get(), p.get().success(r));
    is.ar.reset();
  }

  public static void stop(LibTInternal is, ComponentProxy proxy, HopsTorrentStopEvent.Request stop) throws
    FSMException {
    if (is.ar.isStopping()) {
      proxy.answer(stop, stop.fail(Result.logicalFail("stop already in progress")));
    } else {
      try {
        failed(is, proxy, Result.logicalFail("stop event"));
      } catch (FSMException ex) {
        //no active request - possible when actively downloading/uploading
      }
      is.setStop(stop);
    }
  }
}
