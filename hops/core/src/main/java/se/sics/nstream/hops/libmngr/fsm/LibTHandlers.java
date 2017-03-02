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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.security.UserGroupInformation;
import org.javatuples.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.ktoolbox.nutil.fsm.api.FSMBasicStateNames;
import se.sics.ktoolbox.nutil.fsm.api.FSMEvent;
import se.sics.ktoolbox.nutil.fsm.api.FSMStateName;
import se.sics.ktoolbox.nutil.fsm.handler.FSMEventHandler;
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
import se.sics.nstream.hops.libmngr.TorrentBuilder;
import se.sics.nstream.hops.library.Details;
import se.sics.nstream.hops.library.event.core.HopsTorrentDownloadEvent;
import se.sics.nstream.hops.library.event.core.HopsTorrentStopEvent;
import se.sics.nstream.hops.library.event.core.HopsTorrentUploadEvent;
import se.sics.nstream.hops.manifest.DiskHelper;
import se.sics.nstream.hops.manifest.ManifestHelper;
import se.sics.nstream.hops.manifest.ManifestJSON;
import se.sics.nstream.library.restart.TorrentRestart;
import se.sics.nstream.storage.durable.DurableStorageProvider;
import se.sics.nstream.storage.durable.disk.DiskComp;
import se.sics.nstream.storage.durable.disk.DiskEndpoint;
import se.sics.nstream.storage.durable.disk.DiskFED;
import se.sics.nstream.storage.durable.disk.DiskResource;
import se.sics.nstream.storage.durable.events.DEndpoint;
import se.sics.nstream.storage.durable.util.FileExtendedDetails;
import se.sics.nstream.storage.durable.util.MyStream;
import se.sics.nstream.torrent.event.StartTorrent;
import se.sics.nstream.torrent.event.StopTorrent;
import se.sics.nstream.torrent.status.event.DownloadSummaryEvent;
import se.sics.nstream.torrent.transfer.event.ctrl.GetRawTorrent;
import se.sics.nstream.torrent.transfer.event.ctrl.SetupTransfer;
import se.sics.nstream.transfer.MyTorrent;

/**
 *
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class LibTHandlers {
  private static final Logger LOG = LoggerFactory.getLogger(LibTFSM.class);
  
  static FSMEventHandler fallbackHandler = new FSMEventHandler<LibTExternal, LibTInternal, FSMEvent>() {
      @Override
      public FSMStateName handle(FSMStateName state, LibTExternal es, LibTInternal is, FSMEvent event) {
        if (event instanceof HopsTorrentDownloadEvent.StartRequest) {
          HopsTorrentDownloadEvent.StartRequest req = (HopsTorrentDownloadEvent.StartRequest) event;
          es.getProxy().answer(req, req.failed(Result.logicalFail("torrent:" + is.getTorrentId() + "is active already")));
        } else if (event instanceof HopsTorrentUploadEvent.Request) {
          HopsTorrentUploadEvent.Request req = (HopsTorrentUploadEvent.Request) event;
          es.getProxy().answer(req, req.failed(Result.logicalFail("torrent:" + is.getTorrentId() + "is active already")));
        } else {
          LOG.warn("state:{} does not handle event:{} and does not register owsa behaviour", state, event);
        }
        return state;
      }
    };
  
  static FSMEventHandler stop0 = new FSMEventHandler<LibTExternal, LibTInternal, HopsTorrentStopEvent.Request>() {
    @Override
    public FSMStateName handle(FSMStateName state, LibTExternal es, LibTInternal is, HopsTorrentStopEvent.Request req) {
      LOG.info("<{}>stop received for torrent:{} - stopping", req.getBaseId(), req.torrentId);
      es.getProxy().answer(req, req.success());
      return FSMBasicStateNames.FINAL;
    }
  };
  
  static FSMEventHandler stop1 = new FSMEventHandler<LibTExternal, LibTInternal, HopsTorrentStopEvent.Request>() {
    @Override
    public FSMStateName handle(FSMStateName state, LibTExternal es, LibTInternal is, HopsTorrentStopEvent.Request req) {
      LOG.info("<{}>stop received for torrent:{} - move to cleaning endpoints", req.getBaseId(), req.torrentId);
      is.reqFailed(es.getProxy());
      is.setStopReq(req);
      cleanStorageEndpoints(es, is, is.endpointRegistration.selfCleaning());
      return LibTStates.CLEAN_STORAGE;
    }
  };

  static FSMEventHandler stop2 = new FSMEventHandler<LibTExternal, LibTInternal, HopsTorrentStopEvent.Request>() {
    @Override
    public FSMStateName handle(FSMStateName state, LibTExternal es, LibTInternal is, HopsTorrentStopEvent.Request req) {
      LOG.info("<{}>stop received for torrent:{} - move to cleaning transfer", req.getBaseId(), req.torrentId);
      is.reqFailed(es.getProxy());
      is.setStopReq(req);
      cleanTransfer(es, is);
      return LibTStates.CLEAN_TRANSFER;
    }
  };

  static FSMEventHandler stop3 = new FSMEventHandler<LibTExternal, LibTInternal, HopsTorrentStopEvent.Request>() {
    @Override
    public FSMStateName handle(FSMStateName state, LibTExternal es, LibTInternal is, HopsTorrentStopEvent.Request req) {
      LOG.info("<{}>stop received for torrent:{} - move to cleaning transfer", req.getBaseId(), is.getTorrentId());
      is.setStopReq(req);
      cleanTransfer(es, is);
      return LibTStates.CLEAN_TRANSFER;
    }
  };
  
  static FSMEventHandler stop4 = new FSMEventHandler<LibTExternal, LibTInternal, HopsTorrentStopEvent.Request>() {
    @Override
    public FSMStateName handle(FSMStateName state, LibTExternal es, LibTInternal is, HopsTorrentStopEvent.Request req) {
       LOG.info("<{}>stop received for torrent:{} - working on cleaning", req.getBaseId(), is.getTorrentId());
       return state;
    }
  };

  static FSMEventHandler initDownload
    = new FSMEventHandler<LibTExternal, LibTInternal, HopsTorrentDownloadEvent.StartRequest>() {
      @Override
      public FSMStateName handle(FSMStateName state, LibTExternal es, LibTInternal is,
        HopsTorrentDownloadEvent.StartRequest req) {
        LOG.info("{}accepting new download:{}", req.getBaseId(), req.torrentName);
        is.setDownloadInit(req);
        MyStream manifestStream = manifestStreamSetup(es, is, req.hdfsEndpoint, req.manifest);
        return downloadInit(es, is, req.torrentId, req.torrentName, req.projectId, manifestStream, req.partners);
      }
    };
  
  static FSMEventHandler initDownloadRestart
    = new FSMEventHandler<LibTExternal, LibTInternal, TorrentRestart.DwldReq>() {
      @Override
      public FSMStateName handle(FSMStateName state, LibTExternal es, LibTInternal is, TorrentRestart.DwldReq req) {
        LOG.info("{}restarting download:{}", req.getBaseId(), req.torrentName);
        is.setDownloadRestartInit(req);
        return downloadInit(es, is, req.torrentId, req.torrentName, req.projectId, req.manifestStream, req.partners);
      }
    };

  private static LibTStates downloadInit(LibTExternal es, LibTInternal is, OverlayId torrentId, String torrentName,
    Integer projectId, MyStream manifestStream, List<KAddress> partners) {
    if (es.library.containsTorrent(torrentId)) {
      throw new RuntimeException("library and fsm do not agree - cannot fix it while running - logic error");
    }
    es.library.prepareDownload(projectId, torrentId, torrentName, partners);
    is.setTorrentBuilder(new TorrentBuilder());
    setupStorageEndpoints(es, is, manifestStream);
    setManifestStream(is, manifestStream);
    if (is.endpointRegistration.isComplete()) {
      is.getTorrentBuilder().setEndpoints(is.endpointRegistration.getSetup());
      setupTransfer(es, is);
      return LibTStates.PREPARE_TRANSFER;
    } else {
      return LibTStates.PREPARE_STORAGE;
    }
  }

  static FSMEventHandler initUpload
    = new FSMEventHandler<LibTExternal, LibTInternal, HopsTorrentUploadEvent.Request>() {
      @Override
      public FSMStateName handle(FSMStateName state, LibTExternal es, LibTInternal is,
        HopsTorrentUploadEvent.Request req) {
        LOG.info("<{}>accepting new upload:{}", req.getBaseId(), req.torrentName);
        is.setUploadInit(req);
        MyStream manifestStream = manifestStreamSetup(es, is, req.hdfsEndpoint, req.manifestResource);
        return initUpload(es, is, req.torrentId, req.torrentName, req.projectId, manifestStream);
      }
    };

  static FSMEventHandler initUploadRestart
    = new FSMEventHandler<LibTExternal, LibTInternal, TorrentRestart.UpldReq>() {
      @Override
      public FSMStateName handle(FSMStateName state, LibTExternal es, LibTInternal is, TorrentRestart.UpldReq req) {
        LOG.info("<{}>accepting new upload:{}", req.getBaseId(), req.torrentName);
        is.setUploadRestartInit(req);
        return initUpload(es, is, req.torrentId, req.torrentName, req.projectId, req.manifestStream);
      }
    };

  private static LibTStates initUpload(LibTExternal es, LibTInternal is, OverlayId torrentId, String torrentName,
    Integer projectId, MyStream manifestStream) {
    if (es.library.containsTorrent(torrentId)) {
      throw new RuntimeException("library and fsm do not agree - cannot fix it while running - logic error");
    }
    es.library.prepareUpload(projectId, torrentId, torrentName);
    is.setTorrentBuilder(new TorrentBuilder());
    setupStorageEndpoints(es, is, manifestStream);
    setManifestStream(is, manifestStream);
    if (is.endpointRegistration.isComplete()) {
      is.getTorrentBuilder().setEndpoints(is.endpointRegistration.getSetup());
      setupTransfer(es, is);
      return LibTStates.PREPARE_TRANSFER;
    } else {
      return LibTStates.PREPARE_STORAGE;
    }
  }
  
  static FSMEventHandler prepareStorage
    = new FSMEventHandler<LibTExternal, LibTInternal, DEndpoint.Success>() {
      @Override
      public FSMStateName handle(FSMStateName state, LibTExternal es, LibTInternal is, DEndpoint.Success resp) {
        LOG.debug("<{}>endpoint:{} prepared", resp.getBaseId(), resp.req.endpointProvider.getName());
        is.endpointRegistration.connected(resp.req.endpointId);
        if (is.endpointRegistration.isComplete()) {
          is.getTorrentBuilder().setEndpoints(is.endpointRegistration.getSetup());
          setupTransfer(es, is);
          return LibTStates.PREPARE_TRANSFER;
        } else {
          return LibTStates.PREPARE_STORAGE;
        }
      }
    };

  static FSMEventHandler prepareTransfer
    = new FSMEventHandler<LibTExternal, LibTInternal, StartTorrent.Response>() {
      @Override
      public FSMStateName handle(FSMStateName state, LibTExternal es, LibTInternal is, StartTorrent.Response resp) {
        if (resp.result.isSuccess()) {
          LOG.debug("<{}>torrent:{} - prepared", resp.getBaseId(), resp.overlayId());
          if (is.isDownload()) {
            getManifest(es, is);
            return LibTStates.DOWNLOAD_MANIFEST;
          } else if (is.isDownloadRestart()) {
            getManifest(es, is);
            return LibTStates.DOWNLOAD_MANIFEST;
          } else {
            //on download restart the manifest is present on endpoint
            //on upload req/restart the manifest is present on endpoint
            readManifest(es, is);
            prepareDetails(es, is);
            is.setTorrent(is.getTorrentBuilder().getTorrent());
            advanceTransfer(es, is);
            return LibTStates.ADVANCE_TRANSFER;
          }
        } else {
          LOG.warn("<{}>torrent:{} - start failed", resp.getBaseId(), resp.overlayId());
          throw new RuntimeException("todo deal with failure");
        }
      }
    };

  static FSMEventHandler downloadManifest
    = new FSMEventHandler<LibTExternal, LibTInternal, GetRawTorrent.Response>() {
      @Override
      public FSMStateName handle(FSMStateName state, LibTExternal es, LibTInternal is, GetRawTorrent.Response resp) {
        if (resp.result.isSuccess()) {
          LOG.debug("<{}>torrent:{} - advanced", resp.getBaseId(), resp.overlayId());
          writeManifest(es, is, resp.result.getValue());
          if (withExtendedDetails(es)) {
            getExtendedDetails(es, is);
            return LibTStates.EXTENDED_DETAILS;
          } else {
            prepareDetails(es, is);
            is.setTorrent(is.getTorrentBuilder().getTorrent());
            advanceTransfer(es, is);
            return LibTStates.ADVANCE_TRANSFER;
          }

        } else {
          LOG.warn("<{}>torrent:{} - start failed", resp.getBaseId(), resp.overlayId());
          throw new RuntimeException("todo deal with failure");
        }
      }
    };

  static FSMEventHandler extendedDetails
    = new FSMEventHandler<LibTExternal, LibTInternal, HopsTorrentDownloadEvent.AdvanceRequest>() {
      @Override
      public FSMStateName handle(FSMStateName state, LibTExternal es, LibTInternal is,
        HopsTorrentDownloadEvent.AdvanceRequest req) {
        if (req.result.isSuccess()) {
          LOG.debug("<{}>torrent:{} - extended details", req.getBaseId(), req.torrentId);
          is.setDownloadAdvance(req);
          prepareDetails(es, is);
          is.setTorrent(is.getTorrentBuilder().getTorrent());
          advanceTransfer(es, is);
          return LibTStates.ADVANCE_TRANSFER;
        } else {
          LOG.warn("<{}>torrent:{} - start failed", req.getBaseId(), req.torrentId);
          throw new RuntimeException("todo deal with failure");
        }
      }
    };

  static FSMEventHandler advanceTransfer
    = new FSMEventHandler<LibTExternal, LibTInternal, SetupTransfer.Response>() {
      @Override
      public FSMStateName handle(FSMStateName state, LibTExternal es, LibTInternal is, SetupTransfer.Response resp) {
        if (resp.result.isSuccess()) {
          LOG.debug("<{}>torrent:{} - transfer - set up", resp.getBaseId(), resp.overlayId());
          if (!es.library.containsTorrent(resp.overlayId())) {
            throw new RuntimeException("mismatch between library and fsm - critical logical error");
          }
          return is.reqSuccess(es.getProxy(), es.library);
        } else {
          LOG.debug("<{}>torrent:{} - transfer - set up failed", resp.getBaseId(), resp.overlayId());
          throw new RuntimeException("todo deal with failure");
        }
      }
    };

  static FSMEventHandler downloadCompleted
    = new FSMEventHandler<LibTExternal, LibTInternal, DownloadSummaryEvent>() {
      @Override
      public FSMStateName handle(FSMStateName state, LibTExternal es, LibTInternal is, DownloadSummaryEvent event) {
        LOG.debug("<{}>torrent:{} - download completed", event.getBaseId(), event.torrentId);
        if (!es.library.containsTorrent(event.torrentId)) {
          throw new RuntimeException("mismatch between library and fsm - critical logical error");
        }
        es.library.finishDownload(event.torrentId);
        return LibTStates.UPLOADING;
      }
    };
  
  static FSMEventHandler endpointCleaning
    = new FSMEventHandler<LibTExternal, LibTInternal, DEndpoint.Disconnected>() {
      @Override
      public FSMStateName handle(FSMStateName state, LibTExternal es, LibTInternal is, DEndpoint.Disconnected resp) {
        LOG.debug("<{}>endpoint:{} cleaned", resp.getBaseId(), resp.req.endpointId);
        is.endpointRegistration.cleaned(resp.req.endpointId);
        if (is.endpointRegistration.cleaningComplete()) {
          HopsTorrentStopEvent.Request stopReq = is.getStopReq();
          es.getProxy().answer(stopReq, stopReq.success());
          es.library.remove(is.getTorrentId());
          return FSMBasicStateNames.FINAL;
        }
        return LibTStates.CLEAN_STORAGE;
      }
    };

  static FSMEventHandler transferCleaning
    = new FSMEventHandler<LibTExternal, LibTInternal, StopTorrent.Response>() {
      @Override
      public FSMStateName handle(FSMStateName state, LibTExternal es, LibTInternal is, StopTorrent.Response resp) {
        LOG.debug("<{}>torrent:{} cleaned", resp.getBaseId(), resp.torrentId);
        if (!resp.result.isSuccess()) {
          throw new RuntimeException("TODO Alex - what do you do when the cleaning operation fails");
        }
        cleanStorageEndpoints(es, is, is.endpointRegistration.selfCleaning());
        return LibTStates.CLEAN_STORAGE;
      }
    };

  private static void setManifestStream(LibTInternal is, MyStream manifestStream) {
    Identifier manifestEndpointId = is.endpointRegistration.nameToId(manifestStream.endpoint.getEndpointName());
    FileId manifestFileId = TorrentIds.fileId(is.getTorrentId(), MyTorrent.MANIFEST_ID);
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
      es.getProxy().trigger(new DEndpoint.Connect(is.getTorrentId(), endpointId, storageProvider), es.endpointPort());
    }
  }

  private static void cleanStorageEndpoints(LibTExternal es, LibTInternal is, Set<Identifier> endpoints) {
    for (Identifier endpointId : endpoints) {
      es.getProxy().trigger(new DEndpoint.Disconnect(is.getTorrentId(), endpointId), es.endpointPort());
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
    GetRawTorrent.Request req = new GetRawTorrent.Request(is.getTorrentId());
    es.getProxy().trigger(req, es.transferCtrlPort());
  }
  
  private static void getExtendedDetails(LibTExternal es, LibTInternal is) {
    HopsTorrentDownloadEvent.StartRequest downloadReq = is.getDownloadReq();
    es.getProxy().answer(downloadReq, downloadReq.success(Result.success(true)));
    is.finishDownloadReq();
  }

  private static void advanceTransfer(LibTExternal es, LibTInternal is) {
    SetupTransfer.Request req = new SetupTransfer.Request(is.getTorrentId(), is.getTorrent());
    es.getProxy().trigger(req, es.transferCtrlPort());
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
}
