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
import org.javatuples.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.ktoolbox.nutil.fsm.FSMBuilder;
import se.sics.ktoolbox.nutil.fsm.FSMachineDef;
import se.sics.ktoolbox.nutil.fsm.MultiFSM;
import se.sics.ktoolbox.nutil.fsm.api.FSMBasicStateNames;
import se.sics.ktoolbox.nutil.fsm.api.FSMEvent;
import se.sics.ktoolbox.nutil.fsm.api.FSMException;
import se.sics.ktoolbox.nutil.fsm.api.FSMIdExtractor;
import se.sics.ktoolbox.nutil.fsm.api.FSMInternalStateBuilders;
import se.sics.ktoolbox.nutil.fsm.api.FSMStateName;
import se.sics.ktoolbox.nutil.fsm.genericsetup.OnFSMExceptionAction;
import se.sics.ktoolbox.nutil.fsm.handler.FSMEventHandler;
import se.sics.ktoolbox.nutil.fsm.ids.FSMDefId;
import se.sics.ktoolbox.nutil.fsm.ids.FSMId;
import se.sics.ktoolbox.util.result.Result;
import se.sics.nstream.hops.library.HopsTorrentPort;
import se.sics.nstream.hops.library.event.core.HopsTorrentDownloadEvent;
import se.sics.nstream.hops.library.event.core.HopsTorrentStopEvent;
import se.sics.nstream.hops.library.event.core.HopsTorrentUploadEvent;
import se.sics.nstream.library.restart.TorrentRestart;
import se.sics.nstream.library.restart.TorrentRestartPort;
import se.sics.nstream.storage.durable.DEndpointCtrlPort;
import se.sics.nstream.storage.durable.events.DEndpoint;
import se.sics.nstream.torrent.TorrentMngrPort;
import se.sics.nstream.torrent.event.StartTorrent;
import se.sics.nstream.torrent.event.StopTorrent;
import se.sics.nstream.torrent.status.event.DownloadSummaryEvent;
import se.sics.nstream.torrent.tracking.TorrentStatusPort;
import se.sics.nstream.torrent.transfer.TransferCtrlPort;
import se.sics.nstream.torrent.transfer.event.ctrl.GetRawTorrent;
import se.sics.nstream.torrent.transfer.event.ctrl.SetupTransfer;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class LibTFSM {

  private static final Logger LOG = LoggerFactory.getLogger(LibTFSM.class);
  public static final String NAME = "dela-torrent-library-fsm";

  public static MultiFSM getFSM(LibTExternal es, OnFSMExceptionAction oexa) {
    try {
      Map<FSMDefId, FSMachineDef> fsmds = new HashMap<>();
      final FSMachineDef torrentFSM = build();
      fsmds.put(torrentFSM.id, torrentFSM);

      FSMInternalStateBuilders builders = new FSMInternalStateBuilders();
      builders.register(torrentFSM.id, new LibTInternal.Builder());

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
    FSMEventHandler fallbackHandler = new FSMEventHandler<LibTExternal, LibTInternal, FSMEvent>() {
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

    FSMBuilder.Machine machine = FSMBuilder.machine()
      .onState(FSMBasicStateNames.START)
      .nextStates(LibTStates.PREPARE_STORAGE, LibTStates.PREPARE_TRANSFER)
      .toFinal()
      .buildTransition()
      .onState(LibTStates.PREPARE_STORAGE)
      .nextStates(LibTStates.PREPARE_STORAGE, LibTStates.PREPARE_TRANSFER)
      .cleanup(LibTStates.CLEAN_STORAGE)
      .buildTransition()
      .onState(LibTStates.PREPARE_TRANSFER)
      .nextStates(LibTStates.DOWNLOAD_MANIFEST, LibTStates.ADVANCE_TRANSFER)
      .cleanup(LibTStates.CLEAN_TRANSFER)
      .buildTransition()
      .onState(LibTStates.DOWNLOAD_MANIFEST)
      .nextStates(LibTStates.EXTENDED_DETAILS, LibTStates.ADVANCE_TRANSFER)
      .cleanup(LibTStates.CLEAN_TRANSFER)
      .buildTransition()
      .onState(LibTStates.EXTENDED_DETAILS)
      .nextStates(LibTStates.ADVANCE_TRANSFER)
      .cleanup(LibTStates.CLEAN_TRANSFER)
      .buildTransition()
      .onState(LibTStates.ADVANCE_TRANSFER)
      .nextStates(LibTStates.DOWNLOADING, LibTStates.UPLOADING)
      .cleanup(LibTStates.CLEAN_TRANSFER)
      .buildTransition()
      .onState(LibTStates.DOWNLOADING)
      .nextStates(LibTStates.UPLOADING)
      .cleanup(LibTStates.CLEAN_TRANSFER)
      .buildTransition()
      .onState(LibTStates.UPLOADING)
      .cleanup(LibTStates.CLEAN_TRANSFER)
      .buildTransition()
      .onState(LibTStates.CLEAN_TRANSFER)
      .cleanup(LibTStates.CLEAN_STORAGE)
      .buildTransition()
      .onState(LibTStates.CLEAN_STORAGE)
      .nextStates(LibTStates.CLEAN_STORAGE)
      .toFinal()
      .buildTransition();

    FSMBuilder.Handlers handlers = FSMBuilder.handlers()
      .events()
        .onEvent(HopsTorrentStopEvent.Request.class)
          .inState(FSMBasicStateNames.START, LibTHandlers.stop0)
          .inState(LibTStates.PREPARE_STORAGE, LibTHandlers.stop1)
          .inState(LibTStates.PREPARE_TRANSFER, LibTHandlers.stop2)
          .inState(LibTStates.DOWNLOAD_MANIFEST, LibTHandlers.stop2)
          .inState(LibTStates.EXTENDED_DETAILS, LibTHandlers.stop2)
          .inState(LibTStates.ADVANCE_TRANSFER, LibTHandlers.stop2)
          .inState(LibTStates.DOWNLOADING, LibTHandlers.stop3)
          .inState(LibTStates.UPLOADING, LibTHandlers.stop3)
          .inState(LibTStates.CLEAN_TRANSFER, null)
          .inState(LibTStates.CLEAN_STORAGE, null)
        .onEvent(HopsTorrentDownloadEvent.StartRequest.class)
          .inState(FSMBasicStateNames.START, LibTHandlers.initDownload)
        .onEvent(TorrentRestart.DwldReq.class)
          .inState(FSMBasicStateNames.START, LibTHandlers.initDownloadRestart)
        .onEvent(HopsTorrentUploadEvent.Request.class)
          .inState(FSMBasicStateNames.START, LibTHandlers.initUpload)
        .onEvent(TorrentRestart.UpldReq.class)
          .inState(FSMBasicStateNames.START, LibTHandlers.initUploadRestart)
        .onEvent(DEndpoint.Success.class)
          .inState(LibTStates.PREPARE_STORAGE, LibTHandlers.prepareStorage)
        .onEvent(StartTorrent.Response.class)
          .inState(LibTStates.PREPARE_TRANSFER, LibTHandlers.prepareTransfer)
        .onEvent(GetRawTorrent.Response.class)
          .inState(LibTStates.DOWNLOAD_MANIFEST, LibTHandlers.downloadManifest)
        .onEvent(HopsTorrentDownloadEvent.AdvanceRequest.class)
          .inState(LibTStates.EXTENDED_DETAILS, LibTHandlers.extendedDetails)
        .onEvent(SetupTransfer.Response.class)
          .inState(LibTStates.ADVANCE_TRANSFER, LibTHandlers.advanceTransfer)
        .onEvent(DownloadSummaryEvent.class)
          .inState(LibTStates.DOWNLOADING, LibTHandlers.downloadCompleted)
        .onEvent(StopTorrent.Response.class)
          .inState(LibTStates.CLEAN_TRANSFER, LibTHandlers.transferCleaning)
        .onEvent(DEndpoint.Disconnected.class)
          .inState(LibTStates.CLEAN_STORAGE, LibTHandlers.endpointCleaning)
      .buildEvents()
      .fallback(fallbackHandler)
      .buildFallbacks();

    FSMachineDef fsm = machine.complete(NAME, handlers);
    return fsm;
  }
}