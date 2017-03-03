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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.ktoolbox.nutil.fsm.FSMBuilder;
import se.sics.ktoolbox.nutil.fsm.FSMachineDef;
import se.sics.ktoolbox.nutil.fsm.MultiFSM;
import se.sics.ktoolbox.nutil.fsm.MultiFSMBuilder;
import se.sics.ktoolbox.nutil.fsm.api.FSMBasicStateNames;
import se.sics.ktoolbox.nutil.fsm.api.FSMException;
import se.sics.ktoolbox.nutil.fsm.api.FSMInternalStateBuilder;
import se.sics.ktoolbox.nutil.fsm.genericsetup.OnFSMExceptionAction;
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
      FSMachineDef torrentFSM = build();
      FSMInternalStateBuilder isBuilder = new LibTInternal.Builder();
      MultiFSM fsm = MultiFSMBuilder.instance()
        .events()
          .positivePort(DEndpointCtrlPort.class)
            .event(DEndpoint.Success.class)
            .event(DEndpoint.Failed.class)
            .event(DEndpoint.Disconnected.class)
            .buildPort()
          .positivePort(TorrentMngrPort.class)
            .event(StartTorrent.Response.class)
            .event(StopTorrent.Response.class)
            .buildPort()
          .positivePort(TransferCtrlPort.class)
            .event(SetupTransfer.Response.class)
            .event(GetRawTorrent.Response.class)
            .buildPort()
          .positivePort(TorrentStatusPort.class)
            .event(DownloadSummaryEvent.class)
            .buildPort()
          .negativePort(HopsTorrentPort.class)
            .event(HopsTorrentDownloadEvent.StartRequest.class)
            .event(HopsTorrentDownloadEvent.AdvanceRequest.class)
            .event(HopsTorrentUploadEvent.Request.class)
            .event(HopsTorrentStopEvent.Request.class)
            .buildPort()
          .negativePort(TorrentRestartPort.class)
            .event(TorrentRestart.DwldReq.class)
            .event(TorrentRestart.UpldReq.class)
            .buildPort()
          .buildEvents()
        .buildMultiFSM(torrentFSM, oexa, es, isBuilder);
      return fsm;
    } catch (FSMException ex) {
      throw new RuntimeException(ex);
    }
  }

  public static FSMachineDef build() throws FSMException {

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
        .onEvent(TorrentRestart.DwldReq.class)
          .inState(FSMBasicStateNames.START, LibTHandlers.initDownloadRestart)
        .onEvent(TorrentRestart.UpldReq.class)
          .inState(FSMBasicStateNames.START, LibTHandlers.initUploadRestart)
        .onEvent(HopsTorrentStopEvent.Request.class)
          .inState(FSMBasicStateNames.START, LibTHandlers.stop0)
          .inState(LibTStates.PREPARE_STORAGE, LibTHandlers.stop1)
          .inState(LibTStates.PREPARE_TRANSFER, LibTHandlers.stop2)
          .inState(LibTStates.DOWNLOAD_MANIFEST, LibTHandlers.stop2)
          .inState(LibTStates.EXTENDED_DETAILS, LibTHandlers.stop2)
          .inState(LibTStates.ADVANCE_TRANSFER, LibTHandlers.stop2)
          .inState(LibTStates.DOWNLOADING, LibTHandlers.stop3)
          .inState(LibTStates.UPLOADING, LibTHandlers.stop3)
          .inState(LibTStates.CLEAN_TRANSFER, LibTHandlers.stop4)
          .inState(LibTStates.CLEAN_STORAGE, LibTHandlers.stop4)
        .onEvent(HopsTorrentDownloadEvent.StartRequest.class)
          .inState(FSMBasicStateNames.START, LibTHandlers.initDownload)
        .onEvent(HopsTorrentUploadEvent.Request.class)
          .inState(FSMBasicStateNames.START, LibTHandlers.initUpload)
        .onEvent(HopsTorrentDownloadEvent.AdvanceRequest.class)
          .inState(LibTStates.EXTENDED_DETAILS, LibTHandlers.extendedDetails)
        .onEvent(GetRawTorrent.Response.class)
          .inState(LibTStates.DOWNLOAD_MANIFEST, LibTHandlers.downloadManifest)
        .onEvent(SetupTransfer.Response.class)
          .inState(LibTStates.ADVANCE_TRANSFER, LibTHandlers.advanceTransfer)
        .onEvent(DownloadSummaryEvent.class)
          .inState(LibTStates.DOWNLOADING, LibTHandlers.downloadCompleted)
        .onEvent(StartTorrent.Response.class)
          .inState(LibTStates.PREPARE_TRANSFER, LibTHandlers.prepareTransfer)
        .onEvent(StopTorrent.Response.class)
          .inState(LibTStates.CLEAN_TRANSFER, LibTHandlers.transferCleaning)
        .onEvent(DEndpoint.Success.class)
          .inState(LibTStates.PREPARE_STORAGE, LibTHandlers.prepareStorage)
        .onEvent(DEndpoint.Disconnected.class)
          .inState(LibTStates.CLEAN_STORAGE, LibTHandlers.endpointCleaning)
        .buildEvent()
      .buildEvents()
      .fallback(LibTHandlers.fallbackHandler)
      .buildFallbacks();

    FSMachineDef fsm = machine.complete(NAME, handlers);
    return fsm;
  }
}