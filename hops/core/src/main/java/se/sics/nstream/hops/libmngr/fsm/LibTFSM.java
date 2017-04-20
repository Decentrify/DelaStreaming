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
import se.sics.ktoolbox.nutil.fsm.MultiFSM;
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

  public static MultiFSM build(LibTExternal es, OnFSMExceptionAction oexa) throws FSMException {

    FSMBuilder.Machine machine = FSMBuilder.machine()
      .onState(FSMBasicStateNames.START)
        .nextStates(LibTStates.PREPARE_MANIFEST_STORAGE, LibTStates.PREPARE_TRANSFER)
        .buildTransition()
      .onState(LibTStates.PREPARE_MANIFEST_STORAGE)
        .nextStates(LibTStates.PREPARE_MANIFEST_STORAGE, LibTStates.PREPARE_TRANSFER, LibTStates.CLEAN_STORAGE)
        .nextStates(LibTStates.PREPARE_MANIFEST_STORAGE, LibTStates.PREPARE_TRANSFER)
        .buildTransition()
      .onState(LibTStates.PREPARE_TRANSFER)
        .nextStates(LibTStates.DOWNLOAD_MANIFEST, LibTStates.ADVANCE_TRANSFER, LibTStates.CLEAN_TRANSFER)
        .buildTransition()
      .onState(LibTStates.DOWNLOAD_MANIFEST)
        .nextStates(LibTStates.EXTENDED_DETAILS, LibTStates.ADVANCE_TRANSFER, LibTStates.CLEAN_TRANSFER)
        .buildTransition()
      .onState(LibTStates.EXTENDED_DETAILS)
        .nextStates(LibTStates.PREPARE_FILES_STORAGE, LibTStates.ADVANCE_TRANSFER, LibTStates.CLEAN_TRANSFER)
        .buildTransition()
      .onState(LibTStates.PREPARE_FILES_STORAGE)
        .nextStates(LibTStates.ADVANCE_TRANSFER, LibTStates.CLEAN_TRANSFER)
        .buildTransition()
      .onState(LibTStates.ADVANCE_TRANSFER)
        .nextStates(LibTStates.DOWNLOADING, LibTStates.UPLOADING, LibTStates.CLEAN_TRANSFER)
        .buildTransition()
      .onState(LibTStates.DOWNLOADING)
        .nextStates(LibTStates.UPLOADING, LibTStates.CLEAN_TRANSFER)
        .buildTransition()
      .onState(LibTStates.UPLOADING)
        .nextStates(LibTStates.CLEAN_TRANSFER)
        .buildTransition()
      .onState(LibTStates.CLEAN_TRANSFER)
        .nextStates(LibTStates.CLEAN_STORAGE)
        .buildTransition()
       .onState(LibTStates.CLEAN_STORAGE)
        .nextStates(LibTStates.CLEAN_STORAGE)
        .toFinal()
      .buildTransition();

    FSMBuilder.Handlers handlers = FSMBuilder.events()
      .negativePort(TorrentRestartPort.class)
        .onEvent(TorrentRestart.DwldReq.class)
          .subscribe(LibTHandlers.initDownloadRestart, FSMBasicStateNames.START)
        .onEvent(TorrentRestart.UpldReq.class)
          .subscribe(LibTHandlers.initUploadRestart, FSMBasicStateNames.START)
        .buildEvents()
      .negativePort(HopsTorrentPort.class)
        .onEvent(HopsTorrentStopEvent.Request.class)
          .subscribe(LibTHandlers.stop0, FSMBasicStateNames.START)
          .subscribe(LibTHandlers.stop1, LibTStates.PREPARE_MANIFEST_STORAGE)
          .subscribe(LibTHandlers.stop2, LibTStates.PREPARE_TRANSFER, LibTStates.DOWNLOAD_MANIFEST, LibTStates.EXTENDED_DETAILS, LibTStates.ADVANCE_TRANSFER)
          .subscribe(LibTHandlers.stop3, LibTStates.DOWNLOADING, LibTStates.UPLOADING)
          .subscribe(LibTHandlers.stop4, LibTStates.CLEAN_TRANSFER, LibTStates.CLEAN_STORAGE)
        .onEvent(HopsTorrentUploadEvent.Request.class)
          .subscribe(LibTHandlers.initUpload, FSMBasicStateNames.START)
          .fallback(LibTHandlers.fallbackUploadStart)
         .onEvent(HopsTorrentDownloadEvent.StartRequest.class)
          .subscribe(LibTHandlers.initDownload, FSMBasicStateNames.START)
          .fallback(LibTHandlers.fallbackUploadStart)
        .onEvent(HopsTorrentDownloadEvent.AdvanceRequest.class)
          .subscribe(LibTHandlers.extendedDetails, LibTStates.EXTENDED_DETAILS)
        .buildEvents()
      .positivePort(TransferCtrlPort.class)
        .onEvent(GetRawTorrent.Response.class)
          .subscribe(LibTHandlers.downloadManifest, LibTStates.DOWNLOAD_MANIFEST)
        .onEvent(SetupTransfer.Response.class)
          .subscribe(LibTHandlers.advanceTransfer, LibTStates.ADVANCE_TRANSFER)
        .buildEvents()
      .positivePort(TorrentStatusPort.class)
        .onEvent(DownloadSummaryEvent.class)
          .subscribe(LibTHandlers.downloadCompleted, LibTStates.DOWNLOADING)
        .buildEvents()
      .positivePort(TorrentMngrPort.class)
        .onEvent(StartTorrent.Response.class)
          .subscribe(LibTHandlers.prepareTransfer, LibTStates.PREPARE_TRANSFER)
        .onEvent(StopTorrent.Response.class)
          .subscribe(LibTHandlers.transferCleaning, LibTStates.CLEAN_TRANSFER)
        .buildEvents()
      .positivePort(DEndpointCtrlPort.class)
        .onEvent(DEndpoint.Success.class)
          .subscribe(LibTHandlers.prepareManifestStorage, LibTStates.PREPARE_MANIFEST_STORAGE)
        .onEvent(DEndpoint.Disconnected.class)
          .subscribe(LibTHandlers.endpointCleaning, LibTStates.CLEAN_STORAGE)
        .buildEvents();

    FSMInternalStateBuilder isb = new LibTInternal.Builder();
    MultiFSM fsm = FSMBuilder.multiFSM(NAME, machine, handlers, es, isb, oexa);
    return fsm;
  }
}