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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.kompics.fsm.BaseIdExtractor;
import se.sics.kompics.fsm.FSMBuilder;
import se.sics.kompics.fsm.FSMEvent;
import se.sics.kompics.fsm.FSMException;
import se.sics.kompics.fsm.FSMInternalStateBuilder;
import se.sics.kompics.fsm.MultiFSM;
import se.sics.kompics.fsm.OnFSMExceptionAction;
import se.sics.kompics.fsm.id.FSMIdentifierFactory;
import se.sics.kompics.util.Identifier;
import se.sics.nstream.hops.library.HopsTorrentPort;
import se.sics.nstream.hops.library.event.core.HopsTorrentDownloadEvent;
import se.sics.nstream.hops.library.event.core.HopsTorrentStopEvent;
import se.sics.nstream.hops.library.event.core.HopsTorrentUploadEvent;
import se.sics.nstream.library.event.torrent.TorrentExtendedStatusEvent;
import se.sics.nstream.library.restart.LibTFSMEvent;
import se.sics.nstream.library.restart.TorrentRestart;
import se.sics.nstream.library.restart.TorrentRestartPort;
import se.sics.nstream.storage.durable.DEndpointCtrlPort;
import se.sics.nstream.storage.durable.events.DEndpoint;
import se.sics.nstream.torrent.TorrentMngrPort;
import se.sics.nstream.torrent.event.StartTorrent;
import se.sics.nstream.torrent.event.StopTorrent;
import se.sics.nstream.torrent.status.event.DownloadSummaryEvent;
import se.sics.nstream.torrent.tracking.TorrentStatusPort;
import se.sics.nstream.torrent.tracking.event.StatusSummaryEvent;
import se.sics.nstream.torrent.transfer.TransferCtrlPort;
import se.sics.nstream.torrent.transfer.event.ctrl.GetRawTorrent;
import se.sics.nstream.torrent.transfer.event.ctrl.SetupTransfer;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class LibTFSM {

  private static final Logger LOG = LoggerFactory.getLogger(LibTFSM.class);
  public static final String NAME = "dela-torrent-library-fsm";

  private static FSMBuilder.StructuralDefinition structuralDef() throws FSMException {

    return FSMBuilder.structuralDef()
      .onStart()
      .nextStates(LibTStates.PREPARE_MANIFEST_STORAGE, LibTStates.PREPARE_TRANSFER)
      .buildTransition()
      .onState(LibTStates.PREPARE_MANIFEST_STORAGE)
      .nextStates(LibTStates.PREPARE_MANIFEST_STORAGE, LibTStates.PREPARE_TRANSFER, LibTStates.CLEAN_STORAGE)
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
      .nextStates(LibTStates.PREPARE_FILES_STORAGE, LibTStates.ADVANCE_TRANSFER, LibTStates.CLEAN_TRANSFER)
      .buildTransition()
      .onState(LibTStates.ADVANCE_TRANSFER)
      .nextStates(LibTStates.DOWNLOADING, LibTStates.UPLOADING, LibTStates.CLEAN_TRANSFER)
      .buildTransition()
      .onState(LibTStates.DOWNLOADING)
      .nextStates(LibTStates.DOWNLOADING, LibTStates.UPLOADING, LibTStates.CLEAN_TRANSFER)
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
  }

  private static FSMBuilder.SemanticDefinition semanticDef() throws FSMException {
    return FSMBuilder.semanticDef()
      .negativePort(TorrentRestartPort.class)
      .onBasicEvent(TorrentRestart.DwldReq.class)
      .subscribeOnStart(LibTHandlers.initDownloadRestart)
      .onBasicEvent(TorrentRestart.UpldReq.class)
      .subscribeOnStart(LibTHandlers.initUploadRestart)
      .buildEvents()
      .negativePort(HopsTorrentPort.class)
      .onBasicEvent(TorrentExtendedStatusEvent.Request.class)
      .subscribe(LibTHandlers.status, LibTStates.DOWNLOADING)
      .onBasicEvent(HopsTorrentStopEvent.Request.class)
      .subscribeOnStart(LibTHandlers.stop0)
      .subscribe(LibTHandlers.stop1, LibTStates.PREPARE_MANIFEST_STORAGE)
      .subscribe(LibTHandlers.stop2, LibTStates.PREPARE_TRANSFER, LibTStates.DOWNLOAD_MANIFEST,
        LibTStates.EXTENDED_DETAILS, LibTStates.ADVANCE_TRANSFER)
      .subscribe(LibTHandlers.stop3, LibTStates.DOWNLOADING, LibTStates.UPLOADING)
      .subscribe(LibTHandlers.stop4, LibTStates.CLEAN_TRANSFER, LibTStates.CLEAN_STORAGE)
      .onBasicEvent(HopsTorrentUploadEvent.Request.class)
      .subscribeOnStart(LibTHandlers.initUpload)
      .fallback(LibTHandlers.fallbackUploadStart)
      .onBasicEvent(HopsTorrentDownloadEvent.StartRequest.class)
      .subscribeOnStart(LibTHandlers.initDownload)
      .fallback(LibTHandlers.fallbackUploadStart)
      .onBasicEvent(HopsTorrentDownloadEvent.AdvanceRequest.class)
      .subscribe(LibTHandlers.extendedDetails, LibTStates.EXTENDED_DETAILS)
      .buildEvents()
      .positivePort(TransferCtrlPort.class)
      .onBasicEvent(GetRawTorrent.Response.class)
      .subscribe(LibTHandlers.downloadManifest, LibTStates.DOWNLOAD_MANIFEST)
      .onBasicEvent(SetupTransfer.Response.class)
      .subscribe(LibTHandlers.advanceTransfer, LibTStates.ADVANCE_TRANSFER)
      .buildEvents()
      .positivePort(TorrentStatusPort.class)
      .onBasicEvent(DownloadSummaryEvent.class)
      .subscribe(LibTHandlers.downloadCompleted, LibTStates.DOWNLOADING)
      .onBasicEvent(StatusSummaryEvent.Response.class)
      .subscribe(LibTHandlers.statusReport, LibTStates.DOWNLOADING)
      .buildEvents()
      .positivePort(TorrentMngrPort.class)
      .onBasicEvent(StartTorrent.Response.class)
      .subscribe(LibTHandlers.prepareTransfer, LibTStates.PREPARE_TRANSFER)
      .onBasicEvent(StopTorrent.Response.class)
      .subscribe(LibTHandlers.transferCleaning, LibTStates.CLEAN_TRANSFER)
      .buildEvents()
      .positivePort(DEndpointCtrlPort.class)
      .onBasicEvent(DEndpoint.Success.class)
      .subscribe(LibTHandlers.prepareManifestStorage, LibTStates.PREPARE_MANIFEST_STORAGE)
      .subscribe(LibTHandlers.prepareFilesStorage, LibTStates.PREPARE_FILES_STORAGE)
      .onBasicEvent(DEndpoint.Disconnected.class)
      .subscribe(LibTHandlers.endpointCleaning, LibTStates.CLEAN_STORAGE)
      .buildEvents();
  }
  static BaseIdExtractor baseIdExtractor = new BaseIdExtractor() {

    @Override
    public Optional<Identifier> fromEvent(FSMEvent event) throws FSMException {
      if (event instanceof LibTFSMEvent) {
        return Optional.of(((LibTFSMEvent) event).getLibTFSMId());
      }
      return Optional.absent();
    }
  };

  public static MultiFSM multifsm(FSMIdentifierFactory fsmIdFactory, LibTExternal es, OnFSMExceptionAction oexa) throws FSMException {
    FSMInternalStateBuilder isb = new LibTInternal.Builder();
    return FSMBuilder.multiFSM(fsmIdFactory, NAME, structuralDef(), semanticDef(), es, isb, oexa, baseIdExtractor);
  }

}
