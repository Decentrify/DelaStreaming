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
package se.sics.silkold.torrentmngr;

import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.kompics.KompicsEvent;
import se.sics.kompics.LoopbackPort;
import se.sics.kompics.fsm.BaseIdExtractor;
import se.sics.kompics.fsm.FSMBuilder;
import se.sics.kompics.fsm.FSMException;
import se.sics.kompics.fsm.FSMInternalStateBuilder;
import se.sics.kompics.fsm.MultiFSM;
import se.sics.kompics.fsm.OnFSMExceptionAction;
import se.sics.kompics.fsm.id.FSMIdentifierFactory;
import se.sics.kompics.util.Identifier;
import se.sics.nstream.torrent.status.event.TorrentReady;
import se.sics.nstream.torrent.tracking.TorrentStatusPort;
import se.sics.silkold.torrent.TorrentMngrPort;
import se.sics.silkold.torrentmngr.event.StartTorrent;
import se.sics.silkold.torrentmngr.event.StopTorrent;
import se.sics.silkold.torrentmngr.event.StoppedTorrentComp;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class TorrentMngrFSM {

  private static final Logger LOG = LoggerFactory.getLogger(TorrentMngrFSM.class);
  public static final String NAME = "dela-torrent-mngr-fsm";

  private static FSMBuilder.StructuralDefinition structuralDef() throws FSMException {
    return FSMBuilder.structuralDef()
      .onStart()
        .nextStates(TorrentMngrStates.PREPARE_COMP)
      .buildTransition()
      .onState(TorrentMngrStates.PREPARE_COMP)
        .nextStates(TorrentMngrStates.READY, TorrentMngrStates.STOPPING)
      .buildTransition()
      .onState(TorrentMngrStates.READY)
        .nextStates(TorrentMngrStates.STOPPING)
      .buildTransition()
      .onState(TorrentMngrStates.STOPPING)
        .toFinal()
      .buildTransition();
  }

  private static FSMBuilder.SemanticDefinition semanticDef() throws FSMException {
    return FSMBuilder.semanticDef()
      .negativePort(TorrentMngrPort.class)
        .basicEvent(StartTorrent.Request.class)
          .subscribeOnStart(TorrentMngrHandlers.startTorrent)
        .basicEvent(StopTorrent.Request.class)
          .subscribe(TorrentMngrHandlers.stopTorrent, TorrentMngrStates.PREPARE_COMP, TorrentMngrStates.READY)
        .buildEvents()
      .positivePort(TorrentStatusPort.class)
        .basicEvent(TorrentReady.class)
          .subscribe(TorrentMngrHandlers.torrentReady, TorrentMngrStates.PREPARE_COMP)
        .buildEvents()
      .negativePort(LoopbackPort.class)
        .basicEvent(StoppedTorrentComp.class)
          .subscribe(TorrentMngrHandlers.stoppedTorrentComp, TorrentMngrStates.STOPPING)
        .buildEvents();
  }
  
  static BaseIdExtractor baseIdExtractor = new BaseIdExtractor() {

    @Override
    public Optional<Identifier> fromEvent(KompicsEvent event) throws FSMException {
      if (event instanceof TorrentMngrFSMEvent) {
        return Optional.of(((TorrentMngrFSMEvent) event).getTorrentMngrFSMId());
      }
      return Optional.empty();
    }
  };

  public static MultiFSM multifsm(FSMIdentifierFactory fsmIdFactory, TorrentMngrExternal es, OnFSMExceptionAction oexa) throws FSMException {
    FSMInternalStateBuilder isb = new TorrentMngrInternal.Builder();
    return FSMBuilder.multiFSM(fsmIdFactory, NAME, structuralDef(), semanticDef(), es, isb, oexa, baseIdExtractor);
  }
}
