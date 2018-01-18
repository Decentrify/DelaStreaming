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
package se.sics.silk.r2torrent.torrent;

import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.kompics.ComponentProxy;
import se.sics.kompics.KompicsEvent;
import se.sics.kompics.fsm.BaseIdExtractor;
import se.sics.kompics.fsm.FSMBasicStateNames;
import se.sics.kompics.fsm.FSMBuilder;
import se.sics.kompics.fsm.FSMEvent;
import se.sics.kompics.fsm.FSMException;
import se.sics.kompics.fsm.FSMExternalState;
import se.sics.kompics.fsm.FSMInternalState;
import se.sics.kompics.fsm.FSMInternalStateBuilder;
import se.sics.kompics.fsm.FSMStateName;
import se.sics.kompics.fsm.MultiFSM;
import se.sics.kompics.fsm.OnFSMExceptionAction;
import se.sics.kompics.fsm.handler.FSMBasicEventHandler;
import se.sics.kompics.fsm.id.FSMIdentifier;
import se.sics.kompics.fsm.id.FSMIdentifierFactory;
import se.sics.kompics.util.Identifier;
import se.sics.ktoolbox.util.identifiable.overlay.OverlayId;
import se.sics.silk.DefaultHandlers;
import se.sics.silk.event.SilkEvent;
import se.sics.silk.r2torrent.R2TorrentComp;
import se.sics.silk.r2torrent.R2TorrentPort;
import se.sics.silk.r2torrent.torrent.event.R1HashEvents;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class R1Hash {

  private static final Logger LOG = LoggerFactory.getLogger(FSM.class);
  public static final String NAME = "dela-r1-torrent-hash-fsm";

  public static enum States implements FSMStateName {

    HASH
  }

  public static interface Event extends FSMEvent, SilkEvent.TorrentEvent {
  }
  
  public static interface TorrentEvent extends Event {
  }
  
  public static Identifier fsmBaseId(OverlayId torrentId) {
    return torrentId;
  }

  public static class IS implements FSMInternalState {

    private final FSMIdentifier fsmId;

    public IS(FSMIdentifier fsmId) {
      this.fsmId = fsmId;
    }

    @Override
    public FSMIdentifier getFSMId() {
      return fsmId;
    }
  }

  public static class ISBuilder implements FSMInternalStateBuilder {

    @Override
    public FSMInternalState newState(FSMIdentifier fsmId) {
      return new IS(fsmId);
    }
  }

  public static class ES implements FSMExternalState {

    private R2TorrentComp.Ports ports;
    private ComponentProxy proxy;

    public ES(R2TorrentComp.Ports ports) {
      this.ports = ports;
    }
    
    @Override
    public void setProxy(ComponentProxy proxy) {
      this.proxy = proxy;
    }

    @Override
    public ComponentProxy getProxy() {
      return proxy;
    }
  }

  public static class FSM {

    private static FSMBuilder.StructuralDefinition structuralDef() throws FSMException {
      return FSMBuilder.structuralDef()
        .onStart()
        .nextStates(States.HASH)
        .toFinal()
        .buildTransition()
        .onState(States.HASH)
        .toFinal()
        .buildTransition();
    }

    private static FSMBuilder.SemanticDefinition semanticDef() throws FSMException {
      return FSMBuilder.semanticDef()
        .defaultFallback(DefaultHandlers.basicDefault(), DefaultHandlers.patternDefault())
        .positivePort(R2TorrentPort.class)
        .basicEvent(R1HashEvents.HashReq.class)
        .subscribeOnStart(Handlers.hash)
        .basicEvent(R1HashEvents.HashStop.class)
        .subscribeOnStart(Handlers.stop1)
        .subscribe(Handlers.stop2, States.HASH)
        .buildEvents();
    }

    static BaseIdExtractor baseIdExtractor = new BaseIdExtractor() {

      @Override
      public Optional<Identifier> fromEvent(KompicsEvent event) throws FSMException {
        if (event instanceof Event) {
          Event e = (Event)event;
          return Optional.of(fsmBaseId(e.torrentId()));
        }
        return Optional.empty();
      }
    };

    public static MultiFSM multifsm(FSMIdentifierFactory fsmIdFactory, ES es, OnFSMExceptionAction oexa)
      throws FSMException {
      FSMInternalStateBuilder isb = new ISBuilder();
      return FSMBuilder.multiFSM(fsmIdFactory, NAME, structuralDef(), semanticDef(), es, isb, oexa, baseIdExtractor);
    }
  }
  
  public static class Handlers {
    static FSMBasicEventHandler hash = new FSMBasicEventHandler<ES, IS, R1HashEvents.HashReq>() {
      @Override
      public FSMStateName handle(FSMStateName state, ES es, IS is, R1HashEvents.HashReq req) {
        sendTorrent(es, req.success());
        return FSMBasicStateNames.FINAL;
      }
    };
    
    static FSMBasicEventHandler stop1 = new FSMBasicEventHandler<ES, IS, R1HashEvents.HashStop>() {
      @Override
      public FSMStateName handle(FSMStateName state, ES es, IS is, R1HashEvents.HashStop req) {
        sendTorrent(es, req.ack());
        return FSMBasicStateNames.FINAL;
      }
    };
    
    static FSMBasicEventHandler stop2 = new FSMBasicEventHandler<ES, IS, R1HashEvents.HashStop>() {
      @Override
      public FSMStateName handle(FSMStateName state, ES es, IS is, R1HashEvents.HashStop req) {
        sendTorrent(es, req.ack());
        return FSMBasicStateNames.FINAL;
      }
    };

    private static void sendTorrent(ES es, R2Torrent.HashEvent e) {
      es.getProxy().trigger(e, es.ports.loopbackSend);
    }
  }
}
