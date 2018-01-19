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
import java.util.Random;
import org.javatuples.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.kompics.ComponentProxy;
import se.sics.kompics.KompicsEvent;
import se.sics.kompics.fsm.BaseIdExtractor;
import se.sics.kompics.fsm.FSMBasicStateNames;
import se.sics.kompics.fsm.FSMBuilder;
import se.sics.kompics.fsm.FSMEvent;
import se.sics.kompics.fsm.FSMException;
import se.sics.kompics.fsm.FSMInternalState;
import se.sics.kompics.fsm.FSMInternalStateBuilder;
import se.sics.kompics.fsm.FSMStateName;
import se.sics.kompics.fsm.MultiFSM;
import se.sics.kompics.fsm.OnFSMExceptionAction;
import se.sics.kompics.fsm.handler.FSMBasicEventHandler;
import se.sics.kompics.fsm.id.FSMIdentifier;
import se.sics.kompics.fsm.id.FSMIdentifierFactory;
import se.sics.kompics.util.Identifiable;
import se.sics.kompics.util.Identifier;
import se.sics.ktoolbox.util.identifiable.BasicBuilders;
import se.sics.ktoolbox.util.identifiable.basic.IntIdFactory;
import se.sics.ktoolbox.util.identifiable.basic.PairIdentifier;
import se.sics.ktoolbox.util.identifiable.overlay.OverlayId;
import se.sics.nstream.storage.durable.DStreamControlPort;
import se.sics.nstream.storage.durable.events.DStreamConnect;
import se.sics.nstream.storage.durable.events.DStreamDisconnect;
import se.sics.nstream.storage.durable.events.DStreamEvent;
import se.sics.silk.DefaultHandlers;
import se.sics.silk.event.SilkEvent;
import se.sics.silk.r2torrent.R2TorrentComp;
import se.sics.silk.r2torrent.R2TorrentES;
import se.sics.silk.r2torrent.R2TorrentPort;
import static se.sics.silk.r2torrent.torrent.R2Torrent.HardCodedConfig.seed;
import se.sics.silk.r2torrent.torrent.event.R2StreamMngrEvents;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class R2StreamMngr {

  private static final Logger LOG = LoggerFactory.getLogger(FSM.class);
  public static final String NAME = "dela-r2-stream-mngr-fsm";

  public static enum States implements FSMStateName {

    OPEN,
    ACTIVE,
    CLOSE
  }

  public static interface Event extends FSMEvent, Identifiable {
  }

  public static interface StreamEvent extends Event {
  }

  public static interface R1StreamEvent extends Event, R2StreamCtrlEvent, SilkEvent.TorrentEvent, SilkEvent.FileEvent {
  }

  public static Identifier fsmBaseId(OverlayId torrentId, Identifier fileId) {
    return new PairIdentifier(torrentId, fileId);
  }

  public static class IS implements FSMInternalState {

    private final FSMIdentifier fsmId;
    ISEvents reqs = new ISEvents();

    public IS(FSMIdentifier fsmId) {
      this.fsmId = fsmId;
    }

    @Override
    public FSMIdentifier getFSMId() {
      return fsmId;
    }
  }

  public static class ISEvents {

    R2StreamMngrEvents.Open open;
    R2StreamMngrEvents.Close close;
  }

  public static class ISBuilder implements FSMInternalStateBuilder {

    @Override
    public FSMInternalState newState(FSMIdentifier fsmId) {
      return new IS(fsmId);
    }
  }

  public static class ES implements R2TorrentES {

    private ComponentProxy proxy;
    R2TorrentComp.Ports ports;
    IntIdFactory fileIdFactory;

    public ES() {
      this.fileIdFactory = new IntIdFactory(new Random(seed));
    }

    @Override
    public void setProxy(ComponentProxy proxy) {
      this.proxy = proxy;
    }

    @Override
    public ComponentProxy getProxy() {
      return proxy;
    }

    @Override
    public void setPorts(R2TorrentComp.Ports ports) {
      this.ports = ports;
    }
  }

  public static class FSM {

    private static FSMBuilder.StructuralDefinition structuralDef() throws FSMException {
      return FSMBuilder.structuralDef()
        .onStart()
        .nextStates(States.OPEN)
        .buildTransition()
        .onState(States.OPEN)
        .nextStates(States.ACTIVE, States.CLOSE)
        .buildTransition()
        .onState(States.ACTIVE)
        .nextStates(States.CLOSE)
        .buildTransition()
        .onState(States.CLOSE)
        .toFinal()
        .buildTransition();
    }

    private static FSMBuilder.SemanticDefinition semanticDef() throws FSMException {
      FSMBuilder.SemanticDefinition def = FSMBuilder.semanticDef()
        .defaultFallback(DefaultHandlers.basicDefault(), DefaultHandlers.patternDefault());
      def = def
        .positivePort(R2TorrentPort.class)
        .basicEvent(R2StreamMngrEvents.Open.class)
        .subscribeOnStart(Handlers.streamOpen)
        .basicEvent(R2StreamMngrEvents.Close.class)
        .subscribe(Handlers.streamClose, States.OPEN, States.ACTIVE)
        .buildEvents();
      
      def = def
        .positivePort(DStreamControlPort.class)
        .basicEvent(DStreamConnect.Success.class)
        .subscribe(Handlers.streamOpened, States.OPEN)
        .basicEvent(DStreamDisconnect.Success.class)
        .subscribe(Handlers.streamClosed, States.CLOSE)
        .buildEvents();
      return def;
    }

    static BaseIdExtractor baseIdExtractor(ES es) {
      return (KompicsEvent event) -> {
        if (event instanceof R1StreamEvent) {
          R1StreamEvent e = (R1StreamEvent) event;
          return Optional.of(fsmBaseId(e.torrentId(), e.fileId()));
        } else if (event instanceof DStreamConnect.Success) {
          DStreamConnect.Success e = (DStreamConnect.Success) event;
          OverlayId torrentId = e.req.stream.getValue0().fileId.torrentId;
          Identifier fileId = es.fileIdFactory.
            id(new BasicBuilders.IntBuilder(e.req.stream.getValue0().fileId.fileNr));
          return Optional.of(fsmBaseId(torrentId, fileId));
        } else if (event instanceof DStreamDisconnect.Success) {
          DStreamDisconnect.Success e = (DStreamDisconnect.Success) event;
          OverlayId torrentId = e.req.streamId.fileId.torrentId;
          Identifier fileId = es.fileIdFactory.
            id(new BasicBuilders.IntBuilder(e.req.streamId.fileId.fileNr));
          return Optional.of(fsmBaseId(torrentId, fileId));
        } else {
          return Optional.empty();
        }
      };
    }

    public static MultiFSM multifsm(FSMIdentifierFactory fsmIdFactory, ES es, OnFSMExceptionAction oexa)
      throws FSMException {
      FSMInternalStateBuilder isb = new ISBuilder();
      return FSMBuilder.multiFSM(fsmIdFactory, NAME, structuralDef(), semanticDef(), es, isb, oexa, baseIdExtractor(es));
    }
  }

  public static class Handlers {

    static FSMBasicEventHandler streamOpen = (FSMBasicEventHandler<ES, IS, R2StreamMngrEvents.Open>) (
      FSMStateName state, ES es, IS is, R2StreamMngrEvents.Open event) -> {
        is.reqs.open = event;
        DStreamConnect.Request req = new DStreamConnect.Request(Pair.with(event.streamId, event.stream));
        sendStreamCtrl(es, req);
        return States.OPEN;
      };

    static FSMBasicEventHandler streamOpened = (FSMBasicEventHandler<ES, IS, DStreamConnect.Success>) (
      FSMStateName state, ES es, IS is, DStreamConnect.Success event) -> {
        sendR1StreamCtrl(es, is.reqs.open.success());
        return States.ACTIVE;
      };

    static FSMBasicEventHandler streamClose = (FSMBasicEventHandler<ES, IS, R2StreamMngrEvents.Close>) (
      FSMStateName state, ES es, IS is, R2StreamMngrEvents.Close event) -> {
        is.reqs.close = event;
        DStreamDisconnect.Request req = new DStreamDisconnect.Request(event.streamId);
        sendStreamCtrl(es, req);
        return States.CLOSE;
      };

    static FSMBasicEventHandler streamClosed = (FSMBasicEventHandler<ES, IS, DStreamDisconnect.Success>) (
      FSMStateName state, ES es, IS is, DStreamDisconnect.Success event) -> {
        sendR1StreamCtrl(es, is.reqs.close.ack());
        return FSMBasicStateNames.FINAL;
      };
    
    private static void sendStreamCtrl(ES es, DStreamEvent e) {
      es.getProxy().trigger(e, es.ports.streamCtrl);
    }

    private static void sendR1StreamCtrl(ES es, R2StreamCtrlEvent e) {
      es.getProxy().trigger(e, es.ports.loopbackSend);
    }
  }
}
