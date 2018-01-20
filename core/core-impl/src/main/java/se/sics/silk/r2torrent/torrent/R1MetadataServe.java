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
import se.sics.kompics.PatternExtractor;
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
import se.sics.kompics.fsm.handler.FSMPatternEventHandler;
import se.sics.kompics.fsm.id.FSMIdentifier;
import se.sics.kompics.fsm.id.FSMIdentifierFactory;
import se.sics.kompics.network.Network;
import se.sics.kompics.util.Identifiable;
import se.sics.kompics.util.Identifier;
import se.sics.ktoolbox.util.identifiable.basic.PairIdentifier;
import se.sics.ktoolbox.util.identifiable.overlay.OverlayId;
import se.sics.ktoolbox.util.network.KContentMsg;
import se.sics.ktoolbox.util.network.basic.BasicContentMsg;
import se.sics.silk.DefaultHandlers;
import se.sics.silk.event.SilkEvent;
import se.sics.silk.r2torrent.R2TorrentComp;
import se.sics.silk.r2torrent.R2TorrentES;
import se.sics.silk.SelfPort;
import se.sics.silk.r2torrent.torrent.event.R1MetadataServeEvents;
import se.sics.silk.r2torrent.torrent.msg.R1MetadataMsgs;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class R1MetadataServe {

  private static final Logger LOG = LoggerFactory.getLogger(FSM.class);
  public static final String NAME = "dela-r1-torrent-metadata-serve-fsm";

  public static Identifier fsmBaseId(OverlayId torrentId, Identifier fileId, Identifier seederId) {
    return new PairIdentifier(new PairIdentifier(torrentId, fileId), seederId);
  }

  public static enum States implements FSMStateName {

    ACTIVE
  }

  public static Identifier fsmBaseId(OverlayId torrentId, Identifier fileId) {
    return new PairIdentifier(torrentId, fileId);
  }

  public static interface Event extends FSMEvent, Identifiable, SilkEvent.TorrentEvent, SilkEvent.FileEvent {
  }

  public static interface TorrentEvent extends Event {
  }

  public static interface ConnEvent extends Event {
  }

  public static interface Msg extends Event {
  }
  
  public static interface StreamEvent extends R1MetadataGet.Event {
  }

  public static class IS implements FSMInternalState {

    FSMIdentifier fsmId;
    OverlayId torrentId;
    Identifier fileId;
    R1MetadataServeEvents.ServeReq metaServeReq;

    public IS(FSMIdentifier fsmId) {
      this.fsmId = fsmId;
    }

    @Override
    public FSMIdentifier getFSMId() {
      return fsmId;
    }

    public void init(R1MetadataServeEvents.ServeReq req) {
      torrentId = req.torrentId;
      fileId = req.fileId;
      metaServeReq = req;
    }
  }

  public static class ISBuilder implements FSMInternalStateBuilder {

    @Override
    public FSMInternalState newState(FSMIdentifier fsmId) {
      return new IS(fsmId);
    }
  }

  public static class ES implements R2TorrentES {

    private R2TorrentComp.Ports ports;
    private ComponentProxy proxy;

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
        .nextStates(States.ACTIVE)
        .toFinal()
        .buildTransition()
        .onState(States.ACTIVE)
        .nextStates(States.ACTIVE)
        .toFinal()
        .buildTransition();
    }

    private static FSMBuilder.SemanticDefinition semanticDef() throws FSMException {
      FSMBuilder.SemanticDefinition def = FSMBuilder.semanticDef()
        .defaultFallback(DefaultHandlers.basicDefault(), DefaultHandlers.patternDefault());

      def = def
        .positivePort(SelfPort.class)
        .basicEvent(R1MetadataServeEvents.ServeReq.class)
        .subscribeOnStart(Handlers.serveRequest)
        .basicEvent(R1MetadataServeEvents.Stop.class)
        .subscribe(Handlers.stop, States.ACTIVE)
        .buildEvents();

      def = def
        .positivePort(Network.class)
        .patternEvent(R1MetadataMsgs.Get.class, BasicContentMsg.class)
        .subscribe(Handlers.serve, States.ACTIVE)
        .buildEvents();
      return def;
    }

    static BaseIdExtractor baseIdExtractor = new BaseIdExtractor() {

      @Override
      public Optional<Identifier> fromEvent(KompicsEvent event) throws FSMException {
        if (event instanceof Event) {
          Event e = (Event) event;
          return Optional.of(fsmBaseId(e.torrentId(), e.fileId()));
        } else if (event instanceof PatternExtractor) {
          PatternExtractor pattern = (PatternExtractor) event;
          if (pattern.extractValue() instanceof Event) {
            Event e = (Event) pattern.extractValue();
            return Optional.of(fsmBaseId(e.torrentId(), e.fileId()));
          }
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

    static FSMBasicEventHandler serveRequest = new FSMBasicEventHandler<ES, IS, R1MetadataServeEvents.ServeReq>() {
      @Override
      public FSMStateName handle(FSMStateName state, ES es, IS is, R1MetadataServeEvents.ServeReq req) {
        is.init(req);
        sendTorrent(es, is.metaServeReq.success());
        return States.ACTIVE;
      }
    };

    static FSMBasicEventHandler stop = new FSMBasicEventHandler<ES, IS, R1MetadataServeEvents.Stop>() {
      @Override
      public FSMStateName handle(FSMStateName state, ES es, IS is, R1MetadataServeEvents.Stop req) {
        sendTorrent(es, req.ack());
        return FSMBasicStateNames.FINAL;
      }
    };

    static FSMPatternEventHandler serve = new FSMPatternEventHandler<ES, IS, R1MetadataMsgs.Get, BasicContentMsg>() {

      @Override
      public FSMStateName handle(FSMStateName state, ES es, IS is, R1MetadataMsgs.Get payload,
        BasicContentMsg msg) throws FSMException {
        answerNet(es, msg, payload.answer());
        return state;
      }
    };

    private static <C extends KompicsEvent & Identifiable> void answerNet(ES es, BasicContentMsg msg, C payload) {
      KContentMsg resp = msg.answer(payload);
      es.getProxy().trigger(resp, es.ports.network);
    }

    private static void sendTorrent(ES es, R2Torrent.MetadataEvent e) {
      es.getProxy().trigger(e, es.ports.loopbackSend);
    }
  }
}
