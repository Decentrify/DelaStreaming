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
import se.sics.kompics.network.Transport;
import se.sics.kompics.util.Identifiable;
import se.sics.kompics.util.Identifier;
import se.sics.ktoolbox.util.identifiable.basic.PairIdentifier;
import se.sics.ktoolbox.util.identifiable.overlay.OverlayId;
import se.sics.ktoolbox.util.network.KAddress;
import se.sics.ktoolbox.util.network.KContentMsg;
import se.sics.ktoolbox.util.network.KHeader;
import se.sics.ktoolbox.util.network.basic.BasicContentMsg;
import se.sics.ktoolbox.util.network.basic.BasicHeader;
import se.sics.nutil.network.bestEffort.event.BestEffortMsg;
import se.sics.silk.DefaultHandlers;
import se.sics.silk.event.SilkEvent;
import se.sics.silk.r2torrent.R2TorrentComp;
import se.sics.silk.r2torrent.R2TorrentES;
import se.sics.silk.r2torrent.R2TorrentPort;
import se.sics.silk.r2torrent.conn.R2NodeSeeder.HardCodedConfig;
import se.sics.silk.r2torrent.conn.event.R1TorrentSeederEvents;
import se.sics.silk.r2torrent.conn.event.R2NodeSeederEvents;
import se.sics.silk.r2torrent.torrent.event.R1MetadataGetEvents;
import se.sics.silk.r2torrent.torrent.msg.R1MetadataMsgs;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class R1MetadataGet {

  private static final Logger LOG = LoggerFactory.getLogger(FSM.class);
  public static final String NAME = "dela-r1-torrent-metadata-get-fsm";

  public static Identifier fsmBaseId(OverlayId torrentId, Identifier fileId, Identifier seederId) {
    return new PairIdentifier(new PairIdentifier(torrentId, fileId), seederId);
  }

  public static enum States implements FSMStateName {

    CONNECT,
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
  
  public static interface StreamEvent extends Event, R1StreamCtrlEvent {
  }

  public static class HardCodedcConfig {

    public static final int retries = 2;
    public static final int retryInterval = 1000;
  }

  public static class IS implements FSMInternalState {

    FSMIdentifier fsmId;
    OverlayId torrentId;
    Identifier fileId;
    KAddress seeder;
    R1MetadataGetEvents.GetReq metaGetReq;

    public IS(FSMIdentifier fsmId) {
      this.fsmId = fsmId;
    }

    @Override
    public FSMIdentifier getFSMId() {
      return fsmId;
    }

    public void init(R1MetadataGetEvents.GetReq req) {
      torrentId = req.torrentId;
      fileId = req.fileId;
      seeder = req.seeder;
      metaGetReq = req;
    }
  }

  public static class ISBuilder implements FSMInternalStateBuilder {

    @Override
    public FSMInternalState newState(FSMIdentifier fsmId) {
      return new IS(fsmId);
    }
  }

  public static class ES implements R2TorrentES {

    R2TorrentComp.Ports ports;
    ComponentProxy proxy;
    KAddress selfAdr;

    public ES(KAddress selfAdr) {
      this.selfAdr = selfAdr;
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
        .nextStates(States.CONNECT)
        .toFinal()
        .buildTransition()
        .onState(States.CONNECT)
        .nextStates(States.ACTIVE)
        .toFinal()
        .buildTransition()
        .onState(States.ACTIVE)
        .toFinal()
        .buildTransition();
    }

    private static FSMBuilder.SemanticDefinition semanticDef() throws FSMException {
      FSMBuilder.SemanticDefinition def = FSMBuilder.semanticDef()
        .defaultFallback(DefaultHandlers.basicDefault(), DefaultHandlers.patternDefault());

      def = def
        .positivePort(R2TorrentPort.class)
        .basicEvent(R1MetadataGetEvents.GetReq.class)
        .subscribeOnStart(Handlers.metadataGet)
        .basicEvent(R1TorrentSeederEvents.ConnectSucc.class)
        .subscribe(Handlers.connSucc, States.CONNECT)
        .basicEvent(R2NodeSeederEvents.ConnectFail.class)
        .subscribe(Handlers.connFail, States.CONNECT)
        .basicEvent(R1MetadataGetEvents.Stop.class)
        .subscribe(Handlers.stop)
        .subscribe(Handlers.stop, States.CONNECT, States.ACTIVE)
        .buildEvents();

      def = def
        .positivePort(Network.class)
        .patternEvent(R1MetadataMsgs.Serve.class, BasicContentMsg.class)
        .subscribe(Handlers.getResp, States.ACTIVE)
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

    static FSMBasicEventHandler metadataGet = new FSMBasicEventHandler<ES, IS, R1MetadataGetEvents.GetReq>() {
      @Override
      public FSMStateName handle(FSMStateName state, ES es, IS is, R1MetadataGetEvents.GetReq req) {
        is.init(req);
        R1TorrentSeederEvents.ConnectReq r = new R1TorrentSeederEvents.ConnectReq(is.torrentId, is.fileId, is.seeder);
        sendTorrentSeeder(es, r);
        return States.CONNECT;
      }
    };

    static FSMBasicEventHandler connSucc = new FSMBasicEventHandler<ES, IS, R1TorrentSeederEvents.ConnectSucc>() {
      @Override
      public FSMStateName handle(FSMStateName state, ES es, IS is, R1TorrentSeederEvents.ConnectSucc event) throws
        FSMException {
        R1MetadataMsgs.Get payload = new R1MetadataMsgs.Get(is.torrentId, is.fileId);
        bestEffortMsg(es, is.seeder, payload);
        return States.ACTIVE;
      }
    };

    static FSMBasicEventHandler connFail = new FSMBasicEventHandler<ES, IS, R2NodeSeederEvents.ConnectSucc>() {
      @Override
      public FSMStateName handle(FSMStateName state, ES es, IS is, R2NodeSeederEvents.ConnectSucc event) throws
        FSMException {
        sendTorrent(es, is.metaGetReq.fail());
        return FSMBasicStateNames.FINAL;
      }
    };

    static FSMBasicEventHandler stop = new FSMBasicEventHandler<ES, IS, R1MetadataGetEvents.Stop>() {
      @Override
      public FSMStateName handle(FSMStateName state, ES es, IS is, R1MetadataGetEvents.Stop req) {
        sendTorrent(es, req.ack());
        return FSMBasicStateNames.FINAL;
      }
    };

    static FSMPatternEventHandler getResp
      = new FSMPatternEventHandler<ES, IS, R1MetadataMsgs.Serve, BasicContentMsg>() {

        @Override
        public FSMStateName handle(FSMStateName state, ES es, IS is, R1MetadataMsgs.Serve payload,
          BasicContentMsg container) throws FSMException {
          sendTorrent(es, is.metaGetReq.success());
          return FSMBasicStateNames.FINAL;
        }
      };

    private static <C extends KompicsEvent & Identifiable> void bestEffortMsg(ES es, KAddress target, C content) {
      KHeader header = new BasicHeader(es.selfAdr, target, Transport.UDP);
      BestEffortMsg.Request wrap = new BestEffortMsg.Request(content, HardCodedConfig.retries,
        HardCodedConfig.retryInterval);
      KContentMsg msg = new BasicContentMsg(header, wrap);
      es.getProxy().trigger(msg, es.ports.network);
    }

    private static void sendTorrent(ES es, R1MetadataGetEvents.Ind e) {
      es.getProxy().trigger(e, es.ports.loopbackSend);
    }

    private static void sendTorrentSeeder(ES es, R1TorrentSeederEvents.Req e) {
      es.getProxy().trigger(e, es.ports.loopbackSend);
    }
  }
}
