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
package se.sics.silk.r2torrent;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
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
import se.sics.kompics.fsm.FSMExternalState;
import se.sics.kompics.fsm.FSMInternalState;
import se.sics.kompics.fsm.FSMInternalStateBuilder;
import se.sics.kompics.fsm.FSMStateName;
import se.sics.kompics.fsm.MultiFSM;
import se.sics.kompics.fsm.OnFSMExceptionAction;
import se.sics.kompics.fsm.handler.FSMBasicEventHandler;
import se.sics.kompics.fsm.handler.FSMPatternEventHandler;
import se.sics.kompics.fsm.id.FSMIdentifier;
import se.sics.kompics.fsm.id.FSMIdentifierFactory;
import se.sics.kompics.util.Identifier;
import se.sics.ktoolbox.util.identifiable.overlay.OverlayId;
import se.sics.ktoolbox.util.network.KAddress;
import se.sics.silk.r2torrent.event.R1HashEvents;
import se.sics.silk.r2torrent.event.R1MetadataEvents;
import se.sics.silk.r2torrent.event.R2TorrentCtrlEvents;
import se.sics.silk.r2torrent.util.R2TorrentStatus;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class R2Torrent {

  private static final Logger LOG = LoggerFactory.getLogger(FSM.class);
  public static final String NAME = "dela-r2-torrent-fsm";

  public static class HackConfig {

    public final int seederOpenConn = 5;
    public final int filePerSeeder = 5;
  }

  public static enum States implements FSMStateName {

    META_GET,
    META_SERVE,
    DATA_STORAGE,
    HASH,
    UPLOAD,
    TRANSFER,
    CLEAN_META,
    CLEAN_HASH,
    CLEAN_UPLOAD,
    CLEAN_TRANSFER
  }

  public static interface Event extends FSMEvent {

    public Identifier getR2TorrentFSMId();
  }

  public static interface HashEvent extends Event {
  }

  public static interface MetadataEvent extends Event {

  }

  public static interface CtrlEvent extends Event {
  }

  public static class IS implements FSMInternalState {

    private final FSMIdentifier fsmId;
    OverlayId torrentId;
    final ISSeederState seeders = new ISSeederState();
    final ISCtrlEvents ctrl = new ISCtrlEvents();

    public IS(FSMIdentifier fsmId) {
      this.fsmId = fsmId;
    }

    @Override
    public FSMIdentifier getFSMId() {
      return fsmId;
    }

    public void setGetMetaReq(R2TorrentCtrlEvents.MetaGetReq req) {
      torrentId = req.torrentId;
      seeders.setSample(req.partners);
      ctrl.getMetaReq = req;
    }

    public void setStopReq(R2TorrentCtrlEvents.Stop req) {
      ctrl.stopReq = req;
    }

    public void setUploadReq(R2TorrentCtrlEvents.Upload req) {
      torrentId = req.torrentId;
      ctrl.uploadReq = req;
    }
  }

  /**
   * TODO Alex - naive implementation for testing
   */
  static class ISSeederState {

    List<KAddress> seeders;
    KAddress metadata;

    Map<Identifier, ISSeederConn> openConn = new HashMap<>();

    public void setSample(List<KAddress> sample) {
      seeders = sample;
      metadata = seeders.get(0);
    }

    public KAddress getMetadataSeeder() {
      return metadata;
    }
  }

  static class ISSeederConn {

    KAddress seederAdr;
    Set<Integer> activeFiles = new TreeSet<>();
  }

  public static class ISCtrlEvents {

    private R2TorrentCtrlEvents.MetaGetReq getMetaReq;
    private R2TorrentCtrlEvents.Upload uploadReq;
    private R2TorrentCtrlEvents.Stop stopReq;
  }

  public static class ISBuilder implements FSMInternalStateBuilder {

    @Override
    public FSMInternalState newState(FSMIdentifier fsmId) {
      return new IS(fsmId);
    }
  }

  public static class ES implements FSMExternalState {

    public final R2TorrentComp.Ports ports;
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
        .nextStates(States.META_GET, States.META_SERVE)
        .buildTransition()
        .onState(States.META_GET)
        .nextStates(States.DATA_STORAGE, States.CLEAN_META)
        .toFinal()
        .buildTransition()
        .onState(States.META_SERVE)
        .nextStates(States.HASH, States.CLEAN_META)
        .buildTransition()
        .onState(States.DATA_STORAGE)
        .nextStates(States.HASH, States.CLEAN_HASH)
        .buildTransition()
        .onState(States.HASH)
        .nextStates(States.UPLOAD, States.TRANSFER, States.CLEAN_HASH, States.CLEAN_META)
        .buildTransition()
        .onState(States.TRANSFER)
        .nextStates(States.CLEAN_HASH)
        .buildTransition()
        .onState(States.UPLOAD)
        .nextStates(States.CLEAN_HASH)
        .buildTransition()
        .onState(States.CLEAN_HASH)
        .nextStates(States.CLEAN_META)
        .buildTransition()
        .onState(States.CLEAN_META)
        .toFinal()
        .buildTransition();
    }

    private static FSMBuilder.SemanticDefinition semanticDef() throws FSMException {
      return FSMBuilder.semanticDef()
        .defaultFallback(Handlers.basicDefault(), Handlers.patternDefault())
        .negativePort(R2TorrentCtrlPort.class)
        .basicEvent(R2TorrentCtrlEvents.MetaGetReq.class)
        .subscribeOnStart(Handlers.getMetaReq)
        .basicEvent(R2TorrentCtrlEvents.Download.class)
        .subscribe(Handlers.dataStorage, States.DATA_STORAGE)
        .basicEvent(R2TorrentCtrlEvents.Upload.class)
        .subscribeOnStart(Handlers.upload)
        .basicEvent(R2TorrentCtrlEvents.Stop.class)
        .subscribe(Handlers.stop1, States.META_GET, States.META_SERVE)
        .subscribe(Handlers.stop2, States.DATA_STORAGE, States.HASH, States.UPLOAD, States.TRANSFER)
        .buildEvents()
        .positivePort(R2TorrentPort.class)
        .basicEvent(R1MetadataEvents.MetaGetSucc.class)
        .subscribe(Handlers.metaGetSucc, States.META_GET)
        .basicEvent(R1MetadataEvents.MetaGetFail.class)
        .subscribe(Handlers.metaGetFail, States.META_GET)
        .basicEvent(R1MetadataEvents.MetaServeSucc.class)
        .subscribe(Handlers.metaServeSucc, States.META_SERVE)
        .basicEvent(R1MetadataEvents.MetaStopAck.class)
        .subscribe(Handlers.metaClean, States.CLEAN_META)
        .basicEvent(R1HashEvents.HashSucc.class)
        .subscribe(Handlers.hashSucc, States.HASH)
        .basicEvent(R1HashEvents.HashFail.class)
        .subscribe(Handlers.hashFail, States.HASH)
        .basicEvent(R1HashEvents.HashStopAck.class)
        .subscribe(Handlers.cleanHash, States.CLEAN_HASH)
        .buildEvents();
    }

    static BaseIdExtractor baseIdExtractor = new BaseIdExtractor() {

      @Override
      public Optional<Identifier> fromEvent(KompicsEvent event) throws FSMException {
        if (event instanceof Event) {
          return Optional.of(((Event) event).getR2TorrentFSMId());
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

    static FSMBasicEventHandler basicDefault() {
      return new FSMBasicEventHandler<ES, IS, KompicsEvent>() {
        @Override
        public FSMStateName handle(FSMStateName state, ES es, IS is, KompicsEvent req) {
          if (FSMBasicStateNames.START.equals(state)) {
            return FSMBasicStateNames.FINAL;
          } else {
            return state;
          }
        }
      };
    }

    static FSMPatternEventHandler patternDefault() {
      return new FSMPatternEventHandler<ES, IS, KompicsEvent>() {
        @Override
        public FSMStateName handle(FSMStateName state, ES es, IS is, KompicsEvent req,
          PatternExtractor<Class, KompicsEvent> container) {
          if (FSMBasicStateNames.START.equals(state)) {
            return FSMBasicStateNames.FINAL;
          } else {
            return state;
          }
        }
      };
    }

    //****************************************************CTRL**********************************************************
    static FSMBasicEventHandler stop1 = new FSMBasicEventHandler<ES, IS, R2TorrentCtrlEvents.Stop>() {
      @Override
      public FSMStateName handle(FSMStateName state, ES es, IS is, R2TorrentCtrlEvents.Stop req) {
        is.setStopReq(req);
        R1MetadataEvents.MetaStop r = new R1MetadataEvents.MetaStop(is.torrentId);
        sendMetadata(es, r);
        return States.CLEAN_META;
      }
    };

    static FSMBasicEventHandler stop2 = new FSMBasicEventHandler<ES, IS, R2TorrentCtrlEvents.Stop>() {
      @Override
      public FSMStateName handle(FSMStateName state, ES es, IS is, R2TorrentCtrlEvents.Stop req) {
        is.setStopReq(req);
        R1HashEvents.HashStop r = new R1HashEvents.HashStop(is.torrentId);
        sendHash(es, r);
        return States.CLEAN_HASH;
      }
    };

    static FSMBasicEventHandler getMetaReq = new FSMBasicEventHandler<ES, IS, R2TorrentCtrlEvents.MetaGetReq>() {
      @Override
      public FSMStateName handle(FSMStateName state, ES es, IS is, R2TorrentCtrlEvents.MetaGetReq req) {
        is.setGetMetaReq(req);
        R1MetadataEvents.MetaGetReq r = new R1MetadataEvents.MetaGetReq(is.torrentId, is.seeders.getMetadataSeeder());
        sendMetadata(es, r);
        return States.META_GET;
      }
    };

    static FSMBasicEventHandler dataStorage = new FSMBasicEventHandler<ES, IS, R2TorrentCtrlEvents.Download>() {
      @Override
      public FSMStateName handle(FSMStateName state, ES es, IS is, R2TorrentCtrlEvents.Download req) {
        R1HashEvents.HashReq r = new R1HashEvents.HashReq(is.torrentId);
        sendHash(es, r);
        return States.HASH;
      }
    };

    static FSMBasicEventHandler upload = new FSMBasicEventHandler<ES, IS, R2TorrentCtrlEvents.Upload>() {
      @Override
      public FSMStateName handle(FSMStateName state, ES es, IS is, R2TorrentCtrlEvents.Upload req) {
        is.setUploadReq(req);
        R1MetadataEvents.MetaServeReq r = new R1MetadataEvents.MetaServeReq(is.torrentId);
        sendMetadata(es, r);
        return States.META_SERVE;
      }
    };
    //****************************************************TRANSFER******************************************************
    static FSMBasicEventHandler metaClean = new FSMBasicEventHandler<ES, IS, R1MetadataEvents.MetaStopAck>() {
      @Override
      public FSMStateName handle(FSMStateName state, ES es, IS is, R1MetadataEvents.MetaStopAck ack) {
        if (is.ctrl.stopReq != null) {
          sendCtrl(es, is.ctrl.stopReq.ack());
        }
        return FSMBasicStateNames.FINAL;
      }
    };

    static FSMBasicEventHandler metaGetSucc = new FSMBasicEventHandler<ES, IS, R1MetadataEvents.MetaGetSucc>() {
      @Override
      public FSMStateName handle(FSMStateName state, ES es, IS is, R1MetadataEvents.MetaGetSucc resp) {
        sendCtrl(es, is.ctrl.getMetaReq.success());
        return States.DATA_STORAGE;
      }
    };

    static FSMBasicEventHandler metaGetFail = new FSMBasicEventHandler<ES, IS, R1MetadataEvents.MetaGetFail>() {
      @Override
      public FSMStateName handle(FSMStateName state, ES es, IS is, R1MetadataEvents.MetaGetFail resp) {
        sendCtrl(es, is.ctrl.getMetaReq.fail());
        return FSMBasicStateNames.FINAL;
      }
    };

    static FSMBasicEventHandler metaServeSucc
      = new FSMBasicEventHandler<ES, IS, R1MetadataEvents.MetaServeSucc>() {
        @Override
        public FSMStateName handle(FSMStateName state, ES es, IS is, R1MetadataEvents.MetaServeSucc resp) {
          LOG.info("META-SERVE success - processing");
          sendCtrl(es, is.ctrl.uploadReq.success(R2TorrentStatus.HASH));
          R1HashEvents.HashReq r = new R1HashEvents.HashReq(is.torrentId);
          sendHash(es, r);
          LOG.info("META-SERVE success - processed");
          return States.HASH;
        }
      };

    static FSMBasicEventHandler hashSucc = new FSMBasicEventHandler<ES, IS, R1HashEvents.HashSucc>() {
      @Override
      public FSMStateName handle(FSMStateName state, ES es, IS is, R1HashEvents.HashSucc resp) {
        if (is.ctrl.uploadReq != null) {
          R2TorrentCtrlEvents.TorrentBaseInfo ind = new R2TorrentCtrlEvents.TorrentBaseInfo(is.torrentId,
            R2TorrentStatus.UPLOAD);
          sendCtrl(es, ind);
          return States.UPLOAD;
        } else {
          R2TorrentCtrlEvents.TorrentBaseInfo ind = new R2TorrentCtrlEvents.TorrentBaseInfo(is.torrentId,
            R2TorrentStatus.TRANSFER);
          sendCtrl(es, ind);
          return States.TRANSFER;
        }
      }
    };

    static FSMBasicEventHandler hashFail = new FSMBasicEventHandler<ES, IS, R1HashEvents.HashFail>() {
      @Override
      public FSMStateName handle(FSMStateName state, ES es, IS is, R1HashEvents.HashFail resp) {
        R2TorrentCtrlEvents.TorrentBaseInfo ind = new R2TorrentCtrlEvents.TorrentBaseInfo(is.torrentId,
          R2TorrentStatus.ERROR);
        sendCtrl(es, ind);
        R1MetadataEvents.MetaStop r = new R1MetadataEvents.MetaStop(is.torrentId);
        sendMetadata(es, r);
        return States.CLEAN_META;
      }
    };

    static FSMBasicEventHandler cleanHash = new FSMBasicEventHandler<ES, IS, R1HashEvents.HashStopAck>() {
      @Override
      public FSMStateName handle(FSMStateName state, ES es, IS is, R1HashEvents.HashStopAck ack) {
        R1MetadataEvents.MetaStop r = new R1MetadataEvents.MetaStop(is.torrentId);
        sendMetadata(es, r);
        return States.CLEAN_META;
      }
    };

    private static void sendMetadata(ES es, R1MetadataGet.TorrentEvent e) {
      es.getProxy().trigger(e, es.ports.loopbackSend);
    }

    private static void sendHash(ES es, R1Hash.TorrentEvent e) {
      es.getProxy().trigger(e, es.ports.loopbackSend);
    }

    private static void sendCtrl(ES es, CtrlEvent e) {
      es.getProxy().trigger(e, es.ports.ctrl);
    }
  }
}