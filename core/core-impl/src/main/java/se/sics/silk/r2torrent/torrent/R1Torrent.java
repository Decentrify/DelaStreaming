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

import com.google.common.collect.Sets;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.kompics.ComponentProxy;
import se.sics.kompics.KompicsEvent;
import se.sics.kompics.fsm.BaseIdExtractor;
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
import se.sics.kompics.util.Identifier;
import se.sics.ktoolbox.util.Either;
import se.sics.ktoolbox.util.identifiable.basic.IntIdFactory;
import se.sics.ktoolbox.util.identifiable.overlay.OverlayId;
import se.sics.ktoolbox.util.network.KAddress;
import se.sics.nstream.storage.durable.DEndpointCtrlPort;
import se.sics.nstream.storage.durable.events.DEndpoint;
import se.sics.silk.DefaultHandlers;
import se.sics.silk.event.SilkEvent;
import se.sics.silk.r2torrent.R2TorrentComp;
import se.sics.silk.r2torrent.R2TorrentES;
import static se.sics.silk.r2torrent.torrent.R1Torrent.HardCodedConfig.seed;
import se.sics.silk.r2torrent.torrent.event.R1FileDownloadEvents;
import se.sics.silk.r2torrent.torrent.event.R1FileUploadEvents;
import se.sics.silk.r2torrent.torrent.event.R1TorrentConnEvents;
import se.sics.silk.r2torrent.torrent.event.R1TorrentCtrlEvents;
import se.sics.silk.r2torrent.torrent.util.R1TorrentDetails;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class R1Torrent {

  private static final Logger LOG = LoggerFactory.getLogger(R1Torrent.class);
  public static final String NAME = "dela-r1-torrent-fsm";

  public static enum States implements FSMStateName {

    ENDPOINT,
    DOWNLOAD,
    UPLOAD
  }

  public static interface Event extends FSMEvent, SilkEvent.TorrentEvent {
  }

  public static interface CtrlEvent extends Event {
  }

  public static interface ConnEvent extends Event {
  }

  public static interface DownloadCtrl extends Event {
  }

  public static interface UploadCtrl extends Event {
  }

  public static Identifier fsmBaseId(OverlayId torrentId) {
    return torrentId;
  }

  public static class HardCodedConfig {

    public static final long seed = 1234;
    public final int seederOpenConn = 5;
    public final int filePerSeeder = 5;
  }

  public static class IS implements FSMInternalState {

    private final FSMIdentifier fsmId;
    OverlayId torrentId;
    R1TorrentDetails torrentDetails;
    FileSeeders fileSeeders = new FileSeeders();
    Either<Download, Upload> state;

    public IS(FSMIdentifier fsmId) {
      this.fsmId = fsmId;
    }

    @Override
    public FSMIdentifier getFSMId() {
      return fsmId;
    }

    public void upload(OverlayId torrentId, R1TorrentDetails torrentDetails) {
      this.torrentId = torrentId;
      this.torrentDetails = torrentDetails;
      this.state = Either.right(new Upload());
    }

    public void download(OverlayId torrentId, R1TorrentDetails torrentDetails, 
      Set<KAddress> bootstrap) {
      this.torrentId = torrentId;
      this.torrentDetails = torrentDetails;
      this.state = Either.left(new Download(bootstrap));
    }
  }

  static class Upload {

  }

  static class Download {

    public Set<KAddress> bootstrap;

    public Download(Set<KAddress> bootstrap) {
      this.bootstrap = bootstrap;
    }
  }

  static class FileSeeders {

    Map<Identifier, KAddress> fileSeeders = new HashMap<>();
    Set<KAddress> activeSeeders = new HashSet<>();

    public void downloadFile(Identifier fileId, KAddress seeder) {
      fileSeeders.put(fileId, seeder);
      activeSeeders.add(seeder);
    }

    public Optional<KAddress> completed(Identifier fileId) {
      KAddress seeder = fileSeeders.remove(fileId);
      if (seeder != null) {
        activeSeeders.remove(seeder);
      }
      return Optional.ofNullable(seeder);
    }

    public Optional<KAddress> getSeeder(Identifier fileId) {
      return Optional.ofNullable(fileSeeders.get(fileId));
    }
  }

  public static class ISBuilder implements FSMInternalStateBuilder {

    @Override
    public FSMInternalState newState(FSMIdentifier fsmId) {
      return new IS(fsmId);
    }
  }

  public static class ES implements R2TorrentES {

    public R2TorrentComp.Ports ports;
    private ComponentProxy proxy;
    public final IntIdFactory fileIdFactory;
    public final R1TorrentDetails.Mngr torrentDetailsMngr;

    public ES(R1TorrentDetails.Mngr torrentDetailsMngr) {
      this.fileIdFactory = new IntIdFactory(new Random(seed));
      this.torrentDetailsMngr = torrentDetailsMngr;
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
        .nextStates(States.ENDPOINT)
        .buildTransition()
        .onState(States.ENDPOINT)
        .nextStates(States.DOWNLOAD, States.UPLOAD)
        .buildTransition()
        .onState(States.DOWNLOAD)
        .nextStates(States.DOWNLOAD, States.UPLOAD)
        .toFinal()
        .buildTransition()
        .onState(States.UPLOAD)
        .nextStates(States.UPLOAD)
        .toFinal()
        .buildTransition();
    }

    private static FSMBuilder.SemanticDefinition semanticDef() throws FSMException {
      FSMBuilder.SemanticDefinition def = FSMBuilder.semanticDef()
        .defaultFallback(DefaultHandlers.basicDefault(), DefaultHandlers.patternDefault());
      def = def
        .negativePort(R1TorrentCtrlPort.class)
        .basicEvent(R1TorrentCtrlEvents.Download.class)
        .subscribeOnStart(Handlers.download)
        .basicEvent(R1TorrentCtrlEvents.Upload.class)
        .subscribeOnStart(Handlers.upload)
        .buildEvents();
      def = def
        .positivePort(R1TorrentConnPort.class)
        .basicEvent(R1TorrentConnEvents.Seeders.class)
        .subscribe(Handlers.seeders, States.DOWNLOAD)
        .buildEvents();
      def = def
        .positivePort(DEndpointCtrlPort.class)
        .basicEvent(DEndpoint.Success.class)
        .subscribe(Handlers.endpointConnected, States.ENDPOINT)
        .buildEvents();
      def = def
        .positivePort(R1FileDownloadCtrl.class)
        .basicEvent(R1FileDownloadEvents.Indication.class)
        .subscribe(Handlers.downloadInd, States.DOWNLOAD)
        .basicEvent(R1FileDownloadEvents.Disconnected.class)
        .subscribe(Handlers.downloadDisc, States.DOWNLOAD)
        .buildEvents();
      def = def
        .positivePort(R1FileUploadCtrl.class)
        .basicEvent(R1FileUploadEvents.Indication.class)
        .subscribe(Handlers.uploadInd, States.DOWNLOAD)
        .basicEvent(R1FileUploadEvents.Disconnected.class)
        .subscribe(Handlers.uploadDisc, States.DOWNLOAD)
        .buildEvents();
      return def;
    }

    static BaseIdExtractor baseIdExtractor = new BaseIdExtractor() {

      @Override
      public Optional<Identifier> fromEvent(KompicsEvent event) throws FSMException {
        if (event instanceof Event) {
          Event e = (Event) event;
          return Optional.of(fsmBaseId(e.torrentId()));
        } else if (event instanceof DEndpoint.Indication) {
          DEndpoint.Indication e = (DEndpoint.Indication)event;
          return Optional.of(fsmBaseId(e.req.torrentId));
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

    static FSMBasicEventHandler upload = (FSMBasicEventHandler<ES, IS, R1TorrentCtrlEvents.Upload>) (
      FSMStateName state, ES es, IS is, R1TorrentCtrlEvents.Upload req) -> {
        is.upload(req.torrentId, req.torrentDetails);
        es.torrentDetailsMngr.addTorrent(req.torrentId, req.torrentDetails);
        LOG.info("<{}>upload", req.torrentId.baseId);
        is.torrentDetails.setup();
        sendEndpointConnect(es, is);
        return States.ENDPOINT;
      };

    static FSMBasicEventHandler download = (FSMBasicEventHandler<ES, IS, R1TorrentCtrlEvents.Download>) (
      FSMStateName state, ES es, IS is, R1TorrentCtrlEvents.Download req) -> {
        is.download(req.torrentId, req.torrentDetails, req.bootstrap);
        es.torrentDetailsMngr.addTorrent(req.torrentId, req.torrentDetails);
        LOG.info("<{}>download with partners:{}", new Object[]{is.torrentId.baseId, req.bootstrap});
        is.torrentDetails.setup();
        sendEndpointConnect(es, is);
        return States.ENDPOINT;
      };

    static FSMBasicEventHandler endpointConnected = (FSMBasicEventHandler<ES, IS, DEndpoint.Success>) (
      FSMStateName state, ES es, IS is, DEndpoint.Success req) -> {
        LOG.info("<{}>endpoint connected", new Object[]{is.torrentId.baseId});
        if (is.state.isLeft()) {
          is.torrentDetails.download();
          sendTorrentConn(es, new R1TorrentConnEvents.Bootstrap(is.torrentId, is.state.getLeft().bootstrap));
          return States.DOWNLOAD;
        } else {
          is.torrentDetails.upload();
          return States.UPLOAD;
        }
      };

    static FSMBasicEventHandler downloadInd = (FSMBasicEventHandler<ES, IS, R1FileDownloadEvents.Indication>) (
      FSMStateName state, ES es, IS is, R1FileDownloadEvents.Indication req) -> {
        LOG.info("{}download file:{} indication:{}", new Object[]{is.torrentId, req.fileId, req.state});
        switch (req.state) {
          case COMPLETED: {
            LOG.info("<{}>download file:{} completed", new Object[]{is.torrentId.baseId, req.fileId});
            is.torrentDetails.completed(req.fileId);
            Optional<KAddress> seeder = is.fileSeeders.completed(req.fileId);
            if (is.torrentDetails.isComplete()) {
              LOG.info("{}torrent completed", new Object[]{is.torrentId});
              return States.UPLOAD;
            }
            if (seeder.isPresent()) {
              startDownloadFile(es, is, seeder.get());
            }
            break;
          }
          case IDLE: {
            Optional<KAddress> fileSeeder = is.fileSeeders.getSeeder(req.fileId);
            if (!fileSeeder.isPresent()) {
              throw new RuntimeException("ups");
            }
            LOG.info("<{}>download file:{} connect:{}", new Object[]{is.torrentId.baseId, req.fileId, fileSeeder.get()});
            sendDownloadCtrl(es, new R1FileDownloadEvents.Connect(is.torrentId, req.fileId, fileSeeder.get()));
            break;
          }
        }
        return state;
      };

    static FSMBasicEventHandler downloadDisc = (FSMBasicEventHandler<ES, IS, R1FileDownloadEvents.Disconnected>) (
      FSMStateName state, ES es, IS is, R1FileDownloadEvents.Disconnected req) -> {
        LOG.info("{}download file:{} disc:{}", new Object[]{req.torrentId, req.fileId, req.seederId});
        return state;
      };

    static FSMBasicEventHandler uploadInd = (FSMBasicEventHandler<ES, IS, R1FileUploadEvents.Indication>) (
      FSMStateName state, ES es, IS is, R1FileUploadEvents.Indication req) -> {
        LOG.info("{}upload file:{} indication:{}", new Object[]{req.torrentId, req.fileId, req.state});
        return States.UPLOAD;
      };

    static FSMBasicEventHandler uploadDisc = (FSMBasicEventHandler<ES, IS, R1FileUploadEvents.Disconnected>) (
      FSMStateName state, ES es, IS is, R1FileUploadEvents.Disconnected req) -> {
        LOG.info("{}upload file:{} disc:{}", new Object[]{req.torrentId, req.fileId, req.leecherId});
        return States.UPLOAD;
      };

    static FSMBasicEventHandler seeders = (FSMBasicEventHandler<ES, IS, R1TorrentConnEvents.Seeders>) (
      FSMStateName state, ES es, IS is, R1TorrentConnEvents.Seeders req) -> {
        LOG.info("{}seeders:{}", new Object[]{req.torrentId, req.seeders});
        Set<KAddress> newSeeders = Sets.difference(req.seeders, is.fileSeeders.activeSeeders);
        for (KAddress seeder : newSeeders) {
          if (!startDownloadFile(es, is, seeder)) {
            break;
          }
        }
        return state;
      };

    static boolean startDownloadFile(ES es, IS is, KAddress seeder) {
      Optional<Identifier> fileId = is.torrentDetails.nextInactive();
      if (!fileId.isPresent()) {
        return false;
      }
      LOG.info("{}download file:{} start", new Object[]{is.torrentId, fileId.get()});
      is.fileSeeders.downloadFile(fileId.get(), seeder);
      is.torrentDetails.download(fileId.get());
      sendDownloadCtrl(es, new R1FileDownloadEvents.Start(is.torrentId, fileId.get()));
      return true;
    }

    static void sendEndpointConnect(ES es, IS is) {
      es.proxy.trigger(new DEndpoint.Connect(is.torrentId, is.torrentDetails.endpointId, is.torrentDetails.endpoint), 
        es.ports.endpointCtrl);
    }
    
    static void sendDownloadCtrl(ES es, R1FileDownload.CtrlEvent event) {
      es.proxy.trigger(event, es.ports.fileDownloadCtrlReq);
    }

    static void sendTorrentConn(ES es, KompicsEvent event) {
      es.proxy.trigger(event, es.ports.torrentConnReq);
    }
  }
}
