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
package se.sics.nstream.hops.libmngr;

import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.gvod.mngr.util.ElementSummary;
import se.sics.gvod.mngr.util.TorrentExtendedStatus;
import se.sics.kompics.ComponentProxy;
import se.sics.kompics.Handler;
import se.sics.kompics.Negative;
import se.sics.kompics.Positive;
import se.sics.kompics.config.Config;
import se.sics.ktoolbox.nutil.fsm.MultiFSM;
import se.sics.ktoolbox.nutil.fsm.genericsetup.OnFSMExceptionAction;
import se.sics.ktoolbox.util.identifiable.overlay.OverlayId;
import se.sics.ktoolbox.util.identifiable.overlay.OverlayIdFactory;
import se.sics.ktoolbox.util.network.KAddress;
import se.sics.ktoolbox.util.result.Result;
import se.sics.nstream.TorrentIds;
import se.sics.nstream.hops.library.HopsLibraryKConfig;
import se.sics.nstream.hops.library.HopsTorrentPort;
import se.sics.nstream.library.Library;
import se.sics.nstream.library.disk.LibrarySummaryHelper;
import se.sics.nstream.library.disk.LibrarySummaryJSON;
import se.sics.nstream.library.endpointmngr.EndpointIdRegistry;
import se.sics.nstream.library.event.torrent.HopsContentsEvent;
import se.sics.nstream.library.event.torrent.TorrentExtendedStatusEvent;
import se.sics.nstream.library.restart.TorrentRestart;
import se.sics.nstream.library.restart.TorrentRestartPort;
import se.sics.nstream.library.util.TorrentState;
import se.sics.nstream.torrent.tracking.TorrentStatusPort;
import se.sics.nstream.torrent.tracking.event.StatusSummaryEvent;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class HopsLibraryMngr {

  private static final Logger LOG = LoggerFactory.getLogger(HopsLibraryMngr.class);
  private String logPrefix = "";

  private final Config config;
  private final HopsLibraryKConfig hopsLibraryConfig;
  private final KAddress selfAdr;
  private final MultiFSM fsm;
  private final Library library;
  private final Restart restart;
  private final LibraryDetails libraryDetails;

  public HopsLibraryMngr(OnFSMExceptionAction oexa, ComponentProxy proxy, Config config, String logPrefix,
    KAddress selfAdr) {
    this.logPrefix = logPrefix;
    this.config = config;
    this.selfAdr = selfAdr;
    this.library = new Library(config.getValue("library.summary", String.class));
    this.restart = new Restart(proxy);
    this.libraryDetails = new LibraryDetails(proxy, library);

    hopsLibraryConfig = new HopsLibraryKConfig(config);
    fsm = LibTorrentFSM.getFSM(new LibTorrentFSM.LibTExternal(selfAdr, library, new EndpointIdRegistry(),
      hopsLibraryConfig.baseEndpointType), oexa);
    fsm.setProxy(proxy);
  }

  public void start() {
    //not sure when the provided ports are set, but for sure they are set after Start event. Ports are not set in constructor
    //TODO Alex - might lose some msg between Start and process of Start
    fsm.setupHandlers();
    restart.setup();
    restart.start(hopsLibraryConfig);
    libraryDetails.setup();
  }

  public void close() {
  }

  public static class Restart {

    private static final Logger LOG = LoggerFactory.getLogger(HopsLibraryMngr.class);
    private String logPrefix = "";

    private final ComponentProxy proxy;
    private Positive<TorrentRestartPort> restartPort;

    public Restart(ComponentProxy proxy) {
      this.proxy = proxy;
    }

    public void setup() {
      restartPort = proxy.getNegative(TorrentRestartPort.class).getPair();
      proxy.subscribe(handleDownloadRestartSuccess, restartPort);
      proxy.subscribe(handleDownloadRestartFail, restartPort);
      proxy.subscribe(handleUploadRestartSuccess, restartPort);
      proxy.subscribe(handleUploadRestartFail, restartPort);
    }

    public void start(HopsLibraryKConfig hopsLibraryConfig) {
      OverlayIdFactory torrentIdFactory = TorrentIds.torrentIdFactory();

      Result<LibrarySummaryJSON> librarySummary = LibrarySummaryHelper.readTorrentList(
        hopsLibraryConfig.librarySummaryPath);
      if (!librarySummary.isSuccess()) {
        throw new RuntimeException("TODO fix me - corrupted library");
      }
      Map<OverlayId, Library.Torrent> torrents
        = LibrarySummaryHelper.fromSummary(librarySummary.getValue(), TorrentIds.torrentIdFactory());

      for (Map.Entry<OverlayId, Library.Torrent> t : torrents.entrySet()) {
        if (t.getValue().getTorrentStatus().equals(TorrentState.UPLOADING)) {
          proxy.trigger(new TorrentRestart.UpldReq(t.getKey(), t.getValue().torrentName, t.getValue().projectId,
            t.getValue().getPartners(), t.getValue().getManifestStream()), restartPort);
        } else if (t.getValue().getTorrentStatus().equals(TorrentState.DOWNLOADING)) {
          proxy.trigger(new TorrentRestart.DwldReq(t.getKey(), t.getValue().torrentName, t.getValue().projectId,
            t.getValue().getPartners(), t.getValue().getManifestStream()), restartPort);
        }
      }
    }

    Handler handleDownloadRestartSuccess = new Handler<TorrentRestart.DwldSuccess>() {
      @Override
      public void handle(TorrentRestart.DwldSuccess resp) {
        LOG.info("{}restarted torrent:{}", logPrefix, resp.req.torrentId);
      }
    };

    Handler handleDownloadRestartFail = new Handler<TorrentRestart.DwldFail>() {
      @Override
      public void handle(TorrentRestart.DwldFail resp) {
        LOG.info("{}failed to restart torrent:{}", logPrefix, resp.req.torrentId);
      }
    };

    Handler handleUploadRestartSuccess = new Handler<TorrentRestart.UpldSuccess>() {
      @Override
      public void handle(TorrentRestart.UpldSuccess resp) {
        LOG.info("{}restarted torrent:{}", logPrefix, resp.req.torrentId);
      }
    };

    Handler handleUploadRestartFail = new Handler<TorrentRestart.UpldFail>() {
      @Override
      public void handle(TorrentRestart.UpldFail resp) {
        LOG.info("{}failed to restart torrent:{}", logPrefix, resp.req.torrentId);
      }
    };
  }

  public static class LibraryDetails {

    private static final Logger LOG = LoggerFactory.getLogger(HopsLibraryMngr.class);
    private String logPrefix = "";

    private final ComponentProxy proxy;
    private final Library library;
    private Negative<HopsTorrentPort> libraryCtrlPort;
    private Positive<TorrentStatusPort> torrentStatusPort;

    private final Map<OverlayId, TorrentExtendedStatusEvent.Request> pendingTESE = new HashMap<>();

    public LibraryDetails(ComponentProxy proxy, Library library) {
      this.proxy = proxy;
      this.library = library;
    }

    public void setup() {
      libraryCtrlPort = proxy.getPositive(HopsTorrentPort.class).getPair();
      torrentStatusPort = proxy.getNegative(TorrentStatusPort.class).getPair();
      proxy.subscribe(handleContents, libraryCtrlPort);
      proxy.subscribe(handleTorrentDetails, libraryCtrlPort);
      proxy.subscribe(handleTorrentStatus, torrentStatusPort);
    }

    Handler handleContents = new Handler<HopsContentsEvent.Request>() {

      @Override
      public void handle(HopsContentsEvent.Request req) {
        LOG.trace("{}received:{}", logPrefix, req);
        proxy.answer(req, req.success(library.getSummary(req.projectId)));
      }
    };

    private void torrentNotFound(OverlayId torrentId) {
      LOG.warn("{}torrent:{} not found", logPrefix, torrentId);
      for (ElementSummary es : library.getSummary()) {
        LOG.warn("{}found torrent:{}", logPrefix, es.torrentId);
      }
    }

    Handler handleTorrentDetails = new Handler<TorrentExtendedStatusEvent.Request>() {
      @Override
      public void handle(TorrentExtendedStatusEvent.Request req) {
        LOG.trace("{}received:{}", logPrefix, req);
        TorrentState ts = library.stateOf(req.torrentId);
        if (ts.equals(TorrentState.NONE)) {
          torrentNotFound(req.torrentId);
        }
        switch (ts) {
          case DOWNLOADING:
          case UPLOADING:
            pendingTESE.put(req.torrentId, req);
            proxy.trigger(new StatusSummaryEvent.Request(req.torrentId), torrentStatusPort);
            break;
          default:
            proxy.answer(req, req.succes(new TorrentExtendedStatus(req.torrentId, ts, 0, 0)));
        }
      }
    };

    Handler handleTorrentStatus = new Handler<StatusSummaryEvent.Response>() {
      @Override
      public void handle(StatusSummaryEvent.Response resp) {
        LOG.trace("{}received:{}", logPrefix, resp);
        TorrentExtendedStatusEvent.Request req = pendingTESE.remove(resp.req.torrentId);
        if (req == null) {
          return;
        }
        TorrentState ts = library.stateOf(req.torrentId);
        if (ts.equals(TorrentState.NONE)) {
          torrentNotFound(req.torrentId);
        }
        switch (ts) {
          case DOWNLOADING:
          case UPLOADING:
            proxy.answer(req, req.succes(new TorrentExtendedStatus(req.torrentId, resp.result.torrentStatus,
              resp.result.downloadSpeed, resp.result.percentageComplete)));
            break;
          default:
            proxy.answer(req, req.succes(new TorrentExtendedStatus(req.torrentId, ts, 0, 0)));
        }
      }
    };
  }
}
