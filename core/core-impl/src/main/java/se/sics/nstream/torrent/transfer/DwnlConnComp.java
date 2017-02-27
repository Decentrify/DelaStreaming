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
package se.sics.nstream.torrent.transfer;

import com.google.common.base.Optional;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.javatuples.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.kompics.ClassMatchedHandler;
import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.Handler;
import se.sics.kompics.Negative;
import se.sics.kompics.Positive;
import se.sics.kompics.Start;
import se.sics.kompics.network.Network;
import se.sics.kompics.network.Transport;
import se.sics.kompics.timer.CancelPeriodicTimeout;
import se.sics.kompics.timer.SchedulePeriodicTimeout;
import se.sics.kompics.timer.Timeout;
import se.sics.kompics.timer.Timer;
import se.sics.ktoolbox.util.config.impl.SystemKCWrapper;
import se.sics.ktoolbox.util.identifiable.Identifiable;
import se.sics.ktoolbox.util.identifiable.Identifier;
import se.sics.ktoolbox.util.network.KAddress;
import se.sics.ktoolbox.util.network.KContentMsg;
import se.sics.ktoolbox.util.network.KHeader;
import se.sics.ktoolbox.util.network.basic.BasicContentMsg;
import se.sics.ktoolbox.util.network.basic.BasicHeader;
import se.sics.ledbat.core.AppCongestionWindow;
import se.sics.ledbat.core.DownloadThroughput;
import se.sics.ledbat.core.LedbatConfig;
import se.sics.ledbat.ncore.msg.LedbatMsg;
import se.sics.nstream.ConnId;
import se.sics.nstream.old.torrent.event.TorrentTimeout;
import se.sics.nstream.torrent.old.TransferConfig;
import se.sics.nstream.torrent.transfer.dwnl.event.CompletedBlocks;
import se.sics.nstream.torrent.transfer.dwnl.event.DownloadBlocks;
import se.sics.nstream.torrent.transfer.dwnl.event.FPDControl;
import se.sics.nstream.torrent.transfer.msg.CacheHint;
import se.sics.nstream.torrent.transfer.msg.DownloadHash;
import se.sics.nstream.torrent.transfer.msg.DownloadPiece;
import se.sics.nstream.torrent.transfer.tracking.DownloadTrackingReport;
import se.sics.nstream.torrent.transfer.tracking.DownloadTrackingTrace;
import se.sics.nstream.torrent.transfer.tracking.DwnlConnTracker;
import se.sics.nstream.torrent.transfer.tracking.TransferTrackingPort;
import se.sics.nstream.torrent.transfer.tracking.event.TrackingConnection;
import se.sics.nstream.util.BlockDetails;
import se.sics.nutil.ContentWrapperHelper;
import se.sics.nutil.network.bestEffort.event.BestEffortMsg;
import se.sics.nutil.tracking.load.NetworkQueueLoadProxy;

/**
 * A DwnlConnComp per torrent per peer per file per dwnl (there is an equivalent
 * comp for upld)
 *
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class DwnlConnComp extends ComponentDefinition {

  private static final Logger LOG = LoggerFactory.getLogger(DwnlConnComp.class);
  private String logPrefix;

  private static final long ADVANCE_DOWNLOAD = 1000;
  private static final long CACHE_BASE_TIMEOUT = 1000;
  private static final int CACHE_RETRY = 30;
  private static final long REPORT_PERIOD = 200;
  //**************************************************************************
  Negative<DwnlConnPort> connPort = provides(DwnlConnPort.class);
  Negative<TransferTrackingPort> reportPort = provides(TransferTrackingPort.class);
  Positive<Network> networkPort = requires(Network.class);
  Positive<Timer> timerPort = requires(Timer.class);
  //**************************************************************************
  private final ConnId connId;
  private final List<KAddress> self;
  private final List<KAddress> target;
  private final int parallelPorts;
  //**************************************************************************
  private final NetworkQueueLoadProxy networkQueueLoad;
  private final AppCongestionWindow cwnd;
  private final DwnlConnWorkCtrl workController;
  private final Optional<DwnlConnTracker> tracker;
  //**************************************************************************
  private UUID advanceDownloadTid;
  private UUID cacheTid;
  private UUID reportTid;
  private final Map<Identifier, Identifiable> pendingMsgs = new HashMap<>();
  //**************************************************************************
  private final LedbatConfig ledbatConfig;

  public DwnlConnComp(Init init) {
    connId = init.connId;

    SystemKCWrapper sc = new SystemKCWrapper(config());
    if (sc.parallelPorts.isPresent()) {
      parallelPorts = sc.parallelPorts.get();
    } else {
      parallelPorts = 1;
    }
    self = new ArrayList<>(parallelPorts);
    target = new ArrayList<>(parallelPorts);
    self.add(0, init.self);
    target.add(0, init.target);
    for (int i = 1; i < parallelPorts; i++) {
      KAddress s = init.self.withPort(init.self.getPort() + i);
      self.add(i, s);
      KAddress t = init.target.withPort(init.target.getPort() + i);
      target.add(i, t);
      LOG.info("{}setting s:{} t:{}", new Object[]{logPrefix, s, t});
    }
    logPrefix = "<" + connId.toString() + ">";

    DwnlConnConfig dConfig = new DwnlConnConfig(config());

    ledbatConfig = new LedbatConfig(config());
    networkQueueLoad = NetworkQueueLoadProxy.instance("load_dwnl_" + logPrefix, proxy, config(), dConfig.reportDir);
    cwnd = new AppCongestionWindow(ledbatConfig, connId, dConfig.minRTO, dConfig.reportDir);
    workController = new DwnlConnWorkCtrl(init.defaultBlockDetails, init.withHashes);

    if (dConfig.reportDir.isPresent()) {
      tracker = Optional.fromNullable(DwnlConnTracker.onDisk(dConfig.reportDir.get(), connId, parallelPorts));
    } else {
      tracker = Optional.absent();
    }

    subscribe(handleStart, control);
    subscribe(handleReport, timerPort);
    subscribe(handleAdvanceDownload, timerPort);
    subscribe(handleFPDControl, connPort);
    subscribe(handleNewBlocks, connPort);
    subscribe(handleNetworkTimeouts, networkPort);
    subscribe(handleCache, networkPort);
    subscribe(handleLedbat, networkPort);
  }

  Handler handleStart = new Handler<Start>() {
    @Override
    public void handle(Start event) {
      LOG.info("{}starting conn to:{}", logPrefix, target);
      networkQueueLoad.start();
      scheduleAdvanceDownload();
      scheduleReport();
    }
  };

  @Override
  public void tearDown() {
    LOG.info("{}tear down", logPrefix);
    for (Identifiable msg : pendingMsgs.values()) {
      cancelMsg(msg);
    }
    networkQueueLoad.tearDown();
    cancelAdvanceDownload();
    cancelReport();
    cwnd.close();
    if (tracker.isPresent()) {
      tracker.get().close();
    }
    trigger(new TrackingConnection.Close(connId), reportPort);
  }

  Handler handleReport = new Handler<ReportTimeout>() {
    @Override
    public void handle(ReportTimeout event) {
      LOG.trace("{}reporting", logPrefix);
      Pair<Integer, Integer> queueDelay = networkQueueLoad.queueDelay();
      DownloadThroughput downloadThroughput = cwnd.report();
      DownloadTrackingTrace trace = new DownloadTrackingTrace(downloadThroughput, workController.blockSize(), cwnd.
        cwnd());
      trigger(new DownloadTrackingReport(connId, trace), reportPort);
    }
  };

  Handler handleAdvanceDownload = new Handler<TorrentTimeout.AdvanceDownload>() {
    @Override
    public void handle(TorrentTimeout.AdvanceDownload event) {
      LOG.error("{}advance download", logPrefix);
      long now = System.currentTimeMillis();
      cwnd.adjustState(now, networkQueueLoad.adjustment());
      tryDownload(now);
    }
  };

  Handler handleFPDControl = new Handler<FPDControl>() {
    @Override
    public void handle(FPDControl event) {
      LOG.error("{}fpd", logPrefix);
      double adjustment = Math.min(event.appCwndAdjustment, networkQueueLoad.adjustment());
      long now = System.currentTimeMillis();
      cwnd.adjustState(now, adjustment);
    }
  };

  //**********************************FPD*************************************
  private Handler handleNewBlocks = new Handler<DownloadBlocks>() {
    @Override
    public void handle(DownloadBlocks event) {
      LOG.trace("{}new blocks:{}", logPrefix, event.blocks);
      workController.add(event.blocks, event.irregularBlocks);
    }
  };
  //**************************************************************************
  ClassMatchedHandler handleNetworkTimeouts
    = new ClassMatchedHandler<BestEffortMsg.Timeout, KContentMsg<KAddress, KHeader<KAddress>, BestEffortMsg.Timeout>>() {
      @Override
      public void handle(BestEffortMsg.Timeout wrappedContent,
        KContentMsg<KAddress, KHeader<KAddress>, BestEffortMsg.Timeout> context) {
        Identifiable content = ContentWrapperHelper.getBaseContent(wrappedContent, Identifiable.class);
        if (content instanceof CacheHint.Request) {
          handleCacheTimeout((CacheHint.Request) content);
        } else if (content instanceof DownloadPiece.Request) {
          handlePieceTimeout((DownloadPiece.Request) content);
          reportTimeout(System.currentTimeMillis(), content, wrappedContent.req.rto);
        } else if (content instanceof DownloadHash.Request) {
          handleHashTimeout((DownloadHash.Request) content);
          reportTimeout(System.currentTimeMillis(), content, wrappedContent.req.rto);
        } else {
          LOG.warn("{}!!!possible performance issue - fix:{}", logPrefix, content);
        }
      }
    };

  public void handleCacheTimeout(CacheHint.Request req) {
    LOG.error("{}cache timeout on hint:{} blocks:{}", new Object[]{logPrefix, req.requestCache.lStamp,
      req.requestCache.blocks});
    throw new RuntimeException("ups");
  }

  public void handlePieceTimeout(DownloadPiece.Request req) {
    LOG.debug("{}piece timeout:<{},{}>", new Object[]{logPrefix, req.piece.getValue0(), req.piece.getValue1()});
    if (pendingMsgs.remove(req.msgId) != null) {
      workController.pieceTimeout(req.piece);

    }
    long now = System.currentTimeMillis();
    cwnd.timeout(now, ledbatConfig.mss);
    tryDownload(now);
  }

  public void handleHashTimeout(DownloadHash.Request req) {
    LOG.debug("{}hash timeout:<{}>", new Object[]{logPrefix, req.hashes});
    if (pendingMsgs.remove(req.msgId) != null) {
      workController.hashTimeout(req.hashes);
    }
    long now = System.currentTimeMillis();
    cwnd.timeout(now, ledbatConfig.mss);
    tryDownload(now);
  }

  //**************************************************************************
  private Pair<KAddress, KAddress> getSrcDst(Identifiable content) {
    int portOffset = content.getId().partition(parallelPorts);
    return Pair.with(self.get(portOffset), target.get(portOffset));
  }

  private void cancelMsg(Identifiable content) {
    BestEffortMsg.Cancel wrappedContent = new BestEffortMsg.Cancel<>(content);
    Pair<KAddress, KAddress> srcDst = getSrcDst(content);
    KHeader header = new BasicHeader(srcDst.getValue0(), srcDst.getValue1(), Transport.UDP);
    KContentMsg msg = new BasicContentMsg<>(header, wrappedContent);
    LOG.trace("{}canceling:{}", logPrefix, content);
    trigger(msg, networkPort);
  }

  private void sendSimpleUDP(Identifiable content, int retries, long rto) {
    BestEffortMsg.Request wrappedContent = new BestEffortMsg.Request<>(content, retries, rto);
    Pair<KAddress, KAddress> srcDst = getSrcDst(content);
    KHeader header = new BasicHeader(srcDst.getValue0(), srcDst.getValue1(), Transport.UDP);
    KContentMsg msg = new BasicContentMsg<>(header, wrappedContent);
    LOG.trace("{}sending:{}", logPrefix, content);

    trigger(msg, networkPort);
    reportPortEvent(content);
  }

  private void sendSimpleLedbat(Identifiable content, int retries) {
    LedbatMsg.Request ledbatContent = new LedbatMsg.Request(content);
    sendSimpleUDP(ledbatContent, retries, cwnd.getRTT());
  }

  ClassMatchedHandler handleCache
    = new ClassMatchedHandler<CacheHint.Response, KContentMsg<KAddress, KHeader<KAddress>, CacheHint.Response>>() {

      @Override
      public void handle(CacheHint.Response content,
        KContentMsg<KAddress, KHeader<KAddress>, CacheHint.Response> context) {
        CacheHint.Request req = (CacheHint.Request) pendingMsgs.remove(content.getId());
        if (req != null) {
          LOG.debug("{}cache confirm ts:{}", logPrefix, req.requestCache.lStamp);
          workController.cacheConfirmed(req.requestCache.lStamp);
          tryDownload(System.currentTimeMillis());
        }
      }
    };

  ClassMatchedHandler handleLedbat
    = new ClassMatchedHandler<LedbatMsg.Response, KContentMsg<KAddress, KHeader<KAddress>, LedbatMsg.Response>>() {

      @Override
      public void handle(LedbatMsg.Response content,
        KContentMsg<KAddress, KHeader<KAddress>, LedbatMsg.Response> context) {
        Object baseContent = content.getWrappedContent();
        if (baseContent instanceof DownloadPiece.Success) {
          handlePiece(content);
        } else if (baseContent instanceof DownloadHash.Success) {
          handleHash(content);
        } else if (baseContent instanceof DownloadPiece.BadRequest) {
          LOG.warn("{}dropping bad request:{} - if this is due to retransmission - it should be fine", logPrefix,
            baseContent);
          return;
        } else if (baseContent instanceof DownloadHash.BadRequest) {
          LOG.warn("{}dropping bad request:{} - if this is due to retransmission - it should be fine", logPrefix,
            baseContent);
          return;
        } else {
          throw new RuntimeException("ups");
        }
      }
    };

  private void handlePiece(LedbatMsg.Response<DownloadPiece.Success> content) {
    LOG.trace("{}received:{}", logPrefix, content);
    DownloadPiece.Success resp = content.getWrappedContent();
    long now = System.currentTimeMillis();
    if (pendingMsgs.remove(resp.msgId) != null) {
      workController.piece(resp.piece, resp.val.getRight());
      cwnd.success(now, ledbatConfig.mss, content);
      tryDownload(now);
    } else {
      LOG.debug("{}late piece:<{},{}>", new Object[]{logPrefix, resp.piece.getValue0(), resp.piece.getValue1()});
      workController.latePiece(resp.piece, resp.val.getRight());
      cwnd.late(now, ledbatConfig.mss, content);
      reportLate(System.currentTimeMillis(), content);
    }
    if (workController.hasComplete()) {
      Pair<Map<Integer, byte[]>, Map<Integer, byte[]>> completed = workController.getComplete();
      LOG.debug("{}completed hashes:{} blocks:{}", new Object[]{logPrefix, completed.getValue0().keySet(), completed.
        getValue1().keySet()});
      trigger(new CompletedBlocks(connId, completed.getValue0(), completed.getValue1()), connPort);
    }
  }

  private void handleHash(LedbatMsg.Response<DownloadHash.Success> content) {
    LOG.trace("{}received:{}", logPrefix, content);
    DownloadHash.Success resp = content.getWrappedContent();
    long now = System.currentTimeMillis();
    if (pendingMsgs.remove(resp.msgId) != null) {
      workController.hashes(resp.hashValues);
      cwnd.success(now, ledbatConfig.mss, content);
      tryDownload(now);
    } else {
      LOG.debug("{}late hashes", new Object[]{logPrefix});
      workController.lateHashes(resp.hashValues);
      cwnd.late(now, ledbatConfig.mss, content);
      reportLate(System.currentTimeMillis(), content);
    }
    if (workController.hasComplete()) {
      Pair<Map<Integer, byte[]>, Map<Integer, byte[]>> completed = workController.getComplete();
      LOG.trace("{}completed hashes:{} blocks:{}", new Object[]{logPrefix, completed.getValue0().keySet(), completed.
        getValue1().keySet()});
      trigger(new CompletedBlocks(connId, completed.getValue0(), completed.getValue1()), connPort);
    }
  }

  //**************************************************************************
  private void tryDownload(long now) {
    if (workController.hasNewHint()) {
      CacheHint.Request req = new CacheHint.Request(connId.fileId, workController.newHint());
      LOG.debug("{}cache hint:{} ts:{} blocks:{}", new Object[]{logPrefix, req.getId(), req.requestCache.lStamp,
        req.requestCache.blocks});
      sendSimpleUDP(req, CACHE_RETRY, CACHE_BASE_TIMEOUT);
      pendingMsgs.put(req.getId(), req);
    }
    while (workController.hasHashes() && cwnd.canSend()) {
      DownloadHash.Request req = new DownloadHash.Request(connId.fileId, workController.nextHashes());
      sendSimpleLedbat(req, 5);
      pendingMsgs.put(req.getId(), req);
      cwnd.request(now, ledbatConfig.mss);
    }
    int batch =2;
    while (workController.hasPiece() && cwnd.canSend() && batch > 0) {
      batch--;
      DownloadPiece.Request req = new DownloadPiece.Request(connId.fileId, workController.nextPiece());
      sendSimpleLedbat(req, 1);
      pendingMsgs.put(req.getId(), req);
      cwnd.request(now, ledbatConfig.mss);
    }
  }

  //**************************************************************************
  private void reportTimeout(long now, Identifiable event, long rto) {
    if (tracker.isPresent()) {
      tracker.get().reportTimeout(now, event.getId(), rto);
    }
  }

  private void reportLate(long now, LedbatMsg.Response late) {
    if (tracker.isPresent()) {
      tracker.get().reportLate(now, late);

    }
  }

  private void reportPortEvent(Identifiable content) {
    if (tracker.isPresent()) {
      int portOffset = content.getId().partition(parallelPorts);
      tracker.get().reportPortEvent(System.currentTimeMillis(), portOffset);
    }
  }

  public static class Init extends se.sics.kompics.Init<DwnlConnComp> {

    public final ConnId connId;
    public final KAddress self;
    public final KAddress target;
    public final BlockDetails defaultBlockDetails;
    public final boolean withHashes;

    public Init(ConnId connId, KAddress self, KAddress target,
      BlockDetails defaultBlockDetails, boolean withHashes) {
      this.connId = connId;
      this.self = self;
      this.target = target;
      this.defaultBlockDetails = defaultBlockDetails;
      this.withHashes = withHashes;
    }
  }

  private void scheduleAdvanceDownload() {
    SchedulePeriodicTimeout spt = new SchedulePeriodicTimeout(TransferConfig.advanceDownloadPeriod,
      TransferConfig.advanceDownloadPeriod);
    TorrentTimeout.AdvanceDownload tt = new TorrentTimeout.AdvanceDownload(spt);
    spt.setTimeoutEvent(tt);
    advanceDownloadTid = tt.getTimeoutId();
    trigger(spt, timerPort);
  }

  private void cancelAdvanceDownload() {
    CancelPeriodicTimeout cpd = new CancelPeriodicTimeout(advanceDownloadTid);
    trigger(cpd, timerPort);
    advanceDownloadTid = null;
  }

  private void scheduleReport() {
    SchedulePeriodicTimeout spt = new SchedulePeriodicTimeout(REPORT_PERIOD, REPORT_PERIOD);
    ReportTimeout rt = new ReportTimeout(spt);
    spt.setTimeoutEvent(rt);
    reportTid = rt.getTimeoutId();
    trigger(spt, timerPort);
  }

  private void cancelReport() {
    CancelPeriodicTimeout cpd = new CancelPeriodicTimeout(reportTid);
    trigger(cpd, timerPort);
    reportTid = null;

  }

  public static class ReportTimeout extends Timeout {

    public ReportTimeout(SchedulePeriodicTimeout spt) {
      super(spt);
    }
  }
};
