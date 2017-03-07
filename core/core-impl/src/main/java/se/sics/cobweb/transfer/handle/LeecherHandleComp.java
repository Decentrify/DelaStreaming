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
package se.sics.cobweb.transfer.handle;

import com.google.common.base.Optional;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.javatuples.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.cobweb.transfer.handle.event.LeecherHandleCtrlE;
import se.sics.cobweb.transfer.handle.event.LeecherHandleE;
import se.sics.cobweb.transfer.handle.msg.HandleM;
import se.sics.cobweb.transfer.handle.util.LeecherActivityReport;
import se.sics.cobweb.transfer.handlemngr.LeecherHandleCreator;
import se.sics.cobweb.util.HandleId;
import se.sics.kompics.ClassMatchedHandler;
import se.sics.kompics.Component;
import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.ComponentProxy;
import se.sics.kompics.Handler;
import se.sics.kompics.Negative;
import se.sics.kompics.Positive;
import se.sics.kompics.Start;
import se.sics.kompics.network.Network;
import se.sics.kompics.network.Transport;
import se.sics.ktoolbox.util.config.impl.SystemKCWrapper;
import se.sics.ktoolbox.util.identifiable.Identifiable;
import se.sics.ktoolbox.util.identifiable.Identifier;
import se.sics.ktoolbox.util.identifiable.overlay.OverlayId;
import se.sics.ktoolbox.util.network.KAddress;
import se.sics.ktoolbox.util.network.KContentMsg;
import se.sics.ktoolbox.util.network.KHeader;
import se.sics.ktoolbox.util.network.basic.BasicContentMsg;
import se.sics.ktoolbox.util.network.basic.BasicHeader;
import se.sics.ledbat.core.AppCongestionWindow;
import se.sics.ledbat.core.LedbatConfig;
import se.sics.ledbat.ncore.msg.LedbatMsg;
import se.sics.nstream.torrent.transfer.DwnlConnConfig;
import se.sics.nstream.torrent.transfer.DwnlConnWorkCtrl;
import se.sics.nstream.torrent.transfer.tracking.DwnlConnTracker;
import se.sics.nutil.ContentWrapperHelper;
import se.sics.nutil.network.bestEffort.event.BestEffortMsg;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class LeecherHandleComp extends ComponentDefinition {

  private static final Logger LOG = LoggerFactory.getLogger(LeecherHandleComp.class);
  private String logPrefix;

  private static final long CACHE_BASE_TIMEOUT = 1000;
  private static final int CACHE_RETRY = 30;
  private final LedbatConfig ledbatConfig;
  //********************************************************************************************************************
  Negative<LeecherHandlePort> handlePort = provides(LeecherHandlePort.class);
  Negative<LeecherHandleCtrlPort> handleCtrlPort = provides(LeecherHandleCtrlPort.class);
  Positive<Network> networkPort = requires(Network.class);
  //********************************************************************************************************************
  private final OverlayId torrentId;
  private final HandleId handleId;
  private List<KAddress> selfAdr;
  private List<KAddress> seederAdr;
  private int parallelPorts;
  //**************************************************************************
//  private final NetworkQueueLoadProxy networkQueueLoad;
  private final AppCongestionWindow cwnd;
  private DwnlConnWorkCtrl workController;
  private final Optional<DwnlConnTracker> tracker;
  private int maxActiveBlocks;
  private int requestedBlocks;
  //**************************************************************************
  private UUID cacheTid;
  private final Map<Identifier, Identifiable> pendingMsgs = new HashMap<>();
  //**************************************************************************
  private LeecherHandleCtrlE.Shutdown shutdownReq;
  private final Map<Identifier, LeecherHandleE.Shutdown> shutdownReqs = new HashMap<>();

  public LeecherHandleComp(Init init) {
    torrentId = init.torrentId;
    handleId = init.handleId;
    logPrefix = "<" + handleId.toString() + ">";

    parallelPorts(init);
    DwnlConnConfig dConfig = new DwnlConnConfig(config());

    ledbatConfig = new LedbatConfig(config());
//    networkQueueLoad = NetworkQueueLoadProxy.instance("load_dwnl_" + logPrefix, proxy, config(), dConfig.reportDir);
    cwnd = new AppCongestionWindow(ledbatConfig, handleId, dConfig.minRTO, dConfig.reportDir);
    maxActiveBlocks = init.activeBlocks;

    if (dConfig.reportDir.isPresent()) {
      tracker = Optional.fromNullable(DwnlConnTracker.onDisk(dConfig.reportDir.get(), handleId, parallelPorts));
    } else {
      tracker = Optional.absent();
    }

    subscribe(handleStart, control);
    subscribe(handleCwndControl, handleCtrlPort);
    subscribe(handleMemSet, handleCtrlPort);
    subscribe(handleShutdown, handleCtrlPort);
    subscribe(handleSetup, handlePort);
    subscribe(handleDownload, handlePort);
    subscribe(handleShutdownAck, handlePort);
    subscribe(handleNetworkTimeouts, networkPort);
    subscribe(handleCache, networkPort);
    subscribe(handleLedbat, networkPort);
  }

  private void unsubShutdown() {
    unsubscribe(handleCwndControl, handleCtrlPort);
    subscribe(handleDownloadShutdown, handlePort);
    unsubscribe(handleDownload, handlePort);
    unsubscribe(handleNetworkTimeouts, networkPort);
    unsubscribe(handleCache, networkPort);
    unsubscribe(handleLedbat, networkPort);
  }

  private void parallelPorts(Init init) {
    //TODO Alex - CRITICAL - multi port expects consecutive ports - use 1 for now
    SystemKCWrapper sc = new SystemKCWrapper(config());
    if (sc.parallelPorts.isPresent()) {
      parallelPorts = sc.parallelPorts.get();
    } else {
      parallelPorts = 1;
    }
    selfAdr = new ArrayList<>(parallelPorts);
    seederAdr = new ArrayList<>(parallelPorts);
    selfAdr.add(0, init.selfAdr);
    seederAdr.add(0, init.seederAdr);

    for (int i = 1; i < parallelPorts; i++) {
      KAddress s = init.selfAdr.withPort(init.selfAdr.getPort() + i);
      selfAdr.add(i, s);
      KAddress t = init.seederAdr.withPort(init.seederAdr.getPort() + i);
      seederAdr.add(i, t);
      LOG.info("{}setting s:{} t:{}", new Object[]{logPrefix, s, t});
    }
  }

  Handler handleStart = new Handler<Start>() {
    @Override
    public void handle(Start event) {
      LOG.info("{}starting conn to:{}", logPrefix, seederAdr);
//      networkQueueLoad.start();
      trigger(new LeecherHandleE.SetupReq(torrentId, handleId), handlePort);
    }
  };

  @Override
  public void tearDown() {
    LOG.info("{}tear down", logPrefix);
    for (Identifiable msg : pendingMsgs.values()) {
      cancelMsg(msg);
    }
//    networkQueueLoad.tearDown();
    cwnd.close();
    if (tracker.isPresent()) {
      tracker.get().close();
    }
  }
  //********************************************************************************************************************
  Handler handleCwndControl = new Handler<LeecherHandleCtrlE.CwndControl>() {
    @Override
    public void handle(LeecherHandleCtrlE.CwndControl event) {
      LOG.error("{}fpd", logPrefix);
      double networkQueueLoadAdjustemnt = Integer.MAX_VALUE;
//      networkQueueLoadAdjustemnt = networkQueueLoad.adjustment()
      double adjustment = Math.min(event.appCwndAdjustment, networkQueueLoadAdjustemnt);
      long now = System.currentTimeMillis();
      cwnd.adjustState(now, adjustment);
    }
  };

  Handler handleMemSet = new Handler<LeecherHandleCtrlE.MemSet>() {

    @Override
    public void handle(LeecherHandleCtrlE.MemSet event) {
      LOG.trace("{}{}", logPrefix, event);
      LOG.info("{}mem set:{}", logPrefix, event.maxActiveBlocks);
      maxActiveBlocks = event.maxActiveBlocks;
    }
  };

  Handler handleShutdown = new Handler<LeecherHandleCtrlE.Shutdown>() {
    @Override
    public void handle(LeecherHandleCtrlE.Shutdown event) {
      LOG.trace("{}{}", logPrefix, event);
      shutdownReq = event;
      unsubShutdown();
      LeecherHandleE.Shutdown req = new LeecherHandleE.Shutdown(torrentId, handleId, workController.allBlocks());
      trigger(req, handlePort);
      shutdownReqs.put(req.eventId, req);
    }
  };

  Handler handleShutdownAck = new Handler<LeecherHandleE.ShutdownAck>() {
    @Override
    public void handle(LeecherHandleE.ShutdownAck resp) {
      LOG.trace("{}{}", logPrefix, resp);
      shutdownReqs.remove(resp.eventId);
      if (shutdownReqs.isEmpty()) {
        answer(shutdownReq, shutdownReq.success());
      }
    }
  };

  Handler handleDownloadShutdown = new Handler<LeecherHandleE.Download>() {
    @Override
    public void handle(LeecherHandleE.Download event) {
      LOG.trace("{}shutdown new blocks:{}", logPrefix, event.blocks);
      LeecherHandleE.Shutdown req = new LeecherHandleE.Shutdown(torrentId, handleId, event.blocks);
      trigger(req, handlePort);
      shutdownReqs.put(req.eventId, req);
    }
  };

  private void report(int completedBlocks) {
    LeecherActivityReport rep = new LeecherActivityReport(completedBlocks, maxActiveBlocks, requestedBlocks);
    trigger(new LeecherHandleCtrlE.Report(torrentId, handleId, rep), handleCtrlPort);
  }
  //********************************************************************************************************************
  Handler handleSetup = new Handler<LeecherHandleE.SetupResp>() {
    @Override
    public void handle(LeecherHandleE.SetupResp resp) {
      LOG.trace("{}{}", logPrefix, resp);
      workController = new DwnlConnWorkCtrl(resp.defaultBlockDetails, resp.withHashes);
      trigger(new LeecherHandleCtrlE.Ready(torrentId, handleId), handleCtrlPort);
      completed();
    }
  };

  Handler handleDownload = new Handler<LeecherHandleE.Download>() {
    @Override
    public void handle(LeecherHandleE.Download event) {
      LOG.trace("{}new blocks:{}", logPrefix, event.blocks);
      workController.add(event.blocks, event.irregularBlocks);
      requestedBlocks -= event.requestedBlocks;
      report(0);
    }
  };

  private void completed() {
    Pair<Map<Integer, byte[]>, Map<Integer, byte[]>> completed = workController.getComplete();
    int activeBlocks = workController.allBlocksSize();
    int requestBlocks = maxActiveBlocks - activeBlocks + requestedBlocks;
    if (requestBlocks < 0) {
      requestBlocks = 0;
    }
    requestedBlocks += requestBlocks;
    LOG.debug("{}completed hashes:{} blocks:{}", new Object[]{logPrefix, completed.getValue0().keySet(),
      completed.getValue1().keySet()});
    trigger(new LeecherHandleE.Completed(torrentId, handleId, completed.getValue0(), completed.getValue1(),
      requestBlocks), handlePort);
    report(completed.getValue1().size());

  }
  //********************************************************************************************************************
  ClassMatchedHandler handleNetworkTimeouts
    = new ClassMatchedHandler<BestEffortMsg.Timeout, KContentMsg<KAddress, KHeader<KAddress>, BestEffortMsg.Timeout>>() {
      @Override
      public void handle(BestEffortMsg.Timeout wrappedContent,
        KContentMsg<KAddress, KHeader<KAddress>, BestEffortMsg.Timeout> context) {
        Identifiable content = ContentWrapperHelper.getBaseContent(wrappedContent, Identifiable.class);
        if (content instanceof HandleM.CacheHintReq) {
          handleCacheTimeout((HandleM.CacheHintReq) content);
        } else if (content instanceof HandleM.DwnlPieceReq) {
          handlePieceTimeout((HandleM.DwnlPieceReq) content);
          reportTimeout(System.currentTimeMillis(), content, wrappedContent.req.rto);
        } else if (content instanceof HandleM.DwnlHashReq) {
          handleHashTimeout((HandleM.DwnlHashReq) content);
          reportTimeout(System.currentTimeMillis(), content, wrappedContent.req.rto);
        } else {
          LOG.warn("{}!!!possible performance issue - fix:{}", logPrefix, content);
        }
      }
    };

  public void handleCacheTimeout(HandleM.CacheHintReq req) {
    LOG.error("{}cache timeout on hint:{} blocks:{}", new Object[]{logPrefix, req.requestCache.lStamp,
      req.requestCache.blocks});
    throw new RuntimeException("ups");
  }

  public void handlePieceTimeout(HandleM.DwnlPieceReq req) {
    LOG.debug("{}piece timeout:<{},{}>", new Object[]{logPrefix, req.piece.getValue0(), req.piece.getValue1()});
    if (pendingMsgs.remove(req.eventId) != null) {
      workController.pieceTimeout(req.piece);

    }
    long now = System.currentTimeMillis();
    cwnd.timeout(now, ledbatConfig.mss);
    tryDownload(now);
  }

  public void handleHashTimeout(HandleM.DwnlHashReq req) {
    LOG.debug("{}hash timeout:<{}>", new Object[]{logPrefix, req.hashes});
    if (pendingMsgs.remove(req.eventId) != null) {
      workController.hashTimeout(req.hashes);
    }
    long now = System.currentTimeMillis();
    cwnd.timeout(now, ledbatConfig.mss);
    tryDownload(now);
  }
  //********************************************************************************************************************
  ClassMatchedHandler handleCache
    = new ClassMatchedHandler<HandleM.CacheHintSuccess, KContentMsg<KAddress, KHeader<KAddress>, HandleM.CacheHintSuccess>>() {

      @Override
      public void handle(HandleM.CacheHintSuccess content,
        KContentMsg<KAddress, KHeader<KAddress>, HandleM.CacheHintSuccess> context) {
        HandleM.CacheHintReq req = (HandleM.CacheHintReq) pendingMsgs.remove(content.getId());
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
        if (baseContent instanceof HandleM.DwnlPieceSuccess) {
          handlePiece(content);
        } else if (baseContent instanceof HandleM.DwnlHashSuccess) {
          handleHash(content);
        } else if (baseContent instanceof HandleM.DwnlPieceFault) {
          LOG.warn("{}dropping bad request:{} - if this is due to retransmission - it should be fine", logPrefix,
            baseContent);
          return;
        } else if (baseContent instanceof HandleM.DwnlHashFault) {
          LOG.warn("{}dropping bad request:{} - if this is due to retransmission - it should be fine", logPrefix,
            baseContent);
          return;
        } else {
          throw new RuntimeException("ups");
        }
      }
    };

  private void handlePiece(LedbatMsg.Response<HandleM.DwnlPieceSuccess> content) {
    LOG.trace("{}received:{}", logPrefix, content);
    HandleM.DwnlPieceSuccess resp = content.getWrappedContent();
    long now = System.currentTimeMillis();
    if (pendingMsgs.remove(resp.eventId) != null) {
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
      completed();
    }
  }

  private void handleHash(LedbatMsg.Response<HandleM.DwnlHashSuccess> content) {
    LOG.trace("{}received:{}", logPrefix, content);
    HandleM.DwnlHashSuccess resp = content.getWrappedContent();
    long now = System.currentTimeMillis();
    if (pendingMsgs.remove(resp.eventId) != null) {
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
      completed();
    }
  }

  //********************************************************************************************************************
  private void tryDownload(long now) {
    if (workController.hasNewHint()) {
      HandleM.CacheHintReq req = new HandleM.CacheHintReq(torrentId, handleId, workController.newHint());
      LOG.debug("{}cache hint:{} ts:{} blocks:{}", new Object[]{logPrefix, req.getId(), req.requestCache.lStamp,
        req.requestCache.blocks});
      sendSimpleUDP(req, CACHE_RETRY, CACHE_BASE_TIMEOUT);
      pendingMsgs.put(req.getId(), req);
    }
    while (workController.hasHashes() && cwnd.canSend()) {
      HandleM.DwnlHashReq req = new HandleM.DwnlHashReq(torrentId, handleId, workController.nextHashes());
      sendSimpleLedbat(req, 5);
      pendingMsgs.put(req.getId(), req);
      cwnd.request(now, ledbatConfig.mss);
    }
    int batch = 2;
    while (workController.hasPiece() && cwnd.canSend() && batch > 0) {
      batch--;
      HandleM.DwnlPieceReq req = new HandleM.DwnlPieceReq(torrentId, handleId, workController.nextPiece());
      sendSimpleLedbat(req, 1);
      pendingMsgs.put(req.getId(), req);
      cwnd.request(now, ledbatConfig.mss);
    }
  }

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

  private Pair<KAddress, KAddress> getSrcDst(Identifiable content) {
    int portOffset = content.getId().partition(parallelPorts);
    return Pair.with(selfAdr.get(portOffset), seederAdr.get(portOffset));
  }

  private void cancelMsg(Identifiable content) {
    BestEffortMsg.Cancel wrappedContent = new BestEffortMsg.Cancel<>(content);
    Pair<KAddress, KAddress> srcDst = getSrcDst(content);
    KHeader header = new BasicHeader(srcDst.getValue0(), srcDst.getValue1(), Transport.UDP);
    KContentMsg msg = new BasicContentMsg<>(header, wrappedContent);
    LOG.trace("{}canceling:{}", logPrefix, content);
    trigger(msg, networkPort);
  }

  public static class Init extends se.sics.kompics.Init<LeecherHandleComp> {

    public final OverlayId torrentId;
    public final HandleId handleId;
    public final KAddress selfAdr;
    public final KAddress seederAdr;
    public final int activeBlocks;

    public Init(OverlayId torrentId, HandleId handleId, KAddress selfAdr, KAddress seederAdr, int activeBlocks) {
      this.torrentId = torrentId;
      this.handleId = handleId;
      this.selfAdr = selfAdr;
      this.seederAdr = seederAdr;
      this.activeBlocks = activeBlocks;
    }
  }

  public static final DefaultCreator DEFAULT_CREATOR = new DefaultCreator();
  public static class DefaultCreator implements LeecherHandleCreator {

    @Override
    public Component connect(ComponentProxy proxy, OverlayId torrentId, HandleId handleId, KAddress selfAdr,
      KAddress seederAdr, int activeBlocks) {
      LeecherHandleComp.Init init = new LeecherHandleComp.Init(torrentId, handleId, selfAdr, seederAdr, activeBlocks);
      return proxy.create(LeecherHandleComp.class, init);
    }
  }
}
