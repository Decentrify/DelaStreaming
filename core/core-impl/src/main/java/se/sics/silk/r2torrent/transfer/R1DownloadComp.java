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
package se.sics.silk.r2torrent.transfer;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.javatuples.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.kompics.ClassMatchedHandler;
import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.ComponentProxy;
import se.sics.kompics.Handler;
import se.sics.kompics.KompicsEvent;
import se.sics.kompics.Negative;
import se.sics.kompics.Positive;
import se.sics.kompics.Start;
import se.sics.kompics.network.Network;
import se.sics.kompics.network.Transport;
import se.sics.kompics.timer.CancelPeriodicTimeout;
import se.sics.kompics.timer.SchedulePeriodicTimeout;
import se.sics.kompics.timer.Timer;
import se.sics.kompics.util.Identifiable;
import se.sics.kompics.util.Identifier;
import se.sics.kompics.util.PatternExtractorHelper;
import se.sics.ktoolbox.util.identifiable.basic.PairIdentifier;
import se.sics.ktoolbox.util.identifiable.overlay.OverlayId;
import se.sics.ktoolbox.util.network.KAddress;
import se.sics.ktoolbox.util.network.KContentMsg;
import se.sics.ktoolbox.util.network.KHeader;
import se.sics.ktoolbox.util.network.basic.BasicContentMsg;
import se.sics.ktoolbox.util.network.basic.BasicHeader;
import se.sics.nstream.util.BlockDetails;
import se.sics.nutil.network.bestEffort.event.BestEffortMsg;
import se.sics.silk.r2torrent.torrent.util.R1FileMetadata;
import se.sics.silk.r2torrent.transfer.events.R1DownloadEvents;
import se.sics.silk.r2torrent.transfer.events.R1DownloadTimeout;
import se.sics.silk.r2torrent.transfer.msgs.R1TransferMsgs;
import se.sics.silk.r2torrent.transfer.util.R1DwnlCwnd;
import se.sics.silk.r2torrent.transfer.util.R1DownloadBlockTracker;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class R1DownloadComp extends ComponentDefinition {

  private static final Logger LOG = LoggerFactory.getLogger(R1DownloadComp.class);
  private String logPrefix = "";

  private final Ports ports;
  public final OverlayId torrentId;
  public final Identifier fileId;
  public final KAddress selfAdr;
  public final KAddress seederAdr;
  public final R1DownloadBlockTracker blockTracker;
  public final R1DwnlCwnd cwnd;
  private final R1FileMetadata fileMetadata;
  private UUID timerId;

  public R1DownloadComp(Init init) {
    ports = new Ports(proxy);
    selfAdr = init.selfAdr;
    seederAdr = init.seederAdr;
    torrentId = init.torrentId;
    fileId = init.fileId;
    this.fileMetadata = init.fileMetadata;
    blockTracker = new R1DownloadBlockTracker(fileMetadata.defaultBlock, true);
    cwnd = new R1DwnlCwnd(HardCodedConfig.CWND_SIZE);
    subscribe(handleStart, control);
    subscribe(handleNewBlocks, ports.ctrl);
    subscribe(handleNetworkTimeouts, ports.network);
    subscribe(handleCache, ports.network);
    subscribe(handleHash, ports.network);
    subscribe(handlePiece, ports.network);
    subscribe(handleTimeout, ports.timer);
  }

  public static Identifier baseId(OverlayId torrentId, Identifier fileId, Identifier seederId) {
    return new PairIdentifier(new PairIdentifier(torrentId, fileId), seederId);
  }

  Handler handleStart = new Handler<Start>() {

    @Override
    public void handle(Start event) {
      LOG.info("starting...");
      scheduleTimer();
    }
  };

  @Override
  public void tearDown() {
    cancelTimer();
  }

  private Handler handleNewBlocks = new Handler<R1DownloadEvents.GetBlocks>() {
    @Override
    public void handle(R1DownloadEvents.GetBlocks event) {
      LOG.trace("{}new blocks:{}", logPrefix, event.blocks);
      Map<Integer, BlockDetails> irregularBlocks = new HashMap<>();
      if (event.blocks.contains(fileMetadata.finalBlock)) {
        irregularBlocks.put(fileMetadata.finalBlock, fileMetadata.lastBlock);
      }
      blockTracker.add(event.blocks, irregularBlocks);
      tryDownload();
    }
  };

  ClassMatchedHandler handleCache
    = new ClassMatchedHandler<R1TransferMsgs.CacheHintAcc, KContentMsg<KAddress, KHeader<KAddress>, R1TransferMsgs.CacheHintAcc>>() {

      @Override
      public void handle(R1TransferMsgs.CacheHintAcc content,
        KContentMsg<KAddress, KHeader<KAddress>, R1TransferMsgs.CacheHintAcc> context) {
        LOG.debug("{}cache confirm ts:{}", logPrefix, content.cacheHint.lStamp);
        blockTracker.cacheConfirmed(content.cacheHint.lStamp);
        tryDownload();
      }
    };

  ClassMatchedHandler handleHash
    = new ClassMatchedHandler<R1TransferMsgs.HashResp, KContentMsg<KAddress, KHeader<KAddress>, R1TransferMsgs.HashResp>>() {

      @Override
      public void handle(R1TransferMsgs.HashResp content,
        KContentMsg<KAddress, KHeader<KAddress>, R1TransferMsgs.HashResp> context) {
        LOG.trace("{}received:{}", logPrefix, content);
        blockTracker.hashes(content.hashValues);
        tryDownload();
      }
    };

  ClassMatchedHandler handlePiece
    = new ClassMatchedHandler<R1TransferMsgs.PieceResp, KContentMsg<KAddress, KHeader<KAddress>, R1TransferMsgs.PieceResp>>() {

      @Override
      public void handle(R1TransferMsgs.PieceResp content,
        KContentMsg<KAddress, KHeader<KAddress>, R1TransferMsgs.PieceResp> context) {
        LOG.trace("{}received:{}", logPrefix, content);
        blockTracker.piece(content.piece, content.val.getRight());
        checkComplete();
        cwnd.receive(content.eventId, content.piece);
        tryDownload();
      }
    };

  ClassMatchedHandler handleNetworkTimeouts
    = new ClassMatchedHandler<BestEffortMsg.Timeout, KContentMsg<KAddress, KHeader<KAddress>, BestEffortMsg.Timeout>>() {
      @Override
      public void handle(BestEffortMsg.Timeout wrappedContent,
        KContentMsg<KAddress, KHeader<KAddress>, BestEffortMsg.Timeout> context) {
        Identifiable content = (Identifiable) PatternExtractorHelper.peelAllLayers(wrappedContent);
        if (content instanceof R1TransferMsgs.CacheHintReq) {
          R1TransferMsgs.CacheHintReq req = (R1TransferMsgs.CacheHintReq) wrappedContent.content;
          LOG.error("{}cache timeout on hint:{} blocks:{}", new Object[]{logPrefix, req.cacheHint.lStamp,
            req.cacheHint.blocks});
          throw new RuntimeException("ups");
        } else if (content instanceof R1TransferMsgs.PieceReq) {
          R1TransferMsgs.PieceReq req = (R1TransferMsgs.PieceReq) wrappedContent.content;
          blockTracker.pieceTimeout(req.piece);
          cwnd.timeout(req.eventId, req.piece);
        } else if (content instanceof R1TransferMsgs.HashReq) {
          R1TransferMsgs.HashReq req = (R1TransferMsgs.HashReq) wrappedContent.content;
          blockTracker.hashTimeout(req.hashes);
        } else {
          LOG.warn("{}!!!possible performance issue - fix:{}", logPrefix, content);
        }
      }
    };

  Handler handleTimeout = new Handler<R1DownloadTimeout>() {
    @Override
    public void handle(R1DownloadTimeout event) {
      tryDownload();
    }
  };

  private void checkComplete() {
    if (blockTracker.hasComplete()) {
      Map<Integer, Pair<byte[], byte[]>> completed = blockTracker.getComplete();
      LOG.trace("{}completed blocks:{}", new Object[]{logPrefix, completed.keySet()});
      completed.entrySet().stream().forEach((entry) -> {
        trigger(new R1DownloadEvents.Completed(torrentId, fileId, seederAdr.getId(), entry), ports.ctrl);
      });
    }
  }

  private void tryDownload() {
    if (blockTracker.hasNewHint()) {
      R1TransferMsgs.CacheHintReq req = new R1TransferMsgs.CacheHintReq(torrentId, fileId, blockTracker.newHint());
      LOG.debug("{}cache hint:{} ts:{} blocks:{}", new Object[]{logPrefix, req.getId(), req.cacheHint.lStamp,
        req.cacheHint.blocks});
      bestEffortMsg(req);
    }
    while (blockTracker.hasHashes() && cwnd.canSend()) {
      R1TransferMsgs.HashReq req = new R1TransferMsgs.HashReq(torrentId, fileId, blockTracker.nextHashes());
      bestEffortMsg(req);
    }
    int batch = 1 + HardCodedConfig.DWNL_INC_SPEED;
    while (blockTracker.hasMissedPiece() && cwnd.canSend() && batch > 0) {
      batch--;
      R1TransferMsgs.PieceReq req = new R1TransferMsgs.PieceReq(torrentId, fileId, blockTracker.nextMissedPiece());
      bestEffortMsg(req);
      cwnd.send(req.eventId, req.piece);
    }
    if(blockTracker.hasBlock() && batch > 0 && cwnd.canSend()) {
      Pair<Integer, Integer> block = blockTracker.nextBlock();
      R1TransferMsgs.BlockReq req 
        = new R1TransferMsgs.BlockReq(torrentId, fileId, block.getValue0(), block.getValue1());
      bestEffortMsg(req);
      cwnd.sendAll(req.eventId, block.getValue0(), block.getValue1());
    }
  }

  private <C extends KompicsEvent & Identifiable> void bestEffortMsg(C content) {
    KHeader header = new BasicHeader(selfAdr, seederAdr, Transport.UDP);
    BestEffortMsg.Request wrap
      = new BestEffortMsg.Request(content, HardCodedConfig.beRetries, HardCodedConfig.beRetryInterval);
    KContentMsg msg = new BasicContentMsg(header, wrap);
    trigger(msg, ports.network);
  }

  private void scheduleTimer() {
    SchedulePeriodicTimeout spt
      = new SchedulePeriodicTimeout(HardCodedConfig.timeoutPeriod, HardCodedConfig.timeoutPeriod);
    R1DownloadTimeout rt = new R1DownloadTimeout(spt);
    timerId = rt.getTimeoutId();
    spt.setTimeoutEvent(rt);
    trigger(spt, ports.timer);
  }

  private void cancelTimer() {
    if (timerId != null) {
      CancelPeriodicTimeout cpt = new CancelPeriodicTimeout(timerId);
      trigger(cpt, ports.timer);
      timerId = null;
    }
  }

  public static class Init extends se.sics.kompics.Init<R1DownloadComp> {

    public final KAddress selfAdr;
    public final OverlayId torrentId;
    public final Identifier fileId;
    public final KAddress seederAdr;
    public final R1FileMetadata fileMetadata;

    public Init(KAddress selfAdr, OverlayId torrentId, Identifier fileId, KAddress seederAdr,
      R1FileMetadata fileMetadata) {
      this.selfAdr = selfAdr;
      this.torrentId = torrentId;
      this.fileId = fileId;
      this.seederAdr = seederAdr;
      this.fileMetadata = fileMetadata;
    }
  }

  public static class Ports {

    public final Negative ctrl;
    public final Positive network;
    public final Positive timer;

    public Ports(ComponentProxy proxy) {
      ctrl = proxy.provides(R1DownloadPort.class);
      network = proxy.requires(Network.class);
      timer = proxy.requires(Timer.class);
    }
  }

  public static class HardCodedConfig {

    public static final int beRetries = 1;
    public static final int beRetryInterval = 2000;
    public static final int BATCHED_HASHES = 20;
    public static int CWND_SIZE = 50;
    public static final long timeoutPeriod = 10000;
    public static int DWNL_INC_SPEED = 1;
  }

};
