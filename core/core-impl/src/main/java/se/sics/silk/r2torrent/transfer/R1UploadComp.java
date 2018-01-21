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

import com.google.common.collect.Sets;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.kompics.ClassMatchedHandler;
import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.ComponentProxy;
import se.sics.kompics.Handler;
import se.sics.kompics.Negative;
import se.sics.kompics.Positive;
import se.sics.kompics.Start;
import se.sics.kompics.network.Network;
import se.sics.kompics.timer.CancelPeriodicTimeout;
import se.sics.kompics.timer.SchedulePeriodicTimeout;
import se.sics.kompics.timer.Timer;
import se.sics.kompics.util.Identifiable;
import se.sics.kompics.util.Identifier;
import se.sics.ktoolbox.util.identifiable.basic.PairIdentifier;
import se.sics.ktoolbox.util.identifiable.overlay.OverlayId;
import se.sics.ktoolbox.util.network.KAddress;
import se.sics.ktoolbox.util.network.KContentMsg;
import se.sics.ktoolbox.util.network.KHeader;
import se.sics.ktoolbox.util.reference.KReference;
import se.sics.ktoolbox.util.reference.KReferenceException;
import se.sics.nstream.torrent.transfer.upld.event.GetBlocks;
import se.sics.nstream.util.BlockDetails;
import se.sics.nstream.util.BlockHelper;
import se.sics.nstream.util.range.KPiece;
import se.sics.nstream.util.range.RangeKReference;
import se.sics.silk.r2torrent.transfer.events.R1UploadEvents;
import se.sics.silk.r2torrent.transfer.events.R1UploadTimeout;
import se.sics.silk.r2torrent.transfer.msgs.R1TransferMsgs;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class R1UploadComp extends ComponentDefinition {

  private static final Logger LOG = LoggerFactory.getLogger(R1DownloadComp.class);
  private String logPrefix;

  private final Ports ports;
  public final OverlayId torrentId;
  public final Identifier fileId;
  public final KAddress selfAdr;
  public final KAddress seederAdr;
  public final BlockDetails defaultBlock;
  private UUID timerId;

  private final Map<Integer, BlockDetails> irregularBlocks = new HashMap<>();
  private final Map<Integer, KReference<byte[]>> servedBlocks = new HashMap<>();
  private final Map<Integer, byte[]> servedHashes = new HashMap<>();
  private KContentMsg<?, ?, R1TransferMsgs.CacheHintReq> pendingCacheReq;

  public R1UploadComp(R1DownloadComp.Init init) {
    ports = new Ports(proxy);
    selfAdr = init.selfAdr;
    seederAdr = init.seederAdr;
    torrentId = init.torrentId;
    fileId = init.fileId;
    defaultBlock = init.defaultBlock;
    subscribe(handleStart, control);
    subscribe(handleTimeout, ports.timer);
  }

  public static Identifier baseId(OverlayId torrentId, Identifier fileId, Identifier seederId) {
    return new PairIdentifier(new PairIdentifier(torrentId, fileId), seederId);
  }

  Handler handleStart = new Handler<Start>() {

    @Override
    public void handle(Start event) {
      scheduleTimer();
    }
  };

  public void tearDown() {
    cancelTimer();
    for (KReference<byte[]> block : servedBlocks.values()) {
      silentRelease(block);
    }
    servedBlocks.clear();
  }

  Handler handleTimeout = new Handler<R1UploadTimeout>() {
    @Override
    public void handle(R1UploadTimeout event) {
    }
  };

  ClassMatchedHandler handlePiece
    = new ClassMatchedHandler<R1TransferMsgs.PieceReq, KContentMsg<KAddress, KHeader<KAddress>, R1TransferMsgs.PieceReq>>() {
      @Override
      public void handle(R1TransferMsgs.PieceReq content,
        KContentMsg<KAddress, KHeader<KAddress>, R1TransferMsgs.PieceReq> msg) {

        int blockNr = content.piece.getValue0();
        BlockDetails blockDetails = irregularBlocks.containsKey(blockNr) ? irregularBlocks.get(blockNr) : defaultBlock;
        KReference<byte[]> block = servedBlocks.get(blockNr);
        if (block == null) {
          throw new RuntimeException("bad request");
        } else {
          KPiece pieceRange = BlockHelper.getPieceRange(content.piece, blockDetails, defaultBlock);
          //retain block here(range create) - release in serializer
          RangeKReference piece = RangeKReference.createInstance(block, BlockHelper.getBlockPos(blockNr, defaultBlock),
            pieceRange);
          answerMsg(msg, content.accept(piece));
        }
      }
    };

  ClassMatchedHandler handleHash
    = new ClassMatchedHandler<R1TransferMsgs.HashReq, KContentMsg<KAddress, KHeader<KAddress>, R1TransferMsgs.HashReq>>() {
      @Override
      public void handle(R1TransferMsgs.HashReq content,
        KContentMsg<KAddress, KHeader<KAddress>, R1TransferMsgs.HashReq> msg) {
        Map<Integer, byte[]> hashValues = new TreeMap<>();
        for (Integer hashNr : content.hashes) {
          byte[] hashVal = servedHashes.get(hashNr);
          if (hashVal == null) {
            LOG.warn("{}no hash for:{} - not serving incomplete", logPrefix, hashNr);
            LOG.warn("{}no hash - serving blocks:{}", logPrefix, servedBlocks.keySet());
            LOG.warn("{}no hash - serving hashes:{}", logPrefix, servedHashes.keySet());
            throw new RuntimeException("bad request");
          }
          hashValues.put(hashNr, hashVal);
        }
        answerMsg(msg, content.accept(hashValues));
      }
    };

  ClassMatchedHandler handleCache
    = new ClassMatchedHandler<R1TransferMsgs.CacheHintReq, KContentMsg<KAddress, KHeader<KAddress>, R1TransferMsgs.CacheHintReq>>() {

      @Override
      public void handle(R1TransferMsgs.CacheHintReq content, KContentMsg<KAddress, KHeader<KAddress>, R1TransferMsgs.CacheHintReq> msg) {
        LOG.trace("{}received:{}", new Object[]{logPrefix, content});
        if (pendingCacheReq == null) {
          LOG.debug("{}cache:{} req - ts:{} blocks:{}",
            new Object[]{logPrefix, content.getId(), content.cacheHint.lStamp, content.cacheHint.blocks});
          pendingCacheReq = msg;
          Set<Integer> newCache = Sets.difference(content.cacheHint.blocks, servedBlocks.keySet());
          Set<Integer> delCache = new HashSet<>(Sets.difference(servedBlocks.keySet(), content.cacheHint.blocks));

          if (!newCache.isEmpty()) {
            KAddress leecher = msg.getHeader().getSource();
            trigger(new R1UploadEvents.BlocksReq(content.torrentId, content.fileId, leecher.getId(), 
              newCache, content.cacheHint), ports.ctrl);
          } else {
            answerCacheHint();
          }
          //release references that were retained when given to us
          for (Integer blockNr : delCache) {
            KReference<byte[]> block = servedBlocks.remove(blockNr);
            servedHashes.remove(blockNr);
            irregularBlocks.remove(blockNr);
            silentRelease(block);
          }
        }
      }
    };

  Handler handleGetBlocks = new Handler<GetBlocks.Response>() {
    @Override
    public void handle(GetBlocks.Response resp) {
      //references are already retained by whoever gives them to us
      LOG.debug("{}serving blocks:{} hashes:{}", new Object[]{logPrefix, resp.blocks.keySet(), resp.hashes.keySet()});
      servedBlocks.putAll(resp.blocks);
      servedHashes.putAll(resp.hashes);
      irregularBlocks.putAll(resp.irregularBlocks);
      answerCacheHint();
    }
  };

  private void answerCacheHint() {
    answerMsg(pendingCacheReq, pendingCacheReq.getContent().accept());
    pendingCacheReq = null;
  }
  
  private void answerMsg(KContentMsg original, Identifiable respContent) {
    LOG.trace("{}answering with:{}", logPrefix, respContent);
    trigger(original.answer(respContent), ports.network);
  }

  private void silentRelease(KReference<byte[]> ref) {
    try {
      ref.release();
    } catch (KReferenceException ex) {
      throw new RuntimeException(ex);
    }
  }

  private void scheduleTimer() {
    SchedulePeriodicTimeout spt
      = new SchedulePeriodicTimeout(HardCodedConfig.timeoutPeriod, HardCodedConfig.timeoutPeriod);
    R1UploadTimeout rt = new R1UploadTimeout(spt);
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

  public static class Init extends se.sics.kompics.Init<R1UploadComp> {

    public final KAddress selfAdr;
    public final OverlayId torrentId;
    public final Identifier fileId;
    public final KAddress seederAdr;
    public final int blockSlots;
    public final BlockDetails defaultBlock;

    public Init(KAddress selfAdr, OverlayId torrentId, Identifier fileId, KAddress seederAdr, int blockSlots,
      BlockDetails defaultBlock) {
      this.selfAdr = selfAdr;
      this.torrentId = torrentId;
      this.fileId = fileId;
      this.seederAdr = seederAdr;
      this.blockSlots = blockSlots;
      this.defaultBlock = defaultBlock;
    }
  }

  public static class Ports {

    public final Negative ctrl;
    public final Positive network;
    public final Positive timer;

    public Ports(ComponentProxy proxy) {
      ctrl = proxy.provides(R1UploadPort.class);
      network = proxy.requires(Network.class);
      timer = proxy.requires(Timer.class);
    }
  }

  public static class HardCodedConfig {

    public static final long timeoutPeriod = 10000;
  }
}
