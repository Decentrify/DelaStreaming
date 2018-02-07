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
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.kompics.ClassMatchedHandler;
import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.ComponentProxy;
import se.sics.kompics.Handler;
import se.sics.kompics.KompicsEvent;
import se.sics.kompics.Positive;
import se.sics.kompics.Start;
import se.sics.kompics.network.Msg;
import se.sics.kompics.network.Network;
import se.sics.kompics.network.Transport;
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
import se.sics.ktoolbox.util.network.basic.BasicContentMsg;
import se.sics.ktoolbox.util.network.basic.BasicHeader;
import se.sics.nstream.util.BlockDetails;
import se.sics.silk.r2torrent.transfer.events.R1UploadEvents;
import se.sics.silk.r2torrent.transfer.events.R1UploadTimeout;
import se.sics.silk.r2torrent.transfer.msgs.R1TransferMsgs;
import se.sics.silk.r2torrent.transfer.util.R1UpldCwnd;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class R1UploadComp extends ComponentDefinition {

  private static final Logger LOG = LoggerFactory.getLogger(R1UploadComp.class);
  private String logPrefix;

  private final Ports ports;
  public final OverlayId torrentId;
  public final Identifier fileId;
  public final KAddress selfAdr;
  public final KAddress leecherAdr;
  private UUID timerId;

  private KContentMsg<?, ?, R1TransferMsgs.CacheHintReq> pendingCacheReq;
  private final R1UpldCwnd cwnd;

  public R1UploadComp(R1UploadComp.Init init) {
    ports = new Ports(proxy);
    selfAdr = init.selfAdr;
    leecherAdr = init.leecherAdr;
    torrentId = init.torrentId;
    fileId = init.fileId;
    cwnd = new R1UpldCwnd(init.defaultBlock, HardCodedConfig.cwndSize, sendMsg);
    subscribe(handleStart, control);
    subscribe(handleTimeout, ports.timer);
    subscribe(handleCache, ports.network);
    subscribe(handleHash, ports.network);
    subscribe(handleBlock, ports.network);
    subscribe(handlePiece, ports.network);
    subscribe(handleServeBlocks, ports.ctrl);
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

  @Override
  public void tearDown() {
    cancelTimer();
    cwnd.clear();
  }

  Handler handleTimeout = new Handler<R1UploadTimeout>() {
    @Override
    public void handle(R1UploadTimeout event) {
      cwnd.send();
    }
  };

  ClassMatchedHandler handleBlock
    = new ClassMatchedHandler<R1TransferMsgs.BlockReq, KContentMsg<KAddress, KHeader<KAddress>, R1TransferMsgs.BlockReq>>() {
      @Override
      public void handle(R1TransferMsgs.BlockReq content,
        KContentMsg<KAddress, KHeader<KAddress>, R1TransferMsgs.BlockReq> msg) {
        cwnd.pendingBlock(content);
      }
    };

  ClassMatchedHandler handlePiece
    = new ClassMatchedHandler<R1TransferMsgs.PieceReq, KContentMsg<KAddress, KHeader<KAddress>, R1TransferMsgs.PieceReq>>() {
      @Override
      public void handle(R1TransferMsgs.PieceReq content,
        KContentMsg<KAddress, KHeader<KAddress>, R1TransferMsgs.PieceReq> msg) {
        cwnd.pendingPiece(content);
      }
    };

  ClassMatchedHandler handleHash
    = new ClassMatchedHandler<R1TransferMsgs.HashReq, KContentMsg<KAddress, KHeader<KAddress>, R1TransferMsgs.HashReq>>() {
      @Override
      public void handle(R1TransferMsgs.HashReq content,
        KContentMsg<KAddress, KHeader<KAddress>, R1TransferMsgs.HashReq> msg) {
        answerMsg(msg, content.accept(cwnd.getHashes(content.hashes)));
      }
    };

  ClassMatchedHandler handleCache
    = new ClassMatchedHandler<R1TransferMsgs.CacheHintReq, KContentMsg<KAddress, KHeader<KAddress>, R1TransferMsgs.CacheHintReq>>() {

      @Override
      public void handle(R1TransferMsgs.CacheHintReq content,
        KContentMsg<KAddress, KHeader<KAddress>, R1TransferMsgs.CacheHintReq> msg) {
        LOG.trace("<{},{},{}>received:{}", 
          new Object[]{torrentId.baseId, fileId, leecherAdr.getId(), content});
        if (pendingCacheReq == null) {
          LOG.debug("<{},{},{}>cache:{} req - ts:{} blocks:{}", new Object[]{torrentId.baseId, fileId, 
            leecherAdr.getId(), content.getId(), content.cacheHint.lStamp, content.cacheHint.blocks});
          pendingCacheReq = msg;
          Set<Integer> servedBlocks = cwnd.servedBlocks();
          Set<Integer> newCache = Sets.difference(content.cacheHint.blocks, servedBlocks);
          Set<Integer> delCache = new HashSet<>(Sets.difference(servedBlocks, content.cacheHint.blocks));

          if (!newCache.isEmpty()) {
            KAddress leecher = msg.getHeader().getSource();
            trigger(new R1UploadEvents.BlocksReq(content.torrentId, content.fileId, leecher.getId(),
                newCache, content.cacheHint), ports.ctrl);
          } else {
            answerCacheHint();
          }
          //release references that were retained when given to us
          cwnd.releaseBlock(delCache);
        }
      }
    };

  Handler handleServeBlocks = new Handler<R1UploadEvents.BlocksResp>() {
    @Override
    public void handle(R1UploadEvents.BlocksResp resp) {
      //references are already retained by whoever gives them to us
      LOG.debug("{}serving blocks:{} hashes:{}", new Object[]{logPrefix, resp.blocks.keySet(), resp.hashes.keySet()});
      cwnd.serveBlocks(resp);
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

  private final Consumer<KompicsEvent> sendMsg = new Consumer<KompicsEvent>() {
    @Override
    public void accept(KompicsEvent payload) {
      KHeader header = new BasicHeader<>(selfAdr, leecherAdr, Transport.UDP);
      Msg msg = new BasicContentMsg(header, payload);
      trigger(msg, ports.network);
    }
  };

  public static class Init extends se.sics.kompics.Init<R1UploadComp> {

    public final KAddress selfAdr;
    public final OverlayId torrentId;
    public final Identifier fileId;
    public final KAddress leecherAdr;
    public final BlockDetails defaultBlock;

    public Init(KAddress selfAdr, OverlayId torrentId, Identifier fileId, KAddress lecherAdr,
      BlockDetails defaultBlock) {
      this.selfAdr = selfAdr;
      this.torrentId = torrentId;
      this.fileId = fileId;
      this.leecherAdr = lecherAdr;
      this.defaultBlock = defaultBlock;
    }
  }

  public static class Ports {

    public final Positive<R1UploadPort> ctrl;
    public final Positive<Network> network;
    public final Positive<Timer> timer;

    public Ports(ComponentProxy proxy) {
      ctrl = proxy.requires(R1UploadPort.class);
      network = proxy.requires(Network.class);
      timer = proxy.requires(Timer.class);
    }
  }

  public static class HardCodedConfig {

    public static final long timeoutPeriod = 100;
    public static int cwndSize = 100;
  }
}
