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

import com.google.common.collect.Sets;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.cobweb.transfer.handle.event.SeederHandleCtrlE;
import se.sics.cobweb.transfer.handle.event.SeederHandleE;
import se.sics.cobweb.transfer.handle.msg.HandleM;
import se.sics.cobweb.transfer.handle.util.SeederActivityReport;
import se.sics.cobweb.transfer.handlemngr.SeederHandleCreator;
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
import se.sics.ktoolbox.util.identifiable.Identifiable;
import se.sics.ktoolbox.util.identifiable.Identifier;
import se.sics.ktoolbox.util.identifiable.overlay.OverlayId;
import se.sics.ktoolbox.util.network.KAddress;
import se.sics.ktoolbox.util.network.KContentMsg;
import se.sics.ktoolbox.util.network.KHeader;
import se.sics.ktoolbox.util.reference.KReference;
import se.sics.ktoolbox.util.reference.KReferenceException;
import se.sics.ledbat.ncore.msg.LedbatMsg;
import se.sics.nstream.util.BlockDetails;
import se.sics.nstream.util.BlockHelper;
import se.sics.nstream.util.range.KPiece;
import se.sics.nstream.util.range.RangeKReference;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class SeederHandleComp extends ComponentDefinition {

  private static final Logger LOG = LoggerFactory.getLogger(LeecherHandleComp.class);
  private String logPrefix;

  Negative<SeederHandlePort> handlePort = provides(SeederHandlePort.class);
  Negative<SeederHandleCtrlPort> handleCtrlPort = provides(SeederHandleCtrlPort.class);
  Positive<Network> networkPort = requires(Network.class);

  private final OverlayId torrentId;
  private final HandleId handleId;
  private final KAddress selfAdr;
  private BlockDetails defaultBlock;
  private boolean withHashes;
  //**************************************************************************
  private final Map<Integer, BlockDetails> irregularBlocks = new HashMap<>();
  private final Map<Integer, KReference<byte[]>> servedBlocks = new HashMap<>();
  private final Map<Integer, byte[]> servedHashes = new HashMap<>();
  private KContentMsg<?, ?, HandleM.CacheHintReq> pendingCacheReq;
  private int sentPieces = 0;
  //**************************************************************************
  private SeederHandleCtrlE.Shutdown shutdownReq;
  private Map<Identifier, SeederHandleE.Shutdown> shutdownReqs = new HashMap<>();

  public SeederHandleComp(Init init) {
    torrentId = init.torrentId;
    handleId = init.handleId;
    selfAdr = init.selfAdr;
    logPrefix = handleId.toString();

    subscribe(handleStart, control);
    subscribe(handleSetup, handlePort);
    subscribe(handleShutdown, handleCtrlPort);
    subscribe(handleBlocks, handlePort);
    subscribe(handleShutdownAck, handlePort);
    subscribe(handleCache, networkPort);
    subscribe(handleLedbat, networkPort);
  }

  private void unsubShutdown() {
    subscribe(handleBlocksShutdown, handlePort);
    unsubscribe(handleBlocks, handlePort);
    unsubscribe(handleCache, networkPort);
    unsubscribe(handleLedbat, networkPort);
  }
  Handler handleStart = new Handler<Start>() {
    @Override
    public void handle(Start event) {
      LOG.info("{}starting", logPrefix);
    }
  };

  @Override
  public void tearDown() {
    LOG.info("{}tear down", logPrefix);
    for (KReference<byte[]> block : servedBlocks.values()) {
      silentRelease(block);
    }
    servedBlocks.clear();
  }
  //********************************************************************************************************************
  Handler handleSetup = new Handler<SeederHandleE.SetupResp>() {
    @Override
    public void handle(SeederHandleE.SetupResp resp) {
      LOG.trace("{}{}", logPrefix, resp);
      defaultBlock = resp.defaultBlockDetails;
      withHashes = resp.withHashes;
      trigger(new SeederHandleCtrlE.Ready(torrentId, handleId), handleCtrlPort);
    }
  };

  Handler handleShutdown = new Handler<SeederHandleCtrlE.Shutdown>() {
    @Override
    public void handle(SeederHandleCtrlE.Shutdown event) {
      LOG.trace("{}{}", logPrefix, event);
      shutdownReq = event;
      unsubShutdown();
      SeederHandleE.Shutdown req = new SeederHandleE.Shutdown(torrentId, handleId);
      shutdownReqs.put(req.eventId, req);
      trigger(req, handlePort);
      report();
    }
  };

  Handler handleShutdownAck = new Handler<SeederHandleE.ShutdownAck>() {
    @Override
    public void handle(SeederHandleE.ShutdownAck resp) {
      LOG.trace("{}{}", logPrefix, resp);
      shutdownReqs.remove(resp.eventId);
      if (shutdownReqs.isEmpty()) {
        answer(shutdownReq, shutdownReq.success());
      }
    }
  };

  Handler handleBlocksShutdown = new Handler<SeederHandleE.BlocksResp>() {
    @Override
    public void handle(SeederHandleE.BlocksResp resp) {
      LOG.trace("{}{}", logPrefix, resp);
      for (KReference ref : resp.blocks.values()) {
        silentRelease(ref);
      }
    }
  };

  private void report() {
    SeederActivityReport activity = new SeederActivityReport(sentPieces);
    sentPieces = 0;
    SeederHandleCtrlE.Report rep = new SeederHandleCtrlE.Report(torrentId, handleId, activity);
    trigger(rep, handleCtrlPort);
  }
  //********************************************************************************************************************
  ClassMatchedHandler handleCache
    = new ClassMatchedHandler<HandleM.CacheHintReq, KContentMsg<KAddress, KHeader<KAddress>, HandleM.CacheHintReq>>() {

      @Override
      public void handle(HandleM.CacheHintReq content,
        KContentMsg<KAddress, KHeader<KAddress>, HandleM.CacheHintReq> context) {
        LOG.trace("{}received:{}", new Object[]{logPrefix, content});
        if (pendingCacheReq == null) {
          LOG.debug("{}cache:{} req - ts:{} blocks:{}",
            new Object[]{logPrefix, content.getId(), content.requestCache.lStamp, content.requestCache.blocks});
          pendingCacheReq = context;
          Set<Integer> newCache = Sets.difference(content.requestCache.blocks, servedBlocks.keySet());
          Set<Integer> delCache = new HashSet<>(Sets.difference(servedBlocks.keySet(), content.requestCache.blocks));

          if (!newCache.isEmpty()) {
            trigger(new SeederHandleE.BlocksReq(torrentId, handleId, newCache, withHashes, content.requestCache),
              handlePort);
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
          report();
        }
      }
    };

  Handler handleBlocks = new Handler<SeederHandleE.BlocksResp>() {
    @Override
    public void handle(SeederHandleE.BlocksResp resp) {
      //references are already retained by whoever gives them to us
      LOG.debug("{}serving blocks:{} hashes:{}", new Object[]{logPrefix, resp.blocks.keySet(), resp.hashes.keySet()});
      servedBlocks.putAll(resp.blocks);
      servedHashes.putAll(resp.hashes);
      irregularBlocks.putAll(resp.irregularBlocks);
      answerCacheHint();
    }
  };

  private void answerCacheHint() {
    answerMsg(pendingCacheReq, pendingCacheReq.getContent().success());
    pendingCacheReq = null;
  }
  //********************************************************************************************************************
  ClassMatchedHandler handleLedbat
    = new ClassMatchedHandler<LedbatMsg.Request, KContentMsg<KAddress, KHeader<KAddress>, LedbatMsg.Request>>() {
      @Override
      public void handle(LedbatMsg.Request content, KContentMsg<KAddress, KHeader<KAddress>, LedbatMsg.Request> context) {
        Object baseContent = content.getWrappedContent();
        if (baseContent instanceof HandleM.DwnlPieceReq) {
          handlePiece(context, content);
        } else if (baseContent instanceof HandleM.DwnlHashReq) {
          handleHashes(context, content);
        } else {
          LOG.error("{}received:{}", logPrefix, content);
          throw new RuntimeException("ups");
        }
      }
    };

  public void handlePiece(KContentMsg msg, LedbatMsg.Request<HandleM.DwnlPieceReq> content) {
    int blockNr = content.getWrappedContent().piece.getValue0();
    BlockDetails blockDetails = irregularBlocks.containsKey(blockNr) ? irregularBlocks.get(blockNr) : defaultBlock;
    KReference<byte[]> block = servedBlocks.get(blockNr);
    if (block == null) {
      LedbatMsg.Response ledbatContent = content.answer(content.getWrappedContent().fault());
      answerMsg(msg, ledbatContent);
    } else {
      KPiece pieceRange = BlockHelper.getPieceRange(content.getWrappedContent().piece, blockDetails, defaultBlock);
      //retain block here(range create) - release in serializer
      RangeKReference piece = RangeKReference.createInstance(block, BlockHelper.getBlockPos(blockNr, defaultBlock),
        pieceRange);
      LedbatMsg.Response ledbatContent = content.answer(content.getWrappedContent().success(piece));
      answerMsg(msg, ledbatContent);
      sentPieces++;
    }
  }

  public void handleHashes(KContentMsg msg, LedbatMsg.Request<HandleM.DwnlHashReq> content) {
    Map<Integer, byte[]> hashValues = new TreeMap<>();
    for (Integer hashNr : content.getWrappedContent().hashes) {
      byte[] hashVal = servedHashes.get(hashNr);
      if (hashVal == null) {
        LOG.warn("{}no hash for:{} - not serving incomplete", logPrefix, hashNr);
        LOG.warn("{}no hash - serving blocks:{}", logPrefix, servedBlocks.keySet());
        LOG.warn("{}no hash - serving hashes:{}", logPrefix, servedHashes.keySet());
        LedbatMsg.Response ledbatContent = content.answer(content.getWrappedContent().fault());
        answerMsg(msg, ledbatContent);
        return;
      }
      hashValues.put(hashNr, hashVal);
    }
    LedbatMsg.Response ledbatContent = content.answer(content.getWrappedContent().success(hashValues));
    answerMsg(msg, ledbatContent);
  }

  //********************************************************************************************************************

  private void answerMsg(KContentMsg original, Identifiable respContent) {
    LOG.trace("{}answering with:{}", logPrefix, respContent);
    trigger(original.answer(respContent), networkPort);
  }

  private void silentRelease(KReference<byte[]> ref) {
    try {
      ref.release();
    } catch (KReferenceException ex) {
      throw new RuntimeException(ex);
    }
  }

  public static class Init extends se.sics.kompics.Init<SeederHandleComp> {

    public final OverlayId torrentId;
    public final HandleId handleId;
    public final KAddress selfAdr;
    public final KAddress leecherAdr;

    public Init(OverlayId torrentId, HandleId handleId, KAddress selfAdr, KAddress leecherAdr) {
      this.torrentId = torrentId;
      this.handleId = handleId;
      this.selfAdr = selfAdr;
      this.leecherAdr = leecherAdr;
    }
  }
  
  public static final DefaultCreator DEFAULT_CREATOR = new DefaultCreator();
  public static class DefaultCreator implements SeederHandleCreator {

    @Override
    public Component connect(ComponentProxy proxy, OverlayId torrentId, HandleId handleId, KAddress selfAdr,
      KAddress leecherAdr) {
      SeederHandleComp.Init init = new SeederHandleComp.Init(torrentId, handleId, selfAdr, leecherAdr);
      return proxy.create(SeederHandleComp.class, init);
    }

  }
}
