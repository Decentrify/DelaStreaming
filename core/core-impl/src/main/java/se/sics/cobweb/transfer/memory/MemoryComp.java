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
package se.sics.cobweb.transfer.memory;

import com.google.common.base.Optional;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.cobweb.transfer.handle.LeecherHandlePort;
import se.sics.cobweb.transfer.handle.SeederHandlePort;
import se.sics.cobweb.transfer.handle.event.LeecherHandleE;
import se.sics.cobweb.transfer.handle.event.SeederHandleE;
import se.sics.cobweb.transfer.memory.event.MemoryE;
import se.sics.cobweb.util.FileDesc;
import se.sics.cobweb.util.FileId;
import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.Handler;
import se.sics.kompics.Negative;
import se.sics.kompics.Positive;
import se.sics.kompics.Start;
import se.sics.ktoolbox.util.identifiable.overlay.OverlayId;
import se.sics.ktoolbox.util.network.KAddress;
import se.sics.ktoolbox.util.reference.KReference;
import se.sics.ktoolbox.util.reference.KReferenceFactory;
import se.sics.nstream.util.BlockDetails;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class MemoryComp extends ComponentDefinition {

  private static final Logger LOG = LoggerFactory.getLogger(MemoryComp.class);
  private final String logPrefix;

  private final static Optional<KAddress> NONE = Optional.absent();
  private final boolean HASHES = false;

  private final Negative<MemoryPort> memoryPort = provides(MemoryPort.class);
  private final Positive<LeecherHandlePort> leecherPort = requires(LeecherHandlePort.class);
  private final Positive<SeederHandlePort> seederPort = requires(SeederHandlePort.class);
  //********************************************************************************************************************
  private final OverlayId torrentId;
  private final FileId fileId;
  private final FileDesc fileDesc;
  //********************************************************************************************************************
  //TODO Alex - improve - this is fast for manifest
  private final TreeMap<Integer, KReference<byte[]>> blocks = new TreeMap<>();
  private boolean started = false;

  public MemoryComp(Init init) {
    torrentId = init.torrentId;
    fileId = init.fileId;
    fileDesc = init.fileDesc;
    logPrefix = fileId.toString();

    subscribe(handleStart, control);
    subscribe(handleGetBlock, seederPort);
    subscribe(handleSeederShutdown, seederPort);
    subscribe(handleLeecherSetup, leecherPort);
    subscribe(handleBlockCompleted, leecherPort);
    subscribe(handleLeecherShutdown, leecherPort);
  }

  Handler handleStart = new Handler<Start>() {

    @Override
    public void handle(Start event) {
      LOG.info("{}start", logPrefix);
    }
  };

  @Override
  public void tearDown() {
    LOG.info("{}tear down", logPrefix);

  }
  //********************************************************************************************************************
  Handler handleSeederShutdown = new Handler<SeederHandleE.Shutdown>() {
    @Override
    public void handle(SeederHandleE.Shutdown req) {
      LOG.trace("{}{}", logPrefix, req);
      answer(req, req.ack());
    }
  };

  Handler handleLeecherShutdown = new Handler<LeecherHandleE.Shutdown>() {
    @Override
    public void handle(LeecherHandleE.Shutdown req) {
      LOG.trace("{}{}", logPrefix, req);
      answer(req, req.ack());
    }
  };

  Handler handleLeecherSetup = new Handler<LeecherHandleE.SetupReq>() {
    @Override
    public void handle(LeecherHandleE.SetupReq req) {
      LOG.trace("{}{}", logPrefix, req);
      answer(req, req.answer(fileDesc.defaultBlock, HASHES));
    }
  };
  //********************************************************************************************************************
  Handler handleGetBlock = new Handler<SeederHandleE.BlocksReq>() {

    @Override
    public void handle(SeederHandleE.BlocksReq req) {
      LOG.trace("{}{}", logPrefix, req);
      if (HASHES) {
        throw new RuntimeException("fix me");
      }
      Map<Integer, byte[]> hashes = new TreeMap<>();
      Map<Integer, KReference<byte[]>> blockValues = new TreeMap<>();
      Map<Integer, BlockDetails> irregularBlocks = new TreeMap<>();

      int lastBlockNr = fileDesc.nrBlocks - 1;
      if (req.blocks.contains(lastBlockNr)) {
        irregularBlocks.put(lastBlockNr, fileDesc.lastBlock);
      }
      for (Integer blockNr : req.blocks) {
        KReference<byte[]> ref = blocks.get(blockNr);
        blockValues.put(blockNr, ref);
        ref.retain();
      }
      answer(req, req.success(hashes, blockValues, irregularBlocks));

      if (req.withHashes) {
        throw new RuntimeException("ups");
      }
    }
  };

  Handler handleBlockCompleted = new Handler<LeecherHandleE.Completed>() {
    @Override
    public void handle(LeecherHandleE.Completed resp) {
      LOG.trace("{}{}", logPrefix, resp);
      if (!started) {
        startDownload(resp);
      } else {
        for (Map.Entry<Integer, byte[]> block : resp.blocks.entrySet()) {
          KReference<byte[]> ref = KReferenceFactory.getReference(block.getValue());
          blocks.put(block.getKey(), ref);
        }
        if (blocks.size() == fileDesc.nrBlocks) {
          complete();
        }
      }
    }
  };

  private void startDownload(LeecherHandleE.Completed event) {
    Set<Integer> b = new HashSet<>();
    Map<Integer, BlockDetails> irregularBlockDetails = new HashMap<>();
    int lastBlockNr = fileDesc.nrBlocks - 1;
    for (int i = 0; i <= lastBlockNr; i++) {
      b.add(i);
    }
    irregularBlockDetails.put(lastBlockNr, fileDesc.lastBlock);
    trigger(new LeecherHandleE.Download(torrentId, event.handleId, b, irregularBlockDetails, event.requestBlocks),
      leecherPort);
  }

  private void complete() {
    List<byte[]> result = new LinkedList<>();
    Iterator<Map.Entry<Integer, KReference<byte[]>>> it = blocks.entrySet().iterator();
    while (it.hasNext()) {
      result.add(it.next().getValue().getValue().get());
    }
    trigger(new MemoryE.Completed(torrentId, result), memoryPort);
  }

  public static class Init extends se.sics.kompics.Init<MemoryComp> {

    public final OverlayId torrentId;
    public final FileId fileId;
    public final FileDesc fileDesc;

    public Init(OverlayId torrentId, FileId fileId, FileDesc fileDesc) {
      this.torrentId = torrentId;
      this.fileId = fileId;
      this.fileDesc = fileDesc;
    }
  }
}
