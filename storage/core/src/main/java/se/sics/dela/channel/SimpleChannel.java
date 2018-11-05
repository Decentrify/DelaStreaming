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
package se.sics.dela.channel;

import se.sics.dela.channel.resource.ChannelResource;
import se.sics.dela.channel.bss.BlockSelectionStrategy;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.function.Consumer;
import se.sics.dela.connector.Sink;
import se.sics.dela.connector.Source;
import se.sics.ktoolbox.nutil.timer.TimerProxy;
import se.sics.ktoolbox.util.TupleHelper;
import se.sics.ktoolbox.util.reference.KReference;
import se.sics.ktoolbox.util.trysf.Try;
import se.sics.ktoolbox.util.trysf.TryHelper;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class SimpleChannel {

  private static final long DELAY = 1000;

  private final Source source;
  private final Sink sink;
  private final ChannelResource channelResource;
  private final BlockSelectionStrategy blockSelection;
  private Optional<Consumer<Try<Boolean>>> completedCallback;
  private final Set<Integer> pendingReadBlocks = new HashSet<>();
  private final Set<Integer> pendingReadHashes = new HashSet<>();
  private final Set<Integer> pendingWriteBlocks = new HashSet<>();
  private final Set<Integer> pendingWriteHashes = new HashSet<>();

  private final TimerProxy timer;
  private UUID periodicCheck;

  public SimpleChannel(Source source, Sink sink, ChannelResource channelResource,
    BlockSelectionStrategy blockSelection, TimerProxy timer) {
    this.source = source;
    this.sink = sink;
    this.channelResource = channelResource;
    this.blockSelection = blockSelection;
    this.timer = timer;
  }

  public void start(Consumer<Try<Boolean>> completedCallback) {
    this.completedCallback = Optional.of(completedCallback);
    setup();
  }

  public void stop(Consumer<Try<Boolean>> callback) {
    pendingReadBlocks.forEach((block) -> source.cancelBlock(0));
    pendingReadHashes.forEach((hash) -> source.cancelHash(hash));
    pendingWriteBlocks.forEach((block) -> sink.cancelBlock(0));
    pendingWriteHashes.forEach((hash) -> sink.cancelHash(hash));
    channelResource.releaseAllSlots();
    if (periodicCheck != null) {
      timer.cancelPeriodicTimer(periodicCheck);
      periodicCheck = null;
    }
    cleanup(Optional.empty(), callback);
  }

  private Consumer<Boolean> periodicCheck() {
    return (notUsed) -> {
      transfer(false);
    };
  }

  private void setup() {
    Set<String> expected = new HashSet<>();
    expected.add(source.getName());
    expected.add(sink.getName());
    TryHelper.MapCollector collector = new TryHelper.MapCollector(expected, setupTransfer());
    source.connect((result) -> collector.collect(source.getName(), result));
    sink.connect((result) -> collector.collect(sink.getName(), result));
  }

  private Consumer<Try<Boolean>> setupTransfer() {
    return (Try<Boolean> result) -> {
      if (result.isFailure()) {
        try {
          result.checkedGet();
          //should never get here
        } catch (Throwable t) {
          if (completedCallback.isPresent()) {
            cleanup(Optional.of(t), completedCallback.get());
            completedCallback = Optional.empty();
          }
        }
      } else {
        periodicCheck = timer.schedulePeriodicTimer(DELAY, DELAY, periodicCheck());
        transfer(false);
      }
    };
  }

  private void transfer(boolean slotReserved) {
    for (int i = 0; i < 2; i++) {
      ChannelResource.State resourceState = channelResource.getState();
      if (ChannelResource.State.HIGH.equals(resourceState)) {
        if (slotReserved) {
          channelResource.releaseSlot();
        }
        break;
      } else if (ChannelResource.State.SATURATED.equals(channelResource.getState())) {
        if (!slotReserved) {
          break;
        }
      } else if (ChannelResource.State.LOW.equals(channelResource.getState())) {
        if (!slotReserved) {
          channelResource.reserveSlot();
        }
      } else {
        throw new IllegalArgumentException("unknown channel resource state:" + resourceState);
      }
      Set<Integer> nextHashes = blockSelection.nextHashes();
      if (nextHashes.isEmpty()) {
        Optional<Integer> nextBlock = blockSelection.nextBlock();
        if (nextBlock.isPresent()) {
          int blockNr = nextBlock.get();
          Consumer readCallback = readBlock(nextBlock.get());
          pendingReadBlocks.add(blockNr);
          source.read(blockNr, readCallback);
        } else {
          channelResource.releaseSlot();
          if (pendingReadBlocks.isEmpty() && pendingReadHashes.isEmpty()
            && pendingWriteBlocks.isEmpty() && pendingWriteHashes.isEmpty()
            && blockSelection.isComplete() && completedCallback.isPresent()) {
            completedCallback.get().accept(new Try.Success(true));
            completedCallback = Optional.empty();
          }
        }
      } else {
        TupleHelper.PairConsumer readCallback = readHashes(nextHashes);
        pendingReadHashes.addAll(nextHashes);
        source.readHash(nextHashes, readCallback);
      }
      slotReserved = false;
    }
  }

  private TupleHelper.PairConsumer<Try<Map<Integer, KReference<byte[]>>>, Set<Integer>>
    readHashes(Set<Integer> hashes) {
    return new TupleHelper.PairConsumer<Try<Map<Integer, KReference<byte[]>>>, Set<Integer>>() {
      @Override
      public void accept(Try<Map<Integer, KReference<byte[]>>> result, Set<Integer> resetHashes) {
        pendingReadHashes.removeAll(resetHashes);
        if (!result.isFailure()) {
          Map<Integer, KReference<byte[]>> hashValues = result.get();
          pendingReadHashes.removeAll(hashValues.keySet());
          pendingWriteHashes.addAll(hashValues.keySet());
          Consumer writeCallback = writeHashes(hashValues.keySet());
          sink.writeHashes(hashValues, writeCallback);
        }
        blockSelection.resetHashes(resetHashes);
      }
    };
  }

  private Consumer<Try<Boolean>> writeHashes(Set<Integer> hashes) {
    return (result) -> {
      pendingWriteHashes.removeAll(hashes);
      if (!result.isFailure()) {
        transfer(true);
      } else {
        blockSelection.resetHashes(hashes);
        channelResource.releaseSlot();
        //TODO - some backoff mechanism allowing the sink to recover
      }
    };
  }

  private Consumer<Try<KReference<byte[]>>> readBlock(int block) {
    return (Try<KReference<byte[]>> result) -> {
      pendingReadBlocks.remove(block);
      if (result.isFailure()) {
        blockSelection.resetBlock(block);
      } else {
        pendingWriteBlocks.add(block);
        Consumer writeCallback = writeBlock(block);
        sink.write(block, result.get(), writeCallback);
      }
    };
  }

  private Consumer<Try<Boolean>> writeBlock(int block) {
    return (Try<Boolean> result) -> {
      pendingWriteBlocks.remove(block);
      if (!result.isFailure()) {
        transfer(true);
      } else {
        blockSelection.resetBlock(block);
        channelResource.releaseSlot();
        //TODO - some backoff mechanism allowing the sink to recover
      }
    };
  }

  private void cleanup(Optional<Throwable> fault, Consumer<Try<Boolean>> cleanCallback) {
    Set<String> expected = new HashSet<>();
    expected.add(source.getName());
    expected.add(sink.getName());
    TryHelper.MapCollector collector = new TryHelper.MapCollector(expected, cleanCallback);
    source.disconnect((result) -> collector.collect(source.getName(), result));
    sink.disconnect((result) -> collector.collect(sink.getName(), result));
  }
}
