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

import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.function.Consumer;
import org.junit.Assert;
import org.junit.Test;
import se.sics.dela.channel.bss.BlockSelectionStrategies;
import se.sics.dela.channel.bss.BlockSelectionStrategy;
import se.sics.dela.channel.resource.ChannelResource;
import se.sics.dela.channel.resource.ChannelResources;
import se.sics.dela.connector.Sink;
import se.sics.dela.connector.Source;
import se.sics.dela.util.TimerProxy;
import se.sics.dela.util.TimerProxyImpl;
import se.sics.kompics.ComponentProxy;
import se.sics.ktoolbox.util.TupleHelper;
import se.sics.ktoolbox.util.reference.KReference;
import se.sics.ktoolbox.util.trysf.Try;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class SimpleChannelTest {

  @Test
  public void testInstaConnectors() {
    LinkedList<Events> events = new LinkedList<>();
    Source source = new SourceTest(events);
    Sink sink = new SinkTest(events);
    ChannelResources.Fixed resource = new ChannelResources.Fixed(5);
    BlockSelectionStrategy strategy = new BlockSelectionStrategies.AllInOrder(3, 5, 5);
    TimerProxy timer = new TimerProxyTest(events);
    SimpleChannel channel = new SimpleChannel(source, sink, resource, strategy, timer);
    Consumer<Try<Boolean>> cleaned = (result) -> {
      events.add(Events.CHANNEL_CLEANED);
    };
    Consumer<Try<Boolean>> completed = (result) -> {
      events.add(Events.TRANSFER_COMPLETED);
      channel.stop(cleaned);
    };
    channel.start(completed);
    checkEvents(events, Events.SOURCE_CONNECT, Events.SINK_CONNECT, Events.SCHEDULE_TIMER);
    checkEvents(events, Events.SOURCE_HASH_READ, Events.SINK_HASH_WRITE);
    checkEvents(events, Events.SOURCE_READ, Events.SINK_WRITE, Events.SOURCE_READ, Events.SINK_WRITE,
      Events.SOURCE_READ, Events.SINK_WRITE, Events.TRANSFER_COMPLETED);
    checkEvents(events, Events.CANCEL_TIMER, Events.SOURCE_DISCONNECT, Events.SINK_DISCONNECT, 
      Events.CHANNEL_CLEANED);
    Assert.assertTrue(events.isEmpty());
    Assert.assertEquals(0, resource.usedSlots());
  }

  @Test
  public void testDelayedConnectors1() {
    LinkedList<Events> events = new LinkedList<>();
    SourceTestDelayed source = new SourceTestDelayed(events);
    SinkTestDelayed sink = new SinkTestDelayed(events);
    ChannelResources.Fixed resource = new ChannelResources.Fixed(5);
    BlockSelectionStrategy strategy = new BlockSelectionStrategies.AllInOrder(3, 5, 5);
    TimerProxy timer = new TimerProxyTest(events);
    SimpleChannel channel = new SimpleChannel(source, sink, resource, strategy, timer);
    Consumer<Try<Boolean>> cleaned = (result) -> {
      events.add(Events.CHANNEL_CLEANED);
    };
    Consumer<Try<Boolean>> completed = (result) -> {
      events.add(Events.TRANSFER_COMPLETED);
      channel.stop(cleaned);
    };
    channel.start(completed);
    checkEvents(events, Events.SOURCE_CONNECT, Events.SINK_CONNECT, Events.SCHEDULE_TIMER);
    checkEvents(events, Events.SOURCE_HASH_READ, Events.SOURCE_READ);
    Assert.assertEquals(2, source.delayed.size());
    //
    source.triggerOneDelayed();
    checkEvents(events, Events.SINK_HASH_WRITE);
    Assert.assertEquals(1, source.delayed.size());
    Assert.assertEquals(1, sink.delayed.size());
    //
    sink.triggerOneDelayed();
    checkEvents(events, Events.SOURCE_READ, Events.SOURCE_READ);
    Assert.assertEquals(3, source.delayed.size());
    Assert.assertEquals(0, sink.delayed.size());
    //
    source.triggerOneDelayed();
    checkEvents(events, Events.SINK_WRITE);
    Assert.assertEquals(2, source.delayed.size());
    Assert.assertEquals(1, sink.delayed.size());
    //
    source.triggerAllDelayed();
    checkEvents(events, Events.SINK_WRITE, Events.SINK_WRITE);
    Assert.assertEquals(0, source.delayed.size());
    Assert.assertEquals(3, sink.delayed.size());
    //
    sink.triggerOneDelayed();
    Assert.assertTrue(events.isEmpty());
    Assert.assertEquals(0, source.delayed.size());
    Assert.assertEquals(2, sink.delayed.size());
    //
    sink.triggerAllDelayed();
    checkEvents(events, Events.TRANSFER_COMPLETED);
    Assert.assertEquals(0, source.delayed.size());
    Assert.assertEquals(0, sink.delayed.size());
    //
    checkEvents(events, Events.CANCEL_TIMER, Events.SOURCE_DISCONNECT, Events.SINK_DISCONNECT, 
      Events.CHANNEL_CLEANED);
    
    Assert.assertTrue(events.isEmpty());
    Assert.assertEquals(0, resource.usedSlots());
  }
  
  @Test
  public void testDelayedConnectors2() {
    LinkedList<Events> events = new LinkedList<>();
    SourceTestDelayed source = new SourceTestDelayed(events);
    SinkTestDelayed sink = new SinkTestDelayed(events);
    ChannelResources.Fixed resource = new ChannelResources.Fixed(3);
    BlockSelectionStrategy strategy = new BlockSelectionStrategies.AllInOrder(5, 5, 5);
    TimerProxy timer = new TimerProxyTest(events);
    SimpleChannel channel = new SimpleChannel(source, sink, resource, strategy, timer);
    Consumer<Try<Boolean>> cleaned = (result) -> {
      events.add(Events.CHANNEL_CLEANED);
    };
    Consumer<Try<Boolean>> completed = (result) -> {
      events.add(Events.TRANSFER_COMPLETED);
      channel.stop(cleaned);
    };
    channel.start(completed);
    checkEvents(events, Events.SOURCE_CONNECT, Events.SINK_CONNECT, Events.SCHEDULE_TIMER);
    checkEvents(events, Events.SOURCE_HASH_READ, Events.SOURCE_READ);
    Assert.assertEquals(2, source.delayed.size());
    //
    source.triggerOneDelayed();
    checkEvents(events, Events.SINK_HASH_WRITE);
    Assert.assertEquals(1, source.delayed.size());
    Assert.assertEquals(1, sink.delayed.size());
    //
    sink.triggerOneDelayed();
    checkEvents(events, Events.SOURCE_READ, Events.SOURCE_READ);
    Assert.assertEquals(3, source.delayed.size());
    Assert.assertEquals(0, sink.delayed.size());
    //
    source.triggerOneDelayed();
    checkEvents(events, Events.SINK_WRITE);
    Assert.assertEquals(2, source.delayed.size());
    Assert.assertEquals(1, sink.delayed.size());
    //
    sink.triggerOneDelayed();
    checkEvents(events, Events.SOURCE_READ);
    Assert.assertEquals(3, source.delayed.size());
    Assert.assertEquals(0, sink.delayed.size());
    //
    source.triggerAllDelayed();
    checkEvents(events, Events.SINK_WRITE, Events.SINK_WRITE, Events.SINK_WRITE);
    Assert.assertEquals(0, source.delayed.size());
    Assert.assertEquals(3, sink.delayed.size());
    //
    sink.triggerAllDelayed();
    checkEvents(events, Events.SOURCE_READ);
    Assert.assertEquals(1, source.delayed.size());
    Assert.assertEquals(0, sink.delayed.size());
    //
    source.triggerOneDelayed();
    checkEvents(events, Events.SINK_WRITE);
    Assert.assertEquals(0, source.delayed.size());
    Assert.assertEquals(1, sink.delayed.size());
    //
    sink.triggerOneDelayed();
    checkEvents(events, Events.TRANSFER_COMPLETED);
    Assert.assertEquals(0, source.delayed.size());
    Assert.assertEquals(0, sink.delayed.size());
    //
    checkEvents(events, Events.CANCEL_TIMER, Events.SOURCE_DISCONNECT, Events.SINK_DISCONNECT, 
      Events.CHANNEL_CLEANED);
    
    Assert.assertTrue(events.isEmpty());
    Assert.assertEquals(0, resource.usedSlots());
  }
  
  @Test
  public void testDelayedConnectors3() {
    LinkedList<Events> events = new LinkedList<>();
    SourceTestDelayed source = new SourceTestDelayed(events);
    SinkTestDelayed sink = new SinkTestDelayed(events);
    ChannelResources.Fixed resource = new ChannelResources.Fixed(3);
    BlockSelectionStrategy strategy = new BlockSelectionStrategies.AllInOrder(5, 5, 5);
    TimerProxyTest timer = new TimerProxyTest(events);
    SimpleChannel channel = new SimpleChannel(source, sink, resource, strategy, timer);
    Consumer<Try<Boolean>> cleaned = (result) -> {
      events.add(Events.CHANNEL_CLEANED);
    };
    Consumer<Try<Boolean>> completed = (result) -> {
      events.add(Events.TRANSFER_COMPLETED);
      channel.stop(cleaned);
    };
    channel.start(completed);
    checkEvents(events, Events.SOURCE_CONNECT, Events.SINK_CONNECT, Events.SCHEDULE_TIMER);
    checkEvents(events, Events.SOURCE_HASH_READ, Events.SOURCE_READ);
    Assert.assertEquals(2, source.delayed.size());
    Assert.assertEquals(0, sink.delayed.size());
    //
    timer.timeout();
    checkEvents(events, Events.SOURCE_READ);
    Assert.assertEquals(3, source.delayed.size());
    Assert.assertEquals(0, sink.delayed.size());
    //
    source.triggerAllDelayed();
    checkEvents(events, Events.SINK_HASH_WRITE, Events.SINK_WRITE, Events.SINK_WRITE);
    Assert.assertEquals(0, source.delayed.size());
    Assert.assertEquals(3, sink.delayed.size());
    //
    timer.timeout();
    Assert.assertTrue(events.isEmpty());
    Assert.assertEquals(0, source.delayed.size());
    Assert.assertEquals(3, sink.delayed.size());
    //
    sink.triggerAllDelayed();
    checkEvents(events, Events.SOURCE_READ, Events.SOURCE_READ, Events.SOURCE_READ);
    Assert.assertEquals(3, source.delayed.size());
    Assert.assertEquals(0, sink.delayed.size());
    //
    source.triggerAllDelayed();
    checkEvents(events, Events.SINK_WRITE, Events.SINK_WRITE, Events.SINK_WRITE);
    Assert.assertEquals(0, source.delayed.size());
    Assert.assertEquals(3, sink.delayed.size());
    //
    sink.triggerAllDelayed();
    checkEvents(events, Events.TRANSFER_COMPLETED);
    Assert.assertEquals(0, source.delayed.size());
    Assert.assertEquals(0, sink.delayed.size());
    //
    checkEvents(events, Events.CANCEL_TIMER, Events.SOURCE_DISCONNECT, Events.SINK_DISCONNECT, 
      Events.CHANNEL_CLEANED);
    
    Assert.assertTrue(events.isEmpty());
    Assert.assertEquals(0, resource.usedSlots());
  }

  void checkEvents(LinkedList<Events> events, Events... expected) {
    Assert.assertTrue(expected.length <= events.size());
    for (Events e : expected) {
      Assert.assertEquals(e, events.pollFirst());
    }
  }

  public static enum Events {
    TRANSFER_COMPLETED,
    SOURCE_CONNECT,
    SINK_CONNECT,
    SOURCE_DISCONNECT,
    SINK_DISCONNECT,
    SOURCE_HASH_READ,
    SINK_HASH_WRITE,
    SOURCE_READ,
    SINK_WRITE,
    SCHEDULE_TIMER,
    CANCEL_TIMER,
    CHANNEL_CLEANED
 }

  public static class SourceTest implements Source {

    final LinkedList<Events> events;

    SourceTest(LinkedList<Events> events) {
      this.events = events;
    }

    @Override
    public String getName() {
      return "source_test";
    }

    @Override
    public void readHash(Set<Integer> hashes,
      TupleHelper.PairConsumer<Try<Map<Integer, KReference<byte[]>>>, Set<Integer>> callback) {
      events.add(Events.SOURCE_HASH_READ);
      Map<Integer, KReference<byte[]>> hashVals = new TreeMap<>();
      hashes.forEach((hash) -> hashVals.put(hash, null));
      callback.accept(new Try.Success(hashVals), new HashSet<>());
    }

    @Override
    public void cancelHash(int hash) {
      throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void read(int blockNr, Consumer<Try<KReference<byte[]>>> callback) {
      events.add(Events.SOURCE_READ);
      callback.accept(new Try.Success(null));
    }

    @Override
    public void cancelBlock(int block) {
      throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void connect(Consumer<Try<Boolean>> callback) {
      events.add(Events.SOURCE_CONNECT);
      callback.accept(new Try.Success(true));
    }

    @Override
    public void disconnect(Consumer<Try<Boolean>> callback) {
      events.add(Events.SOURCE_DISCONNECT);
      callback.accept(new Try.Success(true));
    }
  }

  public static class SourceTestDelayed extends SourceTest {

    LinkedList<Consumer<Boolean>> delayed = new LinkedList<>();

    public SourceTestDelayed(LinkedList<Events> events) {
      super(events);
    }

    @Override
    public void readHash(Set<Integer> hashes,
      TupleHelper.PairConsumer<Try<Map<Integer, KReference<byte[]>>>, Set<Integer>> callback) {
      events.add(Events.SOURCE_HASH_READ);
      Map<Integer, KReference<byte[]>> hashVals = new TreeMap<>();
      hashes.forEach((hash) -> hashVals.put(hash, null));
      delayed.add((event) -> callback.accept(new Try.Success(hashVals), new HashSet<>()));
    }

    @Override
    public void read(int blockNr, Consumer<Try<KReference<byte[]>>> callback) {
      events.add(Events.SOURCE_READ);
      delayed.add((event) -> callback.accept(new Try.Success(null)));
    }

    public void triggerOneDelayed() {
      delayed.pollFirst().accept(true);
    }

    public void triggerAllDelayed() {
      while (!delayed.isEmpty()) {
        delayed.pollFirst().accept(true);
      }
    }
  };

  public static class SinkTest implements Sink {

    final LinkedList<Events> events;

    SinkTest(LinkedList<Events> events) {
      this.events = events;
    }

    @Override
    public String getName() {
      return "sink_test";
    }

    @Override
    public void writeHashes(Map<Integer, KReference<byte[]>> hashes, Consumer<Try<Boolean>> callback) {
      events.add(Events.SINK_HASH_WRITE);
      callback.accept(new Try.Success(true));
    }

    @Override
    public void cancelHash(int hash) {
      throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void write(int block, KReference<byte[]> value, Consumer<Try<Boolean>> callback) {
      events.add(Events.SINK_WRITE);
      callback.accept(new Try.Success(true));
    }

    @Override
    public void cancelBlock(int block) {
      throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void connect(Consumer<Try<Boolean>> callback) {
      events.add(Events.SINK_CONNECT);
      callback.accept(new Try.Success(true));
    }

    @Override
    public void disconnect(Consumer<Try<Boolean>> callback) {
      events.add(Events.SINK_DISCONNECT);
      callback.accept(new Try.Success(true));
    }
  }

  public static class SinkTestDelayed extends SinkTest {

    LinkedList<Consumer<Boolean>> delayed = new LinkedList<>();

    public SinkTestDelayed(LinkedList<Events> events) {
      super(events);
    }

    @Override
    public void writeHashes(Map<Integer, KReference<byte[]>> hashes, Consumer<Try<Boolean>> callback) {
      events.add(Events.SINK_HASH_WRITE);
      delayed.add((event) -> callback.accept(new Try.Success(true)));
    }

    @Override
    public void cancelHash(int hash) {
      throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void write(int block, KReference<byte[]> value, Consumer<Try<Boolean>> callback) {
      events.add(Events.SINK_WRITE);
      delayed.add((event) -> callback.accept(new Try.Success(true)));
    }

    public void triggerOneDelayed() {
      delayed.pollFirst().accept(true);
    }
    
    public void triggerAllDelayed() {
      while (!delayed.isEmpty()) {
        delayed.pollFirst().accept(true);
      }
    }
  };

  public static class TimerProxyTest implements TimerProxy {

    private final LinkedList<Events> events;
    private Consumer<Boolean> timerCallback;

    TimerProxyTest(LinkedList<Events> events) {
      this.events = events;
    }

    @Override
    public TimerProxyImpl setup(ComponentProxy proxy) {
      throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public UUID schedulePeriodicTimer(long delay, long period, Consumer<Boolean> callback) {
      events.add(Events.SCHEDULE_TIMER);
      timerCallback = callback;
      return UUID.randomUUID();
    }

    @Override
    public void cancelPeriodicTimer(UUID timeoutId) {
      events.add(Events.CANCEL_TIMER);
    }
    
    public void timeout() {
      timerCallback.accept(true);
    }
  }
}
