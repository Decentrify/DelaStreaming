/*
 * Copyright (C) 2009 Swedish Institute of Computer Science (SICS) Copyright (C)
 * 2009 Royal Institute of Technology (KTH)
 *
 * KompicsToolbox is free software; you can redistribute it and/or
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
 * along with this program; if not, loss to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
 */
package se.sics.dela.network.ledbat;

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Consumer;
import se.sics.dela.network.util.DatumId;
import se.sics.kompics.ClassMatchedHandler;
import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.Handler;
import se.sics.kompics.Negative;
import se.sics.kompics.Positive;
import se.sics.kompics.Start;
import se.sics.kompics.network.Network;
import se.sics.kompics.network.Transport;
import se.sics.kompics.timer.Timer;
import se.sics.kompics.util.Identifiable;
import se.sics.kompics.util.Identifier;
import se.sics.ktoolbox.nutil.timer.TimerProxy;
import se.sics.ktoolbox.nutil.timer.TimerProxyImpl;
import se.sics.ktoolbox.util.identifiable.BasicIdentifiers;
import se.sics.ktoolbox.util.identifiable.IdentifierFactory;
import se.sics.ktoolbox.util.identifiable.IdentifierRegistryV2;
import se.sics.ktoolbox.util.network.KAddress;
import se.sics.ktoolbox.util.network.KContentMsg;
import se.sics.ktoolbox.util.network.KHeader;
import se.sics.ktoolbox.util.network.basic.BasicContentMsg;
import se.sics.ktoolbox.util.network.basic.BasicHeader;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class LedbatReceiverComp extends ComponentDefinition {

  Negative<LedbatReceiverPort> appPort = provides(LedbatReceiverPort.class);
  Positive<Network> networkPort = requires(Network.class);
  Positive<Timer> timerPort = requires(Timer.class);
  final TimerProxy timer;

  private final KAddress selfAdr;
  private final KAddress srcAdr;
  private final Identifier rivuletId;

  private final IdentifierFactory eventIds;
  private final IdentifierFactory msgIds;
  private UUID batchingTId;
  private final Map<Identifier, Batch> batches = new HashMap<>();

  public LedbatReceiverComp(Init init) {
    this.selfAdr = init.selfAdr;
    this.srcAdr = init.srcAdr;
    this.rivuletId = init.rivuletId;
    this.timer = new TimerProxyImpl(srcAdr.getId()).setup(proxy, logger);
    loggingCtxPutAlways("dstId", init.selfAdr.getId().toString());
    loggingCtxPutAlways("srcId", init.srcAdr.getId().toString());
    this.eventIds = IdentifierRegistryV2.instance(BasicIdentifiers.Values.EVENT, Optional.of(1234l));
    this.msgIds = IdentifierRegistryV2.instance(BasicIdentifiers.Values.MSG, Optional.of(1234l));
    subscribe(handleStart, control);
    subscribe(handleData, networkPort);
  }

  Handler handleStart = new Handler<Start>() {
    @Override
    public void handle(Start event) {
      batchingTId = timer.schedulePeriodicTimer(20, 20, batchingTimeout());
    }
  };

  @Override
  public void tearDown() {
    if (batchingTId != null) {
      timer.cancelPeriodicTimer(batchingTId);
      batchingTId = null;
    }
  }

  private Consumer<Boolean> batchingTimeout() {
    return (_ignore) -> {
      Iterator<Batch> it = batches.values().iterator();
      long now = System.currentTimeMillis();
      while(it.hasNext()) {
        Batch batch = it.next();
        sendBatch(now, batch);
        it.remove();
      }
    };
  }

  ClassMatchedHandler handleData
    = new ClassMatchedHandler<LedbatMsg.Datum, KContentMsg<?, ?, LedbatMsg.Datum>>() {

    @Override
    public void handle(LedbatMsg.Datum datum, KContentMsg<?, ?, LedbatMsg.Datum> msg) {
      answerApp(datum);
      batchAcks(datum, msg);
    }
  };

  private void answerNetwork(KContentMsg<?, ?, LedbatMsg.Datum> msg) {
    KContentMsg<?, ?, LedbatMsg.Ack> resp = msg.answer(msg.getContent().answer());
    trigger(resp, networkPort);
  }

  private void answerApp(LedbatMsg.Datum content) {
    trigger(new LedbatReceiverEvent.Received(eventIds.randomId(), rivuletId, content.datum), appPort);
  }

  private void batchAcks(LedbatMsg.Datum<Identifiable<DatumId>> datum, KContentMsg<?, ?, LedbatMsg.Datum> msg) {
    Batch batch = batches.get(datum.datum.getId().dataId());
    if (batch == null) {
      batch = new Batch(datum.datum.getId().dataId(), msg.getHeader().getSource());
      batches.put(batch.dataId, batch);
    }
    long now = System.currentTimeMillis();
    batch.ack(now, datum);
    if (batch.acks.size() == 10) {
      sendBatch(now, batch);
      batches.remove(batch.dataId);
    }
  }

  private void sendBatch(long now, Batch batch) {
    LedbatMsg.MultiAck content = new LedbatMsg.MultiAck(msgIds.randomId(), batch.dataId, batch.batch(now));
    KHeader header = new BasicHeader(selfAdr, batch.sender, Transport.UDP);
    KContentMsg msg = new BasicContentMsg(header, content);
    trigger(msg, networkPort);
  }

  public static class Init extends se.sics.kompics.Init<LedbatReceiverComp> {

    public final KAddress selfAdr;
    public final KAddress srcAdr;
    public final Identifier rivuletId;

    public Init(KAddress selfAdr, KAddress srcAdr, Identifier rivuletId) {
      this.selfAdr = selfAdr;
      this.srcAdr = srcAdr;
      this.rivuletId = rivuletId;
    }
  }

  static class Batch {

    public final Identifier dataId;
    public List<LedbatMsg.AckVal> acks = new LinkedList<>();
    public final KAddress sender;

    public Batch(Identifier dataId, KAddress sender) {
      this.dataId = dataId;
      this.sender = sender;
    }

    public void ack(long now, LedbatMsg.Datum<Identifiable<DatumId>> datum) {
      LedbatMsg.AckVal ackVal = new LedbatMsg.AckVal(datum.datum.getId().unitId(), datum.getId());
      ackVal.setSt1(datum.ledbatSendTime);
      ackVal.setSt2(datum.dataDelay.send);
      ackVal.setRt1(datum.dataDelay.receive);
      ackVal.setRt2(now);
      acks.add(ackVal);
    }

    public LedbatMsg.BatchAckVal batch(long now) {
      LedbatMsg.BatchAckVal batch = new LedbatMsg.BatchAckVal(new LinkedList<>(acks));
      batch.setRt3(now);
      acks.clear();
      return batch;
    }
  }
}
