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
package se.sics.dela.workers.ctrl.util;

import java.util.Optional;
import java.util.Random;
import java.util.UUID;
import java.util.function.Consumer;
import se.sics.dela.network.ledbat.util.LedbatContainer;
import se.sics.dela.network.util.DatumId;
import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.Handler;
import se.sics.kompics.Positive;
import se.sics.kompics.Start;
import se.sics.kompics.network.Network;
import se.sics.kompics.network.Transport;
import se.sics.kompics.timer.Timer;
import se.sics.kompics.util.Identifier;
import se.sics.ktoolbox.nutil.nxcomp.NxStackId;
import se.sics.ktoolbox.nutil.timer.TimerProxy;
import se.sics.ktoolbox.nutil.timer.TimerProxyImpl;
import se.sics.ktoolbox.util.identifiable.BasicBuilders;
import se.sics.ktoolbox.util.identifiable.BasicIdentifiers;
import se.sics.ktoolbox.util.identifiable.IdentifierFactory;
import se.sics.ktoolbox.util.identifiable.IdentifierRegistryV2;
import se.sics.ktoolbox.util.identifiable.basic.IntIdFactory;
import se.sics.ktoolbox.util.network.KAddress;
import se.sics.ktoolbox.util.network.KContentMsg;
import se.sics.ktoolbox.util.network.KHeader;
import se.sics.ktoolbox.util.network.basic.BasicContentMsg;
import se.sics.ktoolbox.util.network.basic.BasicHeader;
import se.sics.ktoolbox.nutil.network.ledbat.LedbatMsg;
import se.sics.ktoolbox.nutil.network.ledbat.LedbatStatus;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class SenderTaskComp extends ComponentDefinition {

  private final Positive<Timer> timerPort = requires(Timer.class);
  private final Positive<Network> ledbatPort = requires(Network.class);
  private final Positive<LedbatStatus.Port> ledbatStatus = requires(LedbatStatus.Port.class);

  private final KAddress selfAdr;
  private final KAddress receiverAdr;
  private final NxStackId dataStreamId;
  private final Identifier dataId;
  private final IdentifierFactory eventIds;

  private TimerProxy timer;
  private UUID reportTid;

  private final Random rand = new Random(1234);

  private int acked = 0;
  private int timedout = 0;
  private int inFlight = 0;
  private long startTime;
  private static final int TOTAL_MSGS = 15000000;
  private int unitCounter = 0;
  private int timeWindowUnitCounter = 0;
  private IntIdFactory unitIds = new IntIdFactory(Optional.empty());
  private final IdentifierFactory msgIds;

  public SenderTaskComp(Init init) {
    this.selfAdr = init.selfAdr;
    this.receiverAdr = init.receiverAdr;
    this.dataStreamId = init.dataStreamId;
    this.dataId = init.dataId;
    eventIds = IdentifierRegistryV2.instance(BasicIdentifiers.Values.EVENT, Optional.of(1234l));
    timer = new TimerProxyImpl(dataStreamId).setup(proxy, logger);
    this.msgIds = IdentifierRegistryV2.instance(BasicIdentifiers.Values.MSG, Optional.of(1234l));

    subscribe(handleStart, control);
    subscribe(handleAcked, ledbatStatus);
    subscribe(handleTimeout, ledbatStatus);
  }

  Handler handleStart = new Handler<Start>() {
    @Override
    public void handle(Start event) {
      reportTid = timer.schedulePeriodicTimer(1000, 1000, report());
      logger.info("report tid: {}", reportTid);
      startTime = System.nanoTime();
      logger.info("starting transfer sender:{} receiver:{}", selfAdr, receiverAdr);
      trySend(50);
    }
  };

  @Override
  public void tearDown() {
    timer.cancelPeriodicTimer(reportTid);
  }

  Consumer<Boolean> report() {
    return (_in) -> {
      logger.info("{}% window:{}KB/s", Math.round(100 * unitCounter / TOTAL_MSGS), timeWindowUnitCounter);
      logger.info("timedout:{}, acked:{} sent:{}", new Object[]{timedout, acked, unitCounter});
      timeWindowUnitCounter = 0;
    };
  }

  Handler handleAcked = new Handler<LedbatStatus.Ack>() {
    @Override
    public void handle(LedbatStatus.Ack event) {
      logger.info("received:{}", event.acked.size());
      acked += event.acked.size();
      inFlight -= event.acked.size();
      if (unitCounter == TOTAL_MSGS) {
        logger.info("done");
        long stopTime = System.nanoTime();
        long sizeMB = 1l * TOTAL_MSGS * 1024 / (1024 * 1024);
        double time = (double) (stopTime - startTime) / (1000 * 1000 * 1000);
        logger.info("send time(s):" + time);
        logger.info("send avg speed(MB/s):" + sizeMB / time);
        logger.info("timedout:{}, acked:{} sent:{}", new Object[]{timedout, acked, unitCounter});
      }
      trySend(event.maxInFlight);
    }
  };

  Handler handleTimeout = new Handler<LedbatStatus.Timeout>() {
    @Override
    public void handle(LedbatStatus.Timeout event) {
      logger.info("timeout:{}", event.timedOut.size());
      inFlight -= event.timedOut.size();
      timedout += event.timedOut.size();
      trySend(event.maxInFlight);
    }
  };

  private void trySend(int maxInFlight) {
    while (inFlight < maxInFlight) {
      if (unitCounter < TOTAL_MSGS) {
        unitCounter++;
        timeWindowUnitCounter++;
        byte[] dataBytes = new byte[1024];
        rand.nextBytes(dataBytes);
        send(dataBytes);
      } else {
        break;
      }
    }
  }

  private void send(byte[] data) {
    inFlight++;
    Identifier unitId = unitIds.id(new BasicBuilders.IntBuilder(unitCounter));
    DatumId datumId = new DatumId(dataId, unitId);
    LedbatContainer dataContainer = new LedbatContainer(datumId, data);
    LedbatMsg.Datum content = new LedbatMsg.Datum(msgIds.randomId(), dataStreamId, dataContainer);
    KHeader header = new BasicHeader(selfAdr, receiverAdr, Transport.UDP);
    KContentMsg msg = new BasicContentMsg(header, content);
    logger.trace("sending:{}", content.getId());
    trigger(msg, ledbatPort);
  }

  public static class Init extends se.sics.kompics.Init<SenderTaskComp> {

    public final KAddress selfAdr;
    public final KAddress receiverAdr;
    public final Identifier dataId;
    public final NxStackId dataStreamId;

    public Init(KAddress selfAdr, KAddress receiverAdr, Identifier dataId, NxStackId dataStreamId) {
      this.selfAdr = selfAdr;
      this.receiverAdr = receiverAdr;
      this.dataId = dataId;
      this.dataStreamId = dataStreamId;
    }
  }
}
