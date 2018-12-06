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
package se.sics.dela.network.ledbat.sender;

import java.util.Optional;
import java.util.Random;
import java.util.UUID;
import java.util.function.Consumer;
import se.sics.dela.network.ledbat.LedbatSenderEvent;
import se.sics.dela.network.ledbat.LedbatSenderPort;
import se.sics.dela.network.ledbat.util.LedbatContainer;
import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.Handler;
import se.sics.kompics.Positive;
import se.sics.kompics.Start;
import se.sics.kompics.timer.Timer;
import se.sics.kompics.util.Identifier;
import se.sics.ktoolbox.nutil.timer.TimerProxy;
import se.sics.ktoolbox.nutil.timer.TimerProxyImpl;
import se.sics.ktoolbox.util.config.impl.SystemKCWrapper;
import se.sics.ktoolbox.util.identifiable.BasicIdentifiers;
import se.sics.ktoolbox.util.identifiable.IdentifierFactory;
import se.sics.ktoolbox.util.identifiable.IdentifierRegistryV2;

/**
 *
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class DriverComp extends ComponentDefinition {

  Positive<LedbatSenderPort> ledbat = requires(LedbatSenderPort.class);
  Positive<Timer> timerPort = requires(Timer.class);

  private TimerProxy timer;
  
  private Random rand = new Random(123);
  private int acked = 0;
  private int timedout = 0;
  private int inFlight = 0;
  private long startTime;
  private static final int TOTAL_MSGS = 1000000;
  private UUID reportTid;

  private IdentifierFactory eventIds;
  private IdentifierFactory dataIds;

  public DriverComp() {
    SystemKCWrapper systemConfig = new SystemKCWrapper(config());
    long driverSeed = systemConfig.seed;
    this.eventIds = IdentifierRegistryV2.instance(BasicIdentifiers.Values.EVENT, Optional.of(driverSeed));
    this.dataIds = IdentifierRegistryV2.instance(BasicIdentifiers.Values.EVENT, Optional.of(driverSeed));

    timer = new TimerProxyImpl();
    subscribe(handleStart, control);
    subscribe(handleAcked, ledbat);
    subscribe(handleTimeout, ledbat);
  }

  Handler handleStart = new Handler<Start>() {
    @Override
    public void handle(Start event) {
      timer.setup(proxy, logger);
      reportTid = timer.schedulePeriodicTimer(1000, 1000, report());
      startTime = System.nanoTime();
      trySend(1);
    }
  };

  @Override
  public void tearDown() {
    timer.cancelPeriodicTimer(reportTid);
  }

  Consumer<Boolean> report() {
    return (_in) -> {
      logger.info("{}%", Math.round(100 * acked / TOTAL_MSGS));
    };
  }

  Handler handleAcked = new Handler<LedbatSenderEvent.Acked>() {
    @Override
    public void handle(LedbatSenderEvent.Acked event) {
      logger.debug("received:{}", event.req.data.getId());
      acked++;
      inFlight--;
      if (acked == TOTAL_MSGS) {
        logger.info("done");
        long stopTime = System.nanoTime();
        long sizeMB = 1l * TOTAL_MSGS * 1024 / (1024 * 1024);
        double time = (double) (stopTime - startTime) / (1000 * 1000 * 1000);
        logger.info("send time(s):" + time);
        logger.info("send avg speed(MB/s):" + sizeMB / time);
        logger.info("timedout:{}/{}", timedout, TOTAL_MSGS);
      }
      trySend(event.maxInFlight);
    }
  };

  Handler handleTimeout = new Handler<LedbatSenderEvent.Timeout>() {
    @Override
    public void handle(LedbatSenderEvent.Timeout event) {
      logger.debug("timeout:{}", event.req.data.getId());
      inFlight--;
      timedout++;
      trySend(event.maxInFlight);
    }
  };

  private void trySend(int maxInFlight) {
    while (inFlight < maxInFlight) {
      if (acked < TOTAL_MSGS) {
        byte[] dataBytes = new byte[1024];
        rand.nextBytes(dataBytes);
        send(dataBytes);
      }
    }
  }

  private void send(byte[] data) {
    inFlight++;
    Identifier dataId = dataIds.randomId();
    LedbatContainer dataContainer = new LedbatContainer(dataId, data);
    LedbatSenderEvent.Request req = new LedbatSenderEvent.Request(eventIds.randomId(), dataContainer);
    logger.trace("sending:{}", req.data.getId());
    trigger(req, ledbat);
  }
}
