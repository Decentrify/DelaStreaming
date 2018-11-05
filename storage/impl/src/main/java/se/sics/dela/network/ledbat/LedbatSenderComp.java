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

import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Consumer;
import se.sics.dela.network.ledbat.util.Cwnd;
import se.sics.dela.network.ledbat.util.RTTEstimator;
import se.sics.kompics.ClassMatchedHandler;
import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.Handler;
import se.sics.kompics.Negative;
import se.sics.kompics.Positive;
import se.sics.kompics.Start;
import se.sics.kompics.network.Network;
import se.sics.kompics.network.Transport;
import se.sics.kompics.timer.Timer;
import se.sics.kompics.util.Identifier;
import se.sics.ktoolbox.nutil.timer.RingTimer;
import se.sics.ktoolbox.nutil.timer.TimerProxy;
import se.sics.ktoolbox.nutil.timer.TimerProxyImpl;
import se.sics.ktoolbox.util.network.KAddress;
import se.sics.ktoolbox.util.network.KContentMsg;
import se.sics.ktoolbox.util.network.KHeader;
import se.sics.ktoolbox.util.network.basic.BasicContentMsg;
import se.sics.ktoolbox.util.network.basic.BasicHeader;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class LedbatSenderComp extends ComponentDefinition {

  Negative<LedbatSenderPort> appPort = provides(LedbatSenderPort.class);
  Positive<Network> networkPort = requires(Network.class);
  Positive<Timer> timerPort = requires(Timer.class);

  private final KAddress selfAdr;
  private final KAddress dstAdr;
  private final TimerProxy timer;

  private final Cwnd cwnd;
  private final RTTEstimator rttEstimator;
  private final RingTimer<RingContainer> ringTimer;
  private final LedbatConfig ledbatConfig;
  private LinkedList<LedbatSenderEvent.Request> pendingData = new LinkedList<>();

  private UUID ringTid;

  public LedbatSenderComp(Init init) {
    this.selfAdr = init.selfAdr;
    this.dstAdr = init.dstAdr;
    loggingCtxPutAlways("srcId", init.selfAdr.getId().toString());
    loggingCtxPutAlways("dstId", init.dstAdr.getId().toString());
    timer = new TimerProxyImpl().setup(proxy);

    ledbatConfig = new LedbatConfig();
    cwnd = new Cwnd(ledbatConfig);
    rttEstimator = new RTTEstimator(ledbatConfig);
    ringTimer = new RingTimer(LedbatCompConfig.windowSize, LedbatCompConfig.maxTimeout);
    subscribe(handleStart, control);
    subscribe(handleReq, appPort);
    subscribe(handleAck, networkPort);
  }

  Handler handleStart = new Handler<Start>() {
    @Override
    public void handle(Start event) {
      long ringWindow = LedbatCompConfig.windowSize;
      ringTid = timer.schedulePeriodicTimer(ringWindow, ringWindow, ringTimerCallback());
    }
  };

  private Consumer<Boolean> ringTimerCallback() {
    return (_input) -> {
      List<RingContainer> timeouts = ringTimer.windowTick();
      long now = System.currentTimeMillis();
      for (RingContainer c : timeouts) {
        RingContainer rc = (RingContainer) c;
        logger.trace("event:{} timed out", rc.req);
        cwnd.loss(now, rttEstimator.rto(), ledbatConfig.MSS);
        answer(rc.req, rc.req.timeout());
      }
      trySend();
    };
  }

  @Override
  public void tearDown() {
    timer.cancelPeriodicTimer(ringTid);
  }

  Handler handleReq = new Handler<LedbatSenderEvent.Request>() {
    @Override
    public void handle(LedbatSenderEvent.Request req) {
      pendingData.add(req);
      trySend();
    }
  };
  
  ClassMatchedHandler handleAck
    = new ClassMatchedHandler<LedbatMsg.Ack, KContentMsg<?, ?, LedbatMsg.Ack>>() {
    @Override
    public void handle(LedbatMsg.Ack content, KContentMsg<?, ?, LedbatMsg.Ack> msg) {
      logger.trace("incoming:{}", msg);
      Optional<RingContainer> ringContainer = ringTimer.cancelTimeout(content.dataId);
      if (!ringContainer.isPresent()) {
        logger.trace("late:{}", msg);
        return;
      }
      long now = System.currentTimeMillis();
      long rtt = content.ackDelay.receive - content.dataDelay.send;
      long dataDelay = content.dataDelay.receive - content.dataDelay.send;
      cwnd.ack(now, dataDelay, ledbatConfig.MSS);
      rttEstimator.update(rtt);
      trySend();
    }
  };

  private void trySend() {
    while (!pendingData.isEmpty() && cwnd.canSend(ledbatConfig.MSS)) {
      LedbatSenderEvent.Request event = pendingData.removeFirst();
      logger.trace("sending:{}", event);
      send(event);
      cwnd.send(ledbatConfig.MSS);
      ringTimer.setTimeout(rttEstimator.rto(), new RingContainer(event));
    }
  }

  private void send(LedbatSenderEvent.Request req) {
    LedbatMsg.Data content = new LedbatMsg.Data(req.data);
    KHeader header = new BasicHeader(selfAdr, dstAdr, Transport.UDP);
    KContentMsg msg = new BasicContentMsg(header, content);
    trigger(msg, networkPort);
  }

  public static class RingContainer implements RingTimer.Container {

    public final LedbatSenderEvent.Request req;

    public RingContainer(LedbatSenderEvent.Request req) {
      this.req = req;
    }

    @Override
    public Identifier getId() {
      return req.data.getId();
    }
  }

  public static class Init extends se.sics.kompics.Init<LedbatSenderComp> {

    public final KAddress selfAdr;
    public final KAddress dstAdr;

    public Init(KAddress selfAdr, KAddress dstAdr) {
      this.selfAdr = selfAdr;
      this.dstAdr = dstAdr;
    }
  }
}
