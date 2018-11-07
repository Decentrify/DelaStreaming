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
import org.apache.commons.math3.stat.descriptive.SummaryStatistics;
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
import se.sics.ktoolbox.util.config.impl.SystemKCWrapper;
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
public class LedbatSenderComp extends ComponentDefinition {

  Negative<LedbatSenderPort> appPort = provides(LedbatSenderPort.class);
  Positive<Network> networkPort = requires(Network.class);
  Positive<Timer> timerPort = requires(Timer.class);

  private final KAddress selfAdr;
  private final KAddress dstAdr;
  private final TimerProxy timer;

  private final Cwnd cwnd;
  private final RTTEstimator rttEstimator;
  private final RingTimer<WheelContainer> wheelTimer;
  private final DelaLedbatConfig ledbatConfig;
  private LinkedList<LedbatSenderEvent.Request> pendingData = new LinkedList<>();
  private IdentifierFactory msgIds;

  private UUID ringTid;
  private UUID reportTid;
  private SummaryStatistics s1 = new SummaryStatistics();
  private SummaryStatistics s2 = new SummaryStatistics();
  private SummaryStatistics s3 = new SummaryStatistics();
  private SummaryStatistics s4 = new SummaryStatistics();

  public LedbatSenderComp(Init init) {
    this.selfAdr = init.selfAdr;
    this.dstAdr = init.dstAdr;
    SystemKCWrapper systemConfig = new SystemKCWrapper(config());
    long ledbatSeed = systemConfig.seed 
      + selfAdr.getId().partition(Integer.MAX_VALUE)
      + dstAdr.getId().partition(Integer.MAX_VALUE);
    this.msgIds = IdentifierRegistryV2.instance(BasicIdentifiers.Values.MSG, Optional.of(ledbatSeed));
    loggingCtxPutAlways("srcId", init.selfAdr.getId().toString());
    loggingCtxPutAlways("dstId", init.dstAdr.getId().toString());
    timer = new TimerProxyImpl().setup(proxy);

    try {
      ledbatConfig = DelaLedbatConfig.instance(config()).checkedGet();
    } catch (Throwable ex) {
      throw new RuntimeException(ex);
    }
    cwnd = new Cwnd(ledbatConfig.base);
    rttEstimator = new RTTEstimator(ledbatConfig.base);
    wheelTimer = new RingTimer(ledbatConfig.timerWindowSize, ledbatConfig.maxTimeout);
    subscribe(handleStart, control);
    subscribe(handleReq, appPort);
    subscribe(handleAck, networkPort);
  }

  Handler handleStart = new Handler<Start>() {
    @Override
    public void handle(Start event) {
      startWheelTimers();
    }
  };

  private Consumer<Boolean> ringTimer() {
    return (_input) -> {
      long time1 = System.nanoTime();
      List<WheelContainer> timeouts = wheelTimer.windowTick();
      long time2 = System.nanoTime();
      long now = System.currentTimeMillis();
      long time3 = System.nanoTime();
      for (WheelContainer c : timeouts) {
        //mean 0.7 micros
        WheelContainer rc = (WheelContainer) c;
        logger.debug("msg data:{} timed out", rc.req.data.getId());
        cwnd.loss(now, rttEstimator.rto(), ledbatConfig.base.MSS);
        //
        answer(rc.req, rc.req.timeout(maxAppMsgs()));
      }
      trySend();
      s3.addValue(time2 - time1);
      s4.addValue(time3 - time2);

    };
  }

  private Consumer<Boolean> reportTimer() {
    return (_input) -> {
      logger.info("mean1:{}ns mean2:{}ns mean3:{}ns mean4:{}ns", new Object[]{
        Math.round(s1.getMean()), Math.round(s2.getMean()), Math.round(s3.getMean()), Math.round(s4.getMean())});
      rttEstimator.details(logger);
      cwnd.details(logger);
    };
  }

  @Override
  public void tearDown() {
    cancelWheelTimers();
  }

  private void startWheelTimers() {
    ringTid = timer.schedulePeriodicTimer(ledbatConfig.timerWindowSize, ledbatConfig.timerWindowSize,
      ringTimer());
    if (ledbatConfig.reportPeriod.isPresent()) {
      reportTid = timer.schedulePeriodicTimer(ledbatConfig.reportPeriod.get(), ledbatConfig.reportPeriod.get(),
        reportTimer());
    }
  }

  private void cancelWheelTimers() {
    if (ringTid != null) {
      timer.cancelPeriodicTimer(ringTid);
      ringTid = null;
    }
    if (reportTid != null) {
      timer.cancelPeriodicTimer(reportTid);
      reportTid = null;
    }
  }

  Handler handleReq = new Handler<LedbatSenderEvent.Request>() {
    @Override
    public void handle(LedbatSenderEvent.Request req) {
      if (pendingData.size() < ledbatConfig.bufferSize) {
        pendingData.add(req);
      }
      trySend();
    }
  };

  ClassMatchedHandler handleAck
    = new ClassMatchedHandler<LedbatMsg.Ack, KContentMsg<?, ?, LedbatMsg.Ack>>() {
    @Override
    public void handle(LedbatMsg.Ack content, KContentMsg<?, ?, LedbatMsg.Ack> msg) {
      logger.trace("incoming:{}", msg);
      //mean 0.7 micros
      Optional<WheelContainer> ringContainer = wheelTimer.cancelTimeout(content.dataId);
      if (!ringContainer.isPresent()) {
        logger.debug("msg data:{} is late", msg);
        return;
      }
      //mean 1.7 micros
      long now = System.currentTimeMillis();
      long rtt = content.ackDelay.receive - content.dataDelay.send;
      long dataDelay = content.dataDelay.receive - content.dataDelay.send;
      cwnd.ack(now, dataDelay, ledbatConfig.base.MSS);
      rttEstimator.update(rtt);
      //mean 4-5 micros
      appAck(ringContainer.get().req);
      //mean 12.5 micros - 1 msg sent on average
      trySend();
    }
  };

  //mean 5-6 micros
  private void appAck(LedbatSenderEvent.Request req) {
    answer(req, req.ack(maxAppMsgs()));
  }

  private void trySend() {
    while (!pendingData.isEmpty() && cwnd.canSend(ledbatConfig.base.MSS)) {
      LedbatSenderEvent.Request req = pendingData.removeFirst();
      //mean 11 micros
      send(req);
      //mean 0.4 micros
      cwnd.send(ledbatConfig.base.MSS);
      wheelTimer.setTimeout(rttEstimator.rto(), new WheelContainer(req));
    }
  }

  //mean 11-12 micros
  private void send(LedbatSenderEvent.Request req) {
    long time1 = System.nanoTime();
    LedbatMsg.Data content = new LedbatMsg.Data(msgIds.randomId(), req.data);
    long time2 = System.nanoTime();
    KHeader header = new BasicHeader(selfAdr, dstAdr, Transport.UDP);
    KContentMsg msg = new BasicContentMsg(header, content);
    long time3 = System.nanoTime();
    s1.addValue(time2-time3);
    s2.addValue(time3-time2);
    logger.trace("sending:{}", msg);
    //mean 6-8 micros
    trigger(msg, networkPort);
  }

  public static class WheelContainer implements RingTimer.Container {

    public final LedbatSenderEvent.Request req;

    public WheelContainer(LedbatSenderEvent.Request req) {
      this.req = req;
    }

    @Override
    public Identifier getId() {
      return req.data.getId();
    }
  }

  private int maxAppMsgs() {
    return Math.min(ledbatConfig.bufferSize, 2 * inFlightMsgs());
  }

  private int inFlightMsgs() {
    return (int) (cwnd.size() / ledbatConfig.base.MSS);
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
