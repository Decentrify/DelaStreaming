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
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import org.apache.commons.math3.stat.descriptive.SummaryStatistics;
import org.slf4j.Logger;
import se.sics.dela.network.ledbat.util.Cwnd;
import se.sics.dela.network.ledbat.util.RTTEstimator;
import se.sics.kompics.config.Config;
import se.sics.kompics.util.Identifier;
import se.sics.ktoolbox.nutil.timer.RingTimer;
import se.sics.ktoolbox.nutil.timer.TimerProxy;
import se.sics.ktoolbox.util.identifiable.IdentifierFactory;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class LedbatSender {

  private TimerProxy timer;

  private Cwnd cwnd;
  private RTTEstimator rttEstimator;
  private RTTEstimator senderQ1Estimator;
  private RTTEstimator senderQ2Estimator;
  private RTTEstimator receiverQEstimator;
  RingTimer<WheelContainer> wheelTimer;
  private DelaLedbatConfig ledbatConfig;
  private Logger logger;

  private IdentifierFactory msgIds;

  private Consumer<LedbatMsg.Datum> networkSend;
  private BiConsumer<LedbatSenderEvent.Request, LedbatSenderEvent.Indication> appSend;

  private final LinkedList<LedbatSenderEvent.Request> pendingData = new LinkedList<>();
  private UUID ringTid;
  private UUID reportTid;

  SummaryStatistics s1 = new SummaryStatistics();
  SummaryStatistics s2 = new SummaryStatistics();

  public LedbatSender() {
  }

  public LedbatSender setup(TimerProxy timer, Config config, Logger logger, IdentifierFactory msgIds,
    Consumer<LedbatMsg.Datum> networkSend,
    BiConsumer<LedbatSenderEvent.Request, LedbatSenderEvent.Indication> appSend) {
    this.logger = logger;
    this.msgIds = msgIds;
    this.networkSend = networkSend;
    this.appSend = appSend;
    this.timer = timer;
    try {
      ledbatConfig = DelaLedbatConfig.instance(config).checkedGet();
    } catch (Throwable ex) {
      throw new RuntimeException(ex);
    }
//    cwnd = new Cwnd(ledbatConfig.base, LedbatLossCtrl.vanillaLedbat());
    cwnd = new MultiPaneCwnd(ledbatConfig.base, LedbatLossCtrl.twoLvlPercentageLedbat(0.02, 0.1), logger);
    rttEstimator = RTTEstimator.instance(ledbatConfig.base);
    senderQ1Estimator = RTTEstimator.instance();
    senderQ2Estimator = RTTEstimator.instance();
    receiverQEstimator = RTTEstimator.instance();
    wheelTimer = new RingTimer(ledbatConfig.timerWindowSize, 3 * ledbatConfig.maxTimeout);
    return this;
  }

  public LedbatSender start() {
    ringTid = timer.schedulePeriodicTimer(ledbatConfig.timerWindowSize, ledbatConfig.timerWindowSize,
      ringTimer());
    if (ledbatConfig.reportPeriod.isPresent()) {
      reportTid = timer.schedulePeriodicTimer(ledbatConfig.reportPeriod.get(), ledbatConfig.reportPeriod.get(),
        reportTimer());
    }
    return this;
  }

  public void stop() {
    if (ringTid != null) {
      timer.cancelPeriodicTimer(ringTid);
      ringTid = null;
    }
    if (reportTid != null) {
      timer.cancelPeriodicTimer(reportTid);
      reportTid = null;
    }
  }

  //empty timer takes on average 100 micros
  private Consumer<Boolean> ringTimer() {
    return (_input) -> {
      List<WheelContainer> timeouts = wheelTimer.windowTick();
      long now = System.currentTimeMillis();
      for (WheelContainer c : timeouts) {
        //mean 0.7 micros
        WheelContainer rc = (WheelContainer) c;
//        logger.debug("msg data:{} timed out", rc.req.datum.getId());
        Identifier msgId = rc.datumMsgId;
        cwnd.loss(logger, msgId, now, rto(), ledbatConfig.base.MSS);
        //
        appSend.accept(rc.req, rc.req.timeout(maxAppMsgs()));
      }
      trySend(now);
    };
  }

  private long rto() {
    return rttEstimator.rto(ledbatConfig.base.INIT_RTO, ledbatConfig.base.MIN_RTO, ledbatConfig.base.MAX_RTO);
  }

  private int maxAppMsgs() {
    int kompicsEventBatching = 20;
    int ledbatEventBatching = 2;
    int ledbatMaxIncreasePerAck = 1;
    int cwndAsMsgs = (int) (cwnd.size() / ledbatConfig.base.MSS);
    int requestReadyEvents = 2 * cwndAsMsgs + kompicsEventBatching * ledbatEventBatching * ledbatMaxIncreasePerAck;
    return Math.min(ledbatConfig.bufferSize / ledbatConfig.base.MSS, requestReadyEvents);
  }

  private Consumer<Boolean> reportTimer() {
    return (_input) -> {
      logger.info("rto:{} sq1:{} sq2:{} rq:{}", 
        new Object[]{rto(), senderQ1Estimator.rto(0), senderQ2Estimator.rto(0), receiverQEstimator.rto(0)});
      cwnd.details(logger);
    };
  }

  public void bufferData(LedbatSenderEvent.Request req) {
    if (pendingData.size() < ledbatConfig.bufferSize) {
      pendingData.add(req);
    }
    trySend(System.currentTimeMillis());
  }

  public void ackData(LedbatMsg.Ack datumAckMsg) {
    Optional<WheelContainer> ringContainer = wheelTimer.cancelTimeout(datumAckMsg.datumId);
    long now = System.currentTimeMillis();
    long rtt = updateRTTs(now, datumAckMsg);
    long dataDelay = datumAckMsg.dataDelay.receive - datumAckMsg.dataDelay.send;
    if (!ringContainer.isPresent()) {
      return;
    } else {
      Identifier msgId = datumAckMsg.getId();
      cwnd.ack(logger, msgId, now, rtt, dataDelay, ledbatConfig.base.MSS);
      //mean 4-5 micros
      appSend.accept(ringContainer.get().req, ringContainer.get().req.ack(maxAppMsgs()));
      //mean 12.5 micros - 1 msg sent on average
      trySend(now);
    }
  }

  private long updateRTTs(long now, LedbatMsg.Ack datumAckMsg) {
    long senderQ1 = datumAckMsg.dataDelay.send - datumAckMsg.ledbatSendTime;
    long senderQ2 = now - datumAckMsg.ackDelay.receive;
    long receiverQ = datumAckMsg.ackDelay.send - datumAckMsg.dataDelay.receive;
    long rtt = now - datumAckMsg.ledbatSendTime;
    logger.trace("rtt:{} sender q1:{} q2:{} receiver q:{}",
      new Object[]{rtt, senderQ1, senderQ2, receiverQ});
    rttEstimator.update(rtt);
    senderQ1Estimator.update(senderQ1);
    senderQ2Estimator.update(senderQ2);
    receiverQEstimator.update(receiverQ);
    return rtt;
  }

  private void trySend(long now) {
    int batch = 2;
    while (!pendingData.isEmpty() && cwnd.canSend(logger, now, rto(), ledbatConfig.base.MSS) && batch > 0) {
      LedbatSenderEvent.Request req = pendingData.removeFirst();
      //mean 11 micros
      LedbatMsg.Datum datumMsg = new LedbatMsg.Datum(msgIds.randomId(), req.datum, now);
      networkSend.accept(datumMsg);
      //mean 0.4 micros
      Identifier datumMsgId = datumMsg.getId();
      cwnd.send(logger, datumMsgId, now, rto(), ledbatConfig.base.MSS);
//      logger.info("set timeout:{}", 3*rttEstimator.rto());
      wheelTimer.setTimeout(3 * rto(), new WheelContainer(req, datumMsgId));
      batch--;
    }
//    cwnd.details(logger);
//    rttEstimator.details(logger);
    if (pendingData.isEmpty()) {
      logger.error("empty data");
    }
  }

  public static class WheelContainer implements RingTimer.Container {

    public final LedbatSenderEvent.Request req;
    public final Identifier datumMsgId;

    public WheelContainer(LedbatSenderEvent.Request req, Identifier datumMsgId) {
      this.req = req;
      this.datumMsgId = datumMsgId;
    }

    @Override
    public Identifier getId() {
      return req.datum.getId();
    }
  }
}
