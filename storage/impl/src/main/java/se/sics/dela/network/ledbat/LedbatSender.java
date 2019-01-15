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
import org.javatuples.Pair;
import org.slf4j.Logger;
import se.sics.dela.network.ledbat.util.Cwnd;
import se.sics.dela.network.ledbat.util.RTTEstimator;
import se.sics.dela.network.util.DatumId;
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
  private RTTEstimator receiverQ1Estimator;
  private RTTEstimator receiverQ2Estimator;
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
    receiverQ1Estimator = RTTEstimator.instance();
    receiverQ2Estimator = RTTEstimator.instance();
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
      trySend(now, timeouts.size() + 1);
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
        new Object[]{rto(), senderQ1Estimator.rto(0), senderQ2Estimator.rto(0), receiverQ1Estimator.rto(0)});
      cwnd.details(logger);
    };
  }

  public void bufferData(LedbatSenderEvent.Request req) {
    if (pendingData.size() < ledbatConfig.bufferSize) {
      pendingData.add(req);
    }
    trySend(System.currentTimeMillis(), 2);
  }

  public void multiAck(LedbatMsg.MultiAck msg) {
    long now = System.currentTimeMillis();
    long bytesAcked = 0;
    Pair<Long, Long> aux = updateBatchRTTstep1(now, msg.acks);
    for (LedbatMsg.AckVal ack : msg.acks.acks) {
      DatumId datumId = new DatumId(msg.dataId, ack.datumUnitId);
      Optional<WheelContainer> ringContainer = wheelTimer.cancelTimeout(ack.msgId);
      Pair<Long, Long> rtt = updateBatchedRTTstep2(now, msg.acks, ack, aux.getValue0(), aux.getValue1());
      if (ringContainer.isPresent()) {
        appSend.accept(ringContainer.get().req, ringContainer.get().req.ack(maxAppMsgs()));
        cwnd.ackStep1(logger, now, rtt.getValue0(), rtt.getValue1());
        cwnd.ackStep2(logger, ack.msgId, ledbatConfig.base.MSS);
        bytesAcked += ledbatConfig.base.MSS;
      } else {
        logger.info("loss");
      }
    }
    cwnd.ackStep3(logger, ledbatConfig.base.GAIN, bytesAcked);
    trySend(now, msg.acks.acks.size() + 1);
  }

  public void ackData(LedbatMsg.Ack datumAckMsg) {
    Optional<WheelContainer> ringContainer = wheelTimer.cancelTimeout(datumAckMsg.getId());
    long now = System.currentTimeMillis();
    Pair<Long, Long> rtt = updateRTTs(now, datumAckMsg);
    if (ringContainer.isPresent()) {
      //mean 4-5 micros
      appSend.accept(ringContainer.get().req, ringContainer.get().req.ack(maxAppMsgs()));
      //
      Identifier msgId = datumAckMsg.getId();
      cwnd.ackStep1(logger, now, rtt.getValue0(), rtt.getValue1());
      cwnd.ackStep2(logger, msgId, ledbatConfig.base.MSS);
      cwnd.ackStep3(logger, ledbatConfig.base.GAIN, ledbatConfig.base.MSS);
      //mean 12.5 micros - 1 msg sent on average
      trySend(now, 2);
    }
  }

  //TODO update with receiverq2
  private Pair<Long, Long> updateRTTs(long now, LedbatMsg.Ack datumAckMsg) {
    long senderQ1 = datumAckMsg.dataDelay.send - datumAckMsg.ledbatSendTime;
    long senderQ2 = now - datumAckMsg.ackDelay.receive;
    long receiverQ = datumAckMsg.ackDelay.send - datumAckMsg.dataDelay.receive;
    long rtt = now - datumAckMsg.ledbatSendTime;
    long dataDelay = datumAckMsg.dataDelay.receive - datumAckMsg.dataDelay.send;
    logger.debug("rtt:{} sender q1:{} q2:{} receiver q:{}",
      new Object[]{rtt, senderQ1, senderQ2, receiverQ});
    rttEstimator.update(rtt);
    senderQ1Estimator.update(senderQ1);
    senderQ2Estimator.update(senderQ2);
    receiverQ1Estimator.update(receiverQ);
    return Pair.with(rtt, dataDelay);
  }

  private Pair<Long, Long> updateBatchRTTstep1(long now, LedbatMsg.BatchAckVal batch) {
    long receiverQ2 = batch.rt4 - batch.rt3;
    long senderQ2 = now - batch.st3;
    receiverQ2Estimator.update(receiverQ2);
    senderQ2Estimator.update(senderQ2);
    return Pair.with(senderQ2, receiverQ2);
  }
  
  private Pair<Long, Long> updateBatchedRTTstep2(long now, LedbatMsg.BatchAckVal batch, LedbatMsg.AckVal ack, 
    long senderQ2, long receiverQ2) {
    long senderQ1 = ack.st2 - ack.st1;
    long receiverQ1 = ack.rt2 - ack.rt1;
    long batchT = batch.rt3 - ack.rt2;
    
    long rtt = now - ack.st1 - batchT;
    long dataDelay = ack.rt1 - ack.st2;
    logger.debug("rtt:{} sender q1:{} q2:{} receiver q1:{} q2:{} bt:{} dd:{}",
      new Object[]{rtt, senderQ1, senderQ2, receiverQ1, receiverQ2, batchT, dataDelay});
    rttEstimator.update(rtt);
    senderQ1Estimator.update(senderQ1);
    receiverQ1Estimator.update(receiverQ1);
    return Pair.with(rtt, dataDelay);
  }
  
  private void trySend(long now, int batch) {
    while (!pendingData.isEmpty() && cwnd.canSend(logger, now, rto(), ledbatConfig.base.MSS) && batch > 0) {
      LedbatSenderEvent.Request req = pendingData.removeFirst();
      //mean 11 micros
      LedbatMsg.Datum datumMsg = new LedbatMsg.Datum(msgIds.randomId(), req.datum, now);
      networkSend.accept(datumMsg);
      //mean 0.4 micros
      Identifier datumMsgId = datumMsg.getId();
      cwnd.send(logger, datumMsgId, now, rto(), ledbatConfig.base.MSS);
      // 2* - because of WheelTimer 0.5*to account for increase(congestion) + 20 (batching of acks on receiver side)
      long msgRTO = (long) (2.5 * rto()) + 20;
      wheelTimer.setTimeout(msgRTO, new WheelContainer(req, datumMsgId));
      batch--;
    }
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
      return datumMsgId;
    }
  }
}
