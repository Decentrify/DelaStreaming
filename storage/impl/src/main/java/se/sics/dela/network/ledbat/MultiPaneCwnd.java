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
package se.sics.dela.network.ledbat;

import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import org.slf4j.Logger;
import se.sics.dela.network.ledbat.util.Cwnd;
import se.sics.kompics.util.Identifier;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class MultiPaneCwnd implements Cwnd {

  private final LedbatConfig config;
  private final DelayHistory delayHistory;
  private long cwnd;
  private long totalFlightSize = 0;
  private final Map<Identifier, Integer> msgToPane = new HashMap<>();
  private final Map<Integer, Long> panes = new TreeMap<>();
  private final LedbatLossCtrl lossCtrl;

  public MultiPaneCwnd(LedbatConfig config, LedbatLossCtrl lossCtrl, Logger logger) {
    this.config = config;
    this.delayHistory = new DelayHistory(config).resetDelays();
    this.lossCtrl = lossCtrl;
    this.cwnd = config.INIT_CWND * config.MSS;
  }

  @Override
  public void connectionIdle(Logger logger) {
    delayHistory.resetDelays();
  }

  @Override
  public void ack(Logger logger, Identifier msgId, long now, long rtt, long oneWayDelay, long bytesNewlyAcked) {
    lossCtrl.acked(logger, now, rtt);
    double offTarget = delayHistory.offTarget(now, oneWayDelay);
    long preCwnd = cwnd;
    if (offTarget < 0) {
      cwnd = (long) (cwnd * config.DTL_BETA);
    } else {
      cwnd = cwnd + (long) ((config.GAIN * offTarget * bytesNewlyAcked * config.MSS) / cwnd);
    }

    long maxAllowedCwnd = totalFlightSize + (long) (config.ALLOWED_INCREASE * config.MSS);
    cwnd = Math.min(cwnd, maxAllowedCwnd);
    cwnd = Math.max(cwnd, config.MIN_CWND * config.MSS);
//    logger.info("cwnd pre:{} post:{} ot:{}", new Object[]{preCwnd, cwnd, offTarget});
    updatePaneFlightSize(msgId, (-1) * bytesNewlyAcked);
    totalFlightSize -= bytesNewlyAcked;
  }

  @Override
  public void loss(Logger logger, Identifier msgId, long now, long rtt, long bytesNotToBeRetransmitted) {
    if (lossCtrl.loss(logger, now, rtt)) {
      cwnd = Math.max(cwnd / 2, config.MIN_CWND * config.MSS);
    }
    updatePaneFlightSize(msgId, (-1) * bytesNotToBeRetransmitted);
    totalFlightSize -= bytesNotToBeRetransmitted;
  }

  @Override
  public boolean canSend(long now, long rtt, long bytesToSend) {
    Integer paneNr = paneNr(now, rtt);
    long paneFlightSize = panes.get(paneNr);
    long paneCwnd = cwnd / activePanes(now, rtt);
    return (totalFlightSize + bytesToSend) <= cwnd && paneFlightSize <= paneCwnd;
  }

  @Override
  public long size() {
    return cwnd;
  }

  @Override
  public long flightSize() {
    return totalFlightSize;
  }

  @Override
  public void send(Identifier msgId, long now, long rtt, long bytesToSend) {
    Integer paneNr = paneNr(now, rtt);
    updatePaneFlightSize(paneNr, bytesToSend);
    msgToPane.put(msgId, paneNr);
    totalFlightSize += bytesToSend;
  }

  @Override
  public void details(Logger logger) {
    long queuingDelay = delayHistory.queuingDelay();
    double offTarget = delayHistory.offTarget(queuingDelay);
    int cwndMsgs = (int) cwnd / config.MSS;
    logger.info("cwnd:{} qd:{} offTarget:{}", new Object[]{cwndMsgs, queuingDelay, offTarget});
  }

  private void updatePaneFlightSize(Identifier msgId, long adjustBytes) {
    Integer paneNr = msgToPane.remove(msgId);
    if(paneNr != null) {
      updatePaneFlightSize(paneNr, adjustBytes);
    } else {
      throw new RuntimeException("ups");
    }
  }
  
  private void updatePaneFlightSize(Integer paneNr, long adjustBytes) {
    Long paneFlightSize = panes.get(paneNr);
    if (paneFlightSize != null) {
      paneFlightSize += adjustBytes;
      panes.put(paneNr, paneFlightSize);
    } else {
      throw new RuntimeException("ups");
    }
  }

  private Integer paneNr(long now, long rtt) {
    Integer paneNr = (int) ((now % rtt) / config.MIN_RTO);
    if (!panes.containsKey(paneNr)) {
      panes.put(paneNr, 0l);
    }
    return paneNr;
  }

  private int activePanes(long now, long rtt) {
    return (int) (rtt / config.MIN_RTO);
  }
}
