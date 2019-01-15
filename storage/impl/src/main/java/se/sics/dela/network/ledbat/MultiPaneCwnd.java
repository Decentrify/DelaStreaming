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
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
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
  private final Map<Identifier, Pane> msgToPane = new HashMap<>();
  private final LedbatLossCtrl lossCtrl;
  private boolean paneFullSmoother = false;
  private final LinkedList<Pane> pastPanes = new LinkedList<>();
  private Pane currentPane;

  public MultiPaneCwnd(LedbatConfig config, LedbatLossCtrl lossCtrl, Logger logger) {
    this.config = config;
    this.delayHistory = new DelayHistory(config).resetDelays();
    this.lossCtrl = lossCtrl;
    this.cwnd = config.INIT_CWND * config.MSS;
    this.currentPane = new Pane(System.currentTimeMillis(), cwnd);
  }

  @Override
  public void connectionIdle(Logger logger) {
    delayHistory.resetDelays();
  }

  @Override
  public void ackStep1(Logger logger, long now, long rtt, long oneWayDelay) {
    lossCtrl.acked(logger, now, rtt);
    delayHistory.update(now, oneWayDelay);
  }
  
  @Override
  public void ackStep2(Logger logger, Identifier msgId, long msgBytes) {
    reducePaneFlightSize(msgId, msgBytes);
  }
  
  @Override
  public void ackStep3(Logger logger, double cwndGain, long bytesNewlyAcked) {
    adjustCwnd(logger, cwndGain, delayHistory.offTarget(), bytesNewlyAcked);
    totalFlightSize -= bytesNewlyAcked;
  }

  private void adjustCwnd(Logger logger, double gain, double offTarget, long bytesNewlyAcked) {
    long preCwnd = cwnd;
    if (offTarget < 0) {
      cwnd = (long) (cwnd * config.DTL_BETA);
    } else {
      cwnd = cwnd + (long) ((gain * offTarget * bytesNewlyAcked * config.MSS) / cwnd);
    }

    long maxAllowedCwnd;
    if (paneFullSmoother) {
      //if the pane is full we are posponing sending msgs 
      //in order to smooth a bit the pane sizes 
      //so that we better utilize the bandwidth over the whole rtt window
      maxAllowedCwnd = preCwnd + (long) (config.ALLOWED_INCREASE * config.MSS);
    } else {
      maxAllowedCwnd = totalFlightSize + (long) (config.ALLOWED_INCREASE * config.MSS);
    }
    cwnd = Math.min(cwnd, maxAllowedCwnd);
    cwnd = Math.max(cwnd, config.MIN_CWND * config.MSS);
    if (preCwnd > cwnd) {
      logger.info("cwnd pre:{} post:{} ot:{} flight:{}", new Object[]{preCwnd, cwnd, offTarget, totalFlightSize});
    }
  }


  @Override
  public void loss(Logger logger, Identifier msgId, long now, long rtt, long bytesNotToBeRetransmitted) {
    if (lossCtrl.loss(logger, now, rtt)) {
      cwnd = Math.max(cwnd / 2, config.MIN_CWND * config.MSS);
      lossDetails(logger);
    }
    reducePaneFlightSize(msgId, bytesNotToBeRetransmitted);
    totalFlightSize -= bytesNotToBeRetransmitted;
  }

  private void lossDetails(Logger logger) {
    int cwndMsgs = (int) cwnd / config.MSS;
    int flightSizeAsMsgs = (int) totalFlightSize / config.MSS;
    logger.info("loss");
    details(logger);
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
  public boolean canSend(Logger logger, long now, long rtt, long bytesToSend) {
    checkNewPane(now, rtt);
    boolean paneFull = isPaneFull(logger, now, rtt);
    if (totalFlightSize < cwnd) {
      if (paneFull) {
        paneFullSmoother = true;
      }
      return !paneFull;
    } else {
      paneFullSmoother = false;
      return false;
    }
  }

  @Override
  public void send(Logger logger, Identifier msgId, long now, long rtt, long bytesToSend) {
    currentPane.inc(bytesToSend);
    msgToPane.put(msgId, currentPane);
    totalFlightSize += bytesToSend;
  }

  @Override
  public void details(Logger logger) {
    int cwndMsgs = (int) cwnd / config.MSS;
    int flightSizeAsMsgs = (int) totalFlightSize / config.MSS;
    delayHistory.details(logger);
    paneDetails(logger);
    logger.info("cwnd:{} flightSize:{} nrPanes:{}",
      new Object[]{cwndMsgs, flightSizeAsMsgs, pastPanes.size() + 1});
  }
  
  private boolean isPaneFull(Logger logger, long now, long rtt) {
    int activePanes = activePanes(now, rtt);
    long paneCwnd = cwnd / activePanes + (cwnd > currentPane.cwndAtStart ? cwnd - currentPane.cwndAtStart : 0);
    return currentPane.paneFlightSize >= paneCwnd;
  }

  private void paneDetails(Logger logger) {
    StringBuilder sb = new StringBuilder();
    pastPanes.forEach((pane) -> sb.append(pane.paneFlightSize).append("/").append(pane.sent).append(" "));
    sb.append(currentPane.paneFlightSize).append("/").append(currentPane.sent);
    logger.info("panes:{}", sb);
    cleanPastPanes();
  }
  
  private void cleanPastPanes() {
    Iterator<Pane> it = pastPanes.iterator();
    while(it.hasNext()) {
      Pane p = it.next();
      if(p.paneFlightSize > 0) {
        break;
      }
      it.remove();
    }
  }

  private void reducePaneFlightSize(Identifier msgId, long adjustBytes) {
    Pane pane = msgToPane.remove(msgId);
    if (pane != null) {
      pane.red(adjustBytes);
    } else {
      throw new RuntimeException("ups");
    }
  }

  private void checkNewPane(long now, long rtt) {
    if (now - currentPane.startTime >= config.MIN_RTO) {
      pastPanes.add(currentPane);
      currentPane = new Pane(now, cwnd);
    }
  }

  private int activePanes(long now, long rtt) {
    return (int) (rtt / config.MIN_RTO);
  }

  static class Pane {
    long cwndAtStart;
    long startTime;
    long paneFlightSize = 0;
    int sent = 0;

    public Pane(long startTime, long cwnd) {
      this.startTime = startTime;
      this.cwndAtStart = cwnd;
    }
    
    void inc(long bytes) {
      sent++;
      paneFlightSize += bytes;
    }
    
    void red(long bytes) {
      paneFlightSize -= bytes;
    }
  }
}
