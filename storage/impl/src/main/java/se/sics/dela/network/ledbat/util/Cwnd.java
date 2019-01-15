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
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
 */
package se.sics.dela.network.ledbat.util;

import org.slf4j.Logger;
import se.sics.dela.network.ledbat.DelayHistory;
import se.sics.dela.network.ledbat.LedbatConfig;
import se.sics.dela.network.ledbat.LedbatLossCtrl;
import se.sics.kompics.util.Identifier;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public interface Cwnd {

  public void connectionIdle(Logger logger);

  public void ackStep1(Logger logger, long now, long rtt, long oneWayDelay);

  public void ackStep2(Logger logger, Identifier msgId, long msgBytes);

  public void ackStep3(Logger logger, double cwndGain, long bytesNewlyAcked);

  public void loss(Logger logger, Identifier msgId, long now, long rtt, long bytesNotToBeRetransmitted);

  public boolean canSend(Logger logger, long now, long rtt, long bytesToSend);

  public long size();

  public long flightSize();

  public void send(Logger logger, Identifier msgId, long now, long rtt, long bytesToSend);

  public void details(Logger logger);

  public static class Basic implements Cwnd {

    private final LedbatConfig config;
    private final DelayHistory delayHistory;
    private long cwnd;
    private long flightSize;
    private final LedbatLossCtrl lossCtrl;

    public Basic(LedbatConfig config, LedbatLossCtrl lossCtrl, Logger logger) {
      this.config = config;
      this.delayHistory = new DelayHistory(config);
      this.lossCtrl = lossCtrl;
      init(logger);
    }

    private void init(Logger logger) {
      delayHistory.resetDelays();
      cwnd = config.INIT_CWND * config.MSS;
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
      //nothing
    }

    @Override
    public void ackStep3(Logger logger, double cwndGain, long bytesNewlyAcked) {
      adjustCwnd(logger, cwndGain, delayHistory.offTarget(), bytesNewlyAcked);
      flightSize -= bytesNewlyAcked;
    }

    private void adjustCwnd(Logger logger, double cwndGain, double offTarget, long bytesNewlyAcked) {
      long aux = cwnd;
      if (offTarget < 0) {
        cwnd = (long) (cwnd * config.DTL_BETA);
      } else {
        cwnd = cwnd + (long) ((cwndGain * offTarget * bytesNewlyAcked * config.MSS) / cwnd);
      }

      long maxAllowedCwnd = flightSize + (long) (config.ALLOWED_INCREASE * config.MSS);
      cwnd = Math.min(cwnd, maxAllowedCwnd);
      cwnd = Math.max(cwnd, config.MIN_CWND * config.MSS);
      logger.info("cwnd pre:{} post:{} ot:{}", new Object[]{aux, cwnd, offTarget});
    }

    @Override
    public void loss(Logger logger, Identifier msgId, long now, long rtt, long bytesNotToBeRetransmitted) {
      if (lossCtrl.loss(logger, now, rtt)) {
        cwnd = Math.max(cwnd / 2, config.MIN_CWND * config.MSS);
      }
      flightSize -= bytesNotToBeRetransmitted;
    }

    @Override
    public boolean canSend(Logger logger, long now, long rtt, long bytesToSend) {
      return (flightSize + bytesToSend) <= cwnd;
    }

    @Override
    public long size() {
      return cwnd;
    }

    @Override
    public long flightSize() {
      return flightSize;
    }

    @Override
    public void send(Logger logger, Identifier msgId, long now, long rtt, long bytesToSend) {
      flightSize += bytesToSend;
    }

    @Override
    public void details(Logger logger) {
      long queuingDelay = delayHistory.queuingDelay();
      double offTarget = delayHistory.offTarget(queuingDelay);
      int cwndMsgs = (int) cwnd / config.MSS;
      logger.info("cwnd:{} qd:{} offTarget:{}", new Object[]{cwndMsgs, queuingDelay, offTarget});
    }
  }
}
