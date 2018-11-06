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

import java.util.Arrays;
import org.slf4j.Logger;
import se.sics.dela.network.ledbat.LedbatConfig;

/**
 *
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class Cwnd {

  private final LedbatConfig config;
  private long[] currentDelays;
  private int currentDelaysPointer = -1;
  private long[] baseDelays;
  private int baseDelaysPointer = -1;
  private long cwnd;
  private long flightSize;
  private long lastLoss;
  private long lastRolloverMinute;

  public Cwnd(LedbatConfig config) {
    this.config = config;
    init();
  }

  private void init() {
    currentDelays = new long[config.CURRENT_FILTER];
    baseDelays = new long[config.BASE_HISTORY];
    resetDelays();
    cwnd = config.INIT_CWND * config.MSS;
  }

  public void connectionIdle() {
    resetDelays();
  }

  public void ack(long now, long oneWayDelay, long bytesNewlyAcked) {
    updateBaseDelay(now, oneWayDelay);
    updateCurrentDelay(oneWayDelay);

    long queuingDelay = Arrays.stream(currentDelays).min().getAsLong()
      - Arrays.stream(baseDelays).min().getAsLong();
    double offTarget = (config.TARGET - queuingDelay) / config.TARGET;
    if (offTarget < 0) {
      cwnd = (long) (cwnd * config.DTL_BETA);
    } else {
      cwnd = cwnd + (long) ((config.GAIN * offTarget * bytesNewlyAcked * config.MSS) / cwnd);
    }

    long maxAllowedCwnd = flightSize + (long) (config.ALLOWED_INCREASE * config.MSS);
    cwnd = Math.min(cwnd, maxAllowedCwnd);
    cwnd = Math.max(cwnd, config.MIN_CWND * config.MSS);
    flightSize -= bytesNewlyAcked;
  }

  public void loss(long now, long rtt, long bytesNotToBeRetransmitted) {
    if (now - lastLoss > rtt) {
      lastLoss = now;
      cwnd = Math.max(cwnd / 2, config.MIN_RTO * config.MSS);
    }
    flightSize -= bytesNotToBeRetransmitted;
  }
  
  public boolean canSend(long bytesToSend) {
    return (flightSize + bytesToSend) <= cwnd;
  }
  
  public long size() {
    return cwnd;
  }
  
  public void send(long bytesToSend) {
    flightSize += bytesToSend;
  }

  private void resetDelays() {
    lastRolloverMinute = Long.MIN_VALUE;
    for (int i = 0; i < config.CURRENT_FILTER; i++) {
      currentDelays[i] = Long.MAX_VALUE;
    }
    for (int i = 0; i < config.BASE_HISTORY; i++) {
      baseDelays[i] = Long.MAX_VALUE;
    }
  }

  private void updateCurrentDelay(long r) {
    currentDelaysPointer += 1;
    if (currentDelaysPointer >= config.CURRENT_FILTER) {
      currentDelaysPointer = 0;
    }
    currentDelays[currentDelaysPointer] = r;
  }

  private void updateBaseDelay(long now, long r) {
    long nowMinute = roundToMinute(now);
    if (nowMinute > lastRolloverMinute) {
      baseDelaysPointer += 1;
      if (baseDelaysPointer >= config.BASE_HISTORY) {
        baseDelaysPointer = 0;
      }
      baseDelays[baseDelaysPointer] = r;
      lastRolloverMinute = roundToMinute(lastRolloverMinute);
    } else {
      baseDelays[baseDelaysPointer] = Math.min(baseDelays[baseDelaysPointer], r);
    }
  }

  private long roundToMinute(long timeInMillis) {
    return timeInMillis / 60000;
  }
}
