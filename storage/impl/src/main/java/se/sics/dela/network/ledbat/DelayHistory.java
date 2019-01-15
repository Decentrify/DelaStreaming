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

import java.util.Arrays;
import org.slf4j.Logger;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class DelayHistory {

  private final LedbatConfig config;
  private long[] currentDelays;
  private int currentDelaysPointer = -1;
  private long[] baseDelays;
  private int baseDelaysPointer = -1;
  private long lastRolloverMinute;

  public DelayHistory(LedbatConfig config) {
    this.config = config;
    this.currentDelays = new long[config.CURRENT_FILTER];
    this.baseDelays = new long[config.BASE_HISTORY];
  }

  public DelayHistory resetDelays() {
    lastRolloverMinute = Long.MIN_VALUE;
    for (int i = 0; i < config.CURRENT_FILTER; i++) {
      currentDelays[i] = Long.MAX_VALUE;
    }
    for (int i = 0; i < config.BASE_HISTORY; i++) {
      baseDelays[i] = Long.MAX_VALUE;
    }
    return this;
  }

  public void update(long now, long oneWayDelay) {
    updateBaseDelay(now, oneWayDelay);
    updateCurrentDelay(oneWayDelay);
  }
  
  public double offTarget() {
    long queuingDelay = queuingDelay();
    double offTarget = offTarget(queuingDelay);
    return offTarget;
  }
  
  public double offTarget(long queuingDelay) {
    return (config.TARGET - queuingDelay) / config.TARGET;
  }
  
  public long queuingDelay() {
    return Arrays.stream(currentDelays).min().getAsLong()
      - Arrays.stream(baseDelays).min().getAsLong();
  }

  private void updateCurrentDelay(long oneWayDelay) {
    currentDelaysPointer += 1;
    if (currentDelaysPointer >= config.CURRENT_FILTER) {
      currentDelaysPointer = 0;
    }
    currentDelays[currentDelaysPointer] = oneWayDelay;
  }
  
  private void updateBaseDelay(long now, long oneWayDelay) {
    long nowMinute = roundToMinute(now);
    if (nowMinute > lastRolloverMinute) {
      baseDelaysPointer += 1;
      if (baseDelaysPointer >= config.BASE_HISTORY) {
        baseDelaysPointer = 0;
      }
      baseDelays[baseDelaysPointer] = oneWayDelay;
      lastRolloverMinute = roundToMinute(lastRolloverMinute);
    } else {
      baseDelays[baseDelaysPointer] = Math.min(baseDelays[baseDelaysPointer], oneWayDelay);
    }
  }

  private long roundToMinute(long timeInMillis) {
    return timeInMillis / 60000;
  }
  
  public void details(Logger logger) {
    long queuingDelay = queuingDelay();
    double offTarget = offTarget(queuingDelay);
    logger.info("qd:{} ot:{}",
      new Object[]{queuingDelay, offTarget});
  }
}
