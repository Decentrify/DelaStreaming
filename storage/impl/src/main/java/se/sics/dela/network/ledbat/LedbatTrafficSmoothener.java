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

import org.slf4j.Logger;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class LedbatTrafficSmoothener {
  public final long windowPeriod;
  private int windowSize;
  
  private int inFlight = 0;
  
  public LedbatTrafficSmoothener(long windowPeriod) {
    this.windowPeriod = windowPeriod;
  }
  public void update(Logger logger, int cwndAsMsg, long rtt) {
    if(rtt <= windowPeriod) {
      windowSize = cwndAsMsg;
    } else {
      double nrWindowsPerRtt = Math.ceil(rtt/windowPeriod);
      windowSize = (int)(Math.ceil(cwndAsMsg/nrWindowsPerRtt));
    }
    inFlight = 0;
    logger.info("cwnd:{} rtt:{} windowSize:{}", new Object[]{cwndAsMsg, rtt, windowSize});
  }
  
  public boolean canSend() {
    return inFlight < windowSize;
  }
  
  public void send() {
    inFlight++;
  }
}
