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
import se.sics.dela.network.ledbat.LedbatConfig;

/**
 *
 */
public class RTTEstimator {
  public final int k;
  public final double alpha;
  public final double beta;
  public final int g;
  
  private long rto = -1;
  private long srtt;
  private long rttvar;
  
  public RTTEstimator(double alpha, double beta, int k, int g) {
    this.alpha = alpha;
    this.beta = beta;
    this.k = k;
    this.g = g;
  }
  
  public RTTEstimator() {
    this(0.125, 0.25, 4, Integer.MIN_VALUE);
  }
  
  public void update(long r) {
    if(rto == -1) {
      updateFirst(r);
    } else {
      updateNext(r);
    }
    rto = srtt + Math.max(g, k*rttvar);
  }
  
  private void updateFirst(long r) {
    srtt = r;
    rttvar = r/2;
  }
  
  private void updateNext(long r) {
    rttvar = (long)((1 - beta) * rttvar + (beta * Math.abs(srtt - r)));
    srtt = (long)((1 - alpha) * srtt + alpha * r);
  }
  
  public long rto(long initRTO) {
    if(rto == -1) {
      return initRTO;
    } else {
      return rto;
    }
  }
  
  public long rto(long initRTO, long minRTO, long maxRTO) {
    return Math.min(maxRTO, Math.max(minRTO, rto(initRTO)));
  }
  
  public void details(Logger logger) {
    logger.info("rto:{}, rrtvar:{}", new Object[]{rto, rttvar});
  }
  
  public static RTTEstimator instance(LedbatConfig config) {
    return new RTTEstimator(config.ALPHA, config.BETA, config.K, config.G);
  }
  
  public static RTTEstimator instance() {
    return new RTTEstimator();
  }
}
