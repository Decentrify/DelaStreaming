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
package se.sics.dela.network.ledbat;

import se.sics.kompics.config.Config;
import se.sics.ktoolbox.util.trysf.Try;

/**
 *
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class LedbatConfig {
  /**
   * https://tools.ietf.org/html/rfc6817#ref-uTorrent
   */
  public final int CURRENT_FILTER = 10;
  public final int BASE_HISTORY = 10;
  public final int INIT_CWND = 2;
  public final int MIN_CWND = 2;
  public final int MSS = 1000;
  public final int TARGET = 100; //100ms
  public final double GAIN = 1; //same as TCP
  public final double DTL_BETA = 0.99;
  public double ALLOWED_INCREASE = 1;
  
  /**
   * rtt estimator
   * https://tools.ietf.org/html/rfc6298
   */
  public final long INIT_RTO = 1000;
  public final long MIN_RTO = 20; //0.2s
  public final long MAX_RTO = 60000;//60s
  public final int K = 4;
  public final double ALPHA = 0.125;
  public final double BETA = 0.25;
  public final int G = Integer.MIN_VALUE; // not using clock granularity

  public LedbatConfig() {
  }
  
  public static Try<LedbatConfig> instance(Config config) {
    return new Try.Success(new LedbatConfig());
  }
}
