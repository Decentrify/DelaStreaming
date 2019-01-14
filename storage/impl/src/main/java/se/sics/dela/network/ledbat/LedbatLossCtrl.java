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
public interface LedbatLossCtrl {

  /**
   *
   * @param now
   * @param rtt
   * @return signal to adjust cwnd due to loss (true - adjust, false - no adjustment)
   */
  public boolean loss(Logger logger, long now, long rtt);

  /**
   * @param now
   * @param rtt
   * @return signal to adjust cwnd due to acks (true - adjust, false - no adjustment)
   */
  public boolean acked(Logger logger, long now, long rtt);

  public static class Percentage implements LedbatLossCtrl {

    public final double lossLvl1;
    public final double lossLvl2;
    private Window active;
    private Window previous;

    public Percentage(double lossLvl1, double lossLvl2) {
      this.lossLvl1 = lossLvl1;
      this.lossLvl2 = lossLvl2;
      this.active = new Window(0);
      this.previous = new Window(0);
    }

    @Override
    public boolean loss(Logger logger, long now, long rtt) {
      checkReset(now, rtt);
      active.loss++;
      double r = (double) (previous.loss + active.loss) / (active.ack + active.loss + previous.ack + previous.loss);
      if (r >= lossLvl2 && !active.lossAdjustSignal) {
        logger.debug("loss adjust at:{}/{} previous:{}/{}", 
          new Object[]{active.loss, active.ack, previous.loss, previous.ack});
        //adjust cwnd due to loss once per rtt window - set on both active and next
        active.lossAdjustSignal = true;
        active.lossAdjustSignal = true;
        return true;
      }
      return false;
    }

    @Override
    public boolean acked(Logger logger, long now, long rtt) {
      checkReset(now, rtt);
      active.ack++;
      double r = (double) (previous.loss + active.loss) / (active.ack + active.loss + previous.ack + previous.loss);
      if (r < lossLvl1) {
        return true;
      }
      return false;
    }

    private void checkReset(long now, long rtt) {
      if (now - active.start > rtt) {
        previous = active;
        active = new Window(now);
      }
    }
  }

  static class Window {
    long start;
    int ack = 0;
    int loss = 0;
    boolean lossAdjustSignal;
    
    public Window(long start) {
      this.start = start;
    }
  }
  public static class SimpleCounter implements LedbatLossCtrl {

    public final int lossLvl;
    private int t = 0;
    private long lastChange = 0;

    public SimpleCounter(int lossLvl) {
      this.lossLvl = lossLvl;
    }

    @Override
    public boolean loss(Logger logger, long now, long rtt) {
      if (now - lastChange > rtt) {
        lastChange = now;
        t = 1;
      } else {
        t++;
      }
      return t == lossLvl;
    }

    @Override
    public boolean acked(Logger logger, long now, long rtt) {
      return true;
    }
  }

  public static LedbatLossCtrl vanillaLedbat() {
    return new SimpleCounter(1);
  }

  public static LedbatLossCtrl twoLvlPercentageLedbat(double lvl1, double lvl2) {
    return new Percentage(lvl1, lvl2);
  }
}
