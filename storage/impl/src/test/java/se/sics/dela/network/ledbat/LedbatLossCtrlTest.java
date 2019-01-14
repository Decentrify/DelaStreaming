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

import java.util.stream.IntStream;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class LedbatLossCtrlTest {
  private Logger LOG = LoggerFactory.getLogger(LedbatLossCtrlTest.class);
  
  @Test
  public void testPercentage1() {
    LedbatLossCtrl.Percentage lossCtrl = new LedbatLossCtrl.Percentage(0.5, 0.8);
    long rtt = 49;
    Assert.assertTrue(lossCtrl.acked(LOG, 50, rtt));
    Assert.assertTrue(lossCtrl.acked(LOG, 100, rtt));
    
    IntStream.range(0, 20).forEach((i) -> Assert.assertTrue(lossCtrl.acked(LOG, 150, rtt)));
    IntStream.range(0, 79).forEach((i) -> Assert.assertFalse(lossCtrl.loss(LOG, 200, rtt)));
    Assert.assertTrue(lossCtrl.loss(LOG, 200, rtt));
  }
  
  @Test
  public void testPercentage2() {
    LedbatLossCtrl.Percentage lossCtrl = new LedbatLossCtrl.Percentage(0.5, 0.8);
    long rtt = 49;
    Assert.assertTrue(lossCtrl.acked(LOG, 50, rtt));
    Assert.assertTrue(lossCtrl.acked(LOG, 100, rtt));
    
    IntStream.range(0, 20).forEach((i) -> Assert.assertTrue(lossCtrl.acked(LOG, 150, rtt)));
    Assert.assertFalse(lossCtrl.loss(LOG, 200, rtt));
  }
}
