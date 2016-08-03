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
package se.sics.nstream.util.actuator;

import java.util.Random;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class FuzzyTimeoutCounterTest {
    private static final Logger LOG = LoggerFactory.getLogger("Test");
    
    @Test
    public void simpleTest() {
        
        double acceptableTimeouts = 0.5;
        Random rand = new Random(1234);
        FuzzyTimeoutCounter stc = FuzzyTimeoutCounter.getInstance(acceptableTimeouts, new Random(2345));
        int[] decisions;
        double percentage;
        double emulatedTimouts;
        
        //BELOW TARGET
        emulatedTimouts = 0.1;
        decisions = new int[3];
        for (int i = 0; i < 1000 * 1000; i++) {
            for (int j = 0; j < 100*emulatedTimouts; j++) {
                stc.timeout();
            }
            for(int j = 0; j < 100 * (1-emulatedTimouts); j++) {
                stc.success();
            }
            decisions[stc.state().ordinal()]++;
        }
        percentage = (double) decisions[0] / (decisions[0] + decisions[2]);
        LOG.info("percentage:{} maintain:{} slow_down:{} speed_up:{}", new Object[]{percentage, decisions[0], decisions[1], decisions[2]});
        Assert.assertEquals(0.2, percentage, 0.05);
        Assert.assertEquals(0, decisions[1]);
        
        emulatedTimouts = 0.2;
        decisions = new int[3];
        for (int i = 0; i < 1000 * 1000; i++) {
            for (int j = 0; j < 100*emulatedTimouts; j++) {
                stc.timeout();
            }
            for(int j = 0; j < 100 * (1-emulatedTimouts); j++) {
                stc.success();
            }
            decisions[stc.state().ordinal()]++;
        }
        percentage = (double) decisions[0] / (decisions[0] + decisions[2]);
        LOG.info("percentage:{} maintain:{} slow_down:{} speed_up:{}", new Object[]{percentage, decisions[0], decisions[1], decisions[2]});
        Assert.assertEquals(0.4, percentage, 0.05);
        Assert.assertEquals(0, decisions[1]);
        
        //ABOVE TARGET
        emulatedTimouts = 0.6;
        decisions = new int[3];
        for (int i = 0; i < 1000 * 1000; i++) {
            for (int j = 0; j < 100*emulatedTimouts; j++) {
                stc.timeout();
            }
            for(int j = 0; j < 100 * (1-emulatedTimouts); j++) {
                stc.success();
            }
            decisions[stc.state().ordinal()]++;
        }
        percentage = (double) decisions[0] / (decisions[0] + decisions[1]);
        LOG.info("percentage:{} maintain:{} slow_down:{} speed_up:{}", new Object[]{percentage, decisions[0], decisions[1], decisions[2]});
        Assert.assertEquals(1 - 0.2, percentage, 0.05);
        Assert.assertEquals(0, decisions[2]);
        
        emulatedTimouts = 0.7;
        decisions = new int[3];
        for (int i = 0; i < 1000 * 1000; i++) {
            for (int j = 0; j < 100*emulatedTimouts; j++) {
                stc.timeout();
            }
            for(int j = 0; j < 100 * (1-emulatedTimouts); j++) {
                stc.success();
            }
            decisions[stc.state().ordinal()]++;
        }
        percentage = (double) decisions[0] / (decisions[0] + decisions[1]);
        LOG.info("percentage:{} maintain:{} slow_down:{} speed_up:{}", new Object[]{percentage, decisions[0], decisions[1], decisions[2]});
        Assert.assertEquals(1-0.4, percentage, 0.05);
        Assert.assertEquals(0, decisions[2]);
    }
}
