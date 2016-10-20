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

import java.util.Arrays;
import java.util.Random;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class ExponentialSpeedActuatorTest {

    private static final Logger LOG = LoggerFactory.getLogger("Test");

    @Test
    public void learnOptimumSpeed() {
        LOG.info("***********************************************************");
        int optimumSpeed = 80000;
        int smallSteps = 50;
        int bigSteps = 10;
        int[] speeds;;
        ExpRandomSpeedActuator esa = new ExpRandomSpeedActuator(10, 1000 * 1000, 100, new Random(1234), 0.1);

        for (int i = 0; i < bigSteps; i++) {
            speeds = new int[smallSteps];
            speeds[0] = esa.speed();
            for (int j = 1; j < smallSteps; j++) {
                if (speeds[j - 1] < optimumSpeed) {
                    esa.up();
                } else {
                    esa.down();
                }
                speeds[j] = esa.speed();
            }
            int[] finalSpeeds = Arrays.copyOfRange(speeds, smallSteps - 10, smallSteps);
            LOG.info("last speeds:{}", finalSpeeds);
        }
    }
    
    @Test
    public void learnOptimumSpeed2() {
        LOG.info("***********************************************************");
        int optimumSpeed = 3000;
        int smallSteps = 50;
        int bigSteps = 10;
        int[] speeds;;
        ExpRandomSpeedActuator esa = new ExpRandomSpeedActuator(10, 100 * 1000, 2, new Random(1234), 0.1);

        for (int i = 0; i < bigSteps; i++) {
            speeds = new int[smallSteps];
            speeds[0] = esa.speed();
            for (int j = 1; j < smallSteps; j++) {
                if (speeds[j - 1] < optimumSpeed) {
                    esa.up();
                } else {
                    esa.down();
                }
                speeds[j] = esa.speed();
            }
            int[] finalSpeeds = Arrays.copyOfRange(speeds, smallSteps - 10, smallSteps);
            LOG.info("last speeds:{}", finalSpeeds);
        }

    }
}
