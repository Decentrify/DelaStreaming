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

import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class FuzzyDownloadTest {

    private static final Logger LOG = LoggerFactory.getLogger("Test");

    @Test
    public void simpleTest() {
        int smallSteps = 1000 * 100;
        int bigSteps = 500;
        TreeMap<Integer, Double> linkBehaviour = new TreeMap<>();
        linkBehaviour.put(500, 0.0);
        linkBehaviour.put(1000, 0.01);
        linkBehaviour.put(2000, 0.05);
        linkBehaviour.put(3000, 0.06);
        linkBehaviour.put(4000, 0.105);
        linkBehaviour.put(5000, 0.5);
        linkBehaviour.put(6000, 0.9);
        linkBehaviour.put(100*1000, 1.0);

        FuzzyDownload fd = FuzzyDownload.getInstance(new Random(1234), 0.1, 10, 100 * 1000, 2, 0.1);
        Random rand = new Random(2345);
        for (int i = 0; i < bigSteps; i++) {
            for (int j = 0; j < smallSteps; j++) {
                Integer speed = fd.speed();
                Map.Entry<Integer, Double> behaviour = linkBehaviour.ceilingEntry(speed);
                if (rand.nextDouble() < behaviour.getValue()) {
                    fd.timeout();
                } else {
                    fd.success();
                }
            }
            LOG.info("speed:{}", fd.speed());
        }
    }
}
