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
package se.sics.nstream.util.actuator;

import java.util.Random;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class FuzzyState {
    public final double target;
    public final Random rand;

    public FuzzyState(double target, Random rand) {
        assert 0 <= target;
        assert target < 1;
        
        this.target = target;
        this.rand = rand;
    }

    public DownloadStates state(double now) {
        assert 0 <= now;
        if(now > 1) {
            return DownloadStates.SLOW_DOWN;
        }
        double aux = rand.nextDouble();
        if (now < target) {
            double normalized = now/target;
            if (aux < normalized) {
                return DownloadStates.MAINTAIN;
            } else {
                return DownloadStates.SPEED_UP;
            }
        } else {
            double normalized = (now - target) / (1 - target);
            if (normalized < aux) {
                return DownloadStates.MAINTAIN;
            } else {
                return DownloadStates.SLOW_DOWN;
            }
        }
    }
}
