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
package se.sics.gvod.simulation.util;

import java.util.Random;
import se.sics.kompics.p2p.experiment.dsl.distribution.Distribution;

/**
 * @author Alex Ormenisan <aaor@sics.se>
 */
public class IntegerUniformDistribution extends Distribution<Integer> {

    private final Random random;
    private final Integer min;
    private final Integer max;

    public IntegerUniformDistribution(int min, int max, Random random) {
        super(Distribution.Type.UNIFORM, Integer.class);
        if (min < 0 || max < 0) {
            throw new RuntimeException("I can only generate positive numbers");
        }
        this.random = random;
        if (min > max) {
            this.min = max;
            this.max = min;
        } else {
            this.min = min;
            this.max = max;
        }
    }

    @Override
    public final Integer draw() {
        int u = random.nextInt(max - min + 1);
        u += min;
        return u;
    }
}
