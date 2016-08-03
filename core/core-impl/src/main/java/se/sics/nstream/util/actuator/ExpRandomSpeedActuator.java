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

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class ExpRandomSpeedActuator {

    public final int min;
    public final int max;
    public final int baseChange;
    public final double resetChance;
    private final Random rand;
    private boolean accelerate;
    private double changeRate;
    private int speed;

    public ExpRandomSpeedActuator(int min, int max, int baseChange, Random rand, double resetChance) {
        this.min = min;
        this.max = max;
        this.baseChange = baseChange;
        this.rand = rand;
        this.resetChance = resetChance;
        this.accelerate = true;
        this.changeRate = 1;
        this.speed = min;
    }

    public void up() {
        reset();
        if (accelerate) {
            changeRate = changeRate * 2;
        } else {
            accelerate = true;
            changeRate = changeRate / 2;
        }
        if (baseChange > Integer.MAX_VALUE / changeRate) {
            speed = max;
        } else {
            int change = (int) (baseChange * changeRate);
            speed = Math.min(speed + change, max);
        }
    }

    public void down() {
        reset();
        if (accelerate) {
            accelerate = false;
            changeRate = changeRate / 2;
        } else {
            changeRate = changeRate * 2;
        }
        if (baseChange > Integer.MAX_VALUE / changeRate) {
            speed = min;
        } else {
            int change = (int) (baseChange * changeRate);
            speed = Math.max(speed - change, min);
        }
    }
    
    private void reset() {
        double aux = rand.nextDouble();
        if(aux < resetChance) {
            changeRate = 1;
        }
    }

    public int speed() {
        return speed;
    }
}
