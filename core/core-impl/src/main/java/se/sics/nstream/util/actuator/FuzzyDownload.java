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
import org.javatuples.Triplet;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class FuzzyDownload {

    private final FuzzyTimeoutCounter window;
    private final ExpRandomSpeedActuator speedActuator;
    private int currentWindowSize;
    private int lastWindowSize;
    private int waitingTime;
    private int usedSlots;
    private DownloadStates current;
    private DownloadStates old;
    private boolean changed;

    private FuzzyDownload(FuzzyTimeoutCounter window, ExpRandomSpeedActuator speedActuator) {
        this.window = window;
        this.speedActuator = speedActuator;
        this.currentWindowSize = speedActuator.speed();
        this.lastWindowSize = currentWindowSize;
        this.waitingTime = currentWindowSize;
        this.usedSlots = 0;
        this.current = DownloadStates.MAINTAIN;
        this.old = DownloadStates.MAINTAIN;
        this.changed = false;
    }

    public boolean changed() {
        boolean aux = changed;
        changed = false;
        return aux;
    }

    public void success() {
        adjust();
        window.success();
        waitingTime--;
        releaseSlot();
    }

    public void timeout() {
        adjust();
        window.timeout();
        waitingTime--;
        releaseSlot();
    }

    private void releaseSlot() {
        if (usedSlots > 0) {
            usedSlots--;
        }
    }

    public boolean availableSlot() {
        return usedSlots < currentWindowSize;
    }

    public void useSlot() {
        usedSlots++;
    }

    public int windowSize() {
        return currentWindowSize;
    }

    private void adjust() {
        if (waitingTime == 0) {
            double load = usedSlots / currentWindowSize;
            switch (window.state()) {
                case MAINTAIN:
                    if (load < 0.5) {
                        slowDown();
                    } else {
                        maintain();
                    }
                    break;
                case SLOW_DOWN:
                    slowDown();
                    break;
                case SPEED_UP:
                    if (load < 0.9) {
                        maintain();
                    } else {
                        speedUp();
                    }
                    break;
            }
        }
    }

    private void maintain() {
        old = current;
        current = DownloadStates.MAINTAIN;
        lastWindowSize = currentWindowSize;
        waitingTime = currentWindowSize;
    }

    private void slowDown() {
        old = current;
        current = DownloadStates.SLOW_DOWN;
        speedActuator.down();
        lastWindowSize = currentWindowSize;
        currentWindowSize = speedActuator.speed();
        waitingTime = currentWindowSize + lastWindowSize;
        if (usedSlots > currentWindowSize) {
            usedSlots = currentWindowSize;
        }
        if (!old.equals(DownloadStates.SLOW_DOWN)) {
            changed = true;
        }
    }

    private void speedUp() {
        old = current;
        current = DownloadStates.SPEED_UP;
        speedActuator.up();
        lastWindowSize = currentWindowSize;
        currentWindowSize = speedActuator.speed();
        waitingTime = currentWindowSize + lastWindowSize;
        if (!old.equals(DownloadStates.SLOW_DOWN)) {
            changed = true;
        }
    }

    public String report() {
        return "cw:" + currentWindowSize + " used:" + usedSlots;
    }
    
    public void externalSlowDown() {
        slowDown();
    }

    public static FuzzyDownload getInstance(Random rand, Triplet<Double, Double, Double> targetedTimeout, int minSpeed, int maxSpeed, int baseChange, double resetChance) {
        FuzzyTimeoutCounter ftc = FuzzyTimeoutCounter.getInstance(targetedTimeout, rand);
        ExpRandomSpeedActuator ersa = new ExpRandomSpeedActuator(minSpeed, maxSpeed, baseChange, rand, resetChance);
        return new FuzzyDownload(ftc, ersa);
    }
}
