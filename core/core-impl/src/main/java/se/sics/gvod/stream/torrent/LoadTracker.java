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
package se.sics.gvod.stream.torrent;

import java.util.UUID;
import org.javatuples.Triplet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.gvod.stream.congestion.PLedbatMsg;
import se.sics.kompics.ComponentProxy;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class LoadTracker {

    private static final Logger LOG = LoggerFactory.getLogger(LoadTracker.class);
    private String logPrefix = "";

    private final ComponentProxy proxy;
    private final long minQueueDelay;
    private final long maxQueueDelay;

    private UUID tId;

    private double load = 0;
    private long handlingTime = 0;
    private long receivedTime = 0;
    private long kQueueDelay = 0;

    private int slowDownTimer = 0;
    private int speedUpTimer = 0;
    private boolean canSpeedUp = false;
    private boolean shouldSlowDown = false;

    public LoadTracker(ComponentProxy proxy, long minQueueDelay, long maxQueueDelay, String logPrefix) {
        this.proxy = proxy;
        this.minQueueDelay = minQueueDelay;
        this.maxQueueDelay = maxQueueDelay;
        this.logPrefix = logPrefix;
    }

    public void trackLoad(PLedbatMsg.Response content) {
        handlingTime = System.currentTimeMillis();
        receivedTime = content.getReceivedTime();
        kQueueDelay = handlingTime - receivedTime;

        countDown();
        if (kQueueDelay < minQueueDelay) {
            load = 0;
            canSpeedUp = true;
            shouldSlowDown = false;
        } else if (maxQueueDelay < kQueueDelay) {
            load = 1;
            shouldSlowDown = true;
            canSpeedUp = false;
        } else {
            load = ((double) (kQueueDelay - minQueueDelay)) / (maxQueueDelay - minQueueDelay);
        }
    }

    private void countDown() {
        if (slowDownTimer != 0) {
            slowDownTimer--;
        }
        if (speedUpTimer != 0) {
            speedUpTimer--;
        }
    }

    public double getLoad() {
        return load;
    }

    public Triplet<Long, Long, Long> times() {
        return Triplet.with(receivedTime, handlingTime, kQueueDelay);
    }

    public boolean canSpeedUp() {
        if (speedUpTimer == 0) {
            return canSpeedUp;
        }
        return false;
    }

    public void spedUp(int timer) {
        slowDownTimer = 0;
        speedUpTimer = timer;
        canSpeedUp = false;
    }

    public boolean shouldSlowDown() {
        if (slowDownTimer == 0) {
            return shouldSlowDown;
        }
        return false;
    }

    public void slowedDown(int timer) {
        speedUpTimer = 0;
        slowDownTimer = timer;
        shouldSlowDown = false;
    }
}
