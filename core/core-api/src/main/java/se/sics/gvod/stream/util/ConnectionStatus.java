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
package se.sics.gvod.stream.util;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class ConnectionStatus {

    private static final int MAX_SLOTS = 10 * 1024;
    private final int startingSlots;
    private int maxSlots;
    private int usedSlots;
    private int successSlots;
    private int failedSlots;

    private ConnectionStatus(int startingSlots, int usedSlots, int successSlots, int failedSlots) {
        this.startingSlots = startingSlots;
        this.maxSlots = startingSlots;
        this.usedSlots = usedSlots;
        this.successSlots = successSlots;
        this.failedSlots = failedSlots;
    }

    public ConnectionStatus(int maxSlots) {
        this(maxSlots, 0, 0, 0);
    }

    public void increaseSlots() {
        if (maxSlots < MAX_SLOTS) {
            maxSlots++;
        }
    }

    public void decreaseSlots() {
        if (maxSlots > startingSlots) {
            maxSlots--;
        }
    }

    public void halveSlots() {
        if (maxSlots > startingSlots) {
            maxSlots = maxSlots / 2;
        }
    }

    public boolean available() {
        return usedSlots < maxSlots;
    }

    public void useSlot() {
        usedSlots++;
    }

    public void releaseSlot(boolean used) {
        usedSlots--;
        if (used) {
            successSlots++;
        } else {
            failedSlots++;
        }
    }

    public ConnectionStatus copy() {
        return new ConnectionStatus(maxSlots, usedSlots, successSlots, failedSlots);
    }

    public void reset() {
        successSlots = 0;
        failedSlots = 0;
    }

    public int maxSlots() {
        return maxSlots;
    }

    public int usedSlots() {
        return usedSlots;
    }

    public int successSlots() {
        return successSlots;
    }

    public int failedSlots() {
        return failedSlots;
    }
}
