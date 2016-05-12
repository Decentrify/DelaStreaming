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

import org.javatuples.Quartet;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class ConnectionStatus {

    private int maxSlots;
    private int usedSlots;
    private int successSlots;
    private int failedSlots;

    public ConnectionStatus(int maxSlots) {
        this.maxSlots = maxSlots;
        usedSlots = 0;
        successSlots = 0;
        failedSlots = 0;
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

    public Quartet<Integer, Integer, Integer, Integer> report() {
        return Quartet.with(maxSlots, usedSlots, successSlots, failedSlots);
    }
}
