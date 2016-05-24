///*
// * Copyright (C) 2009 Swedish Institute of Computer Science (SICS) Copyright (C)
// * 2009 Royal Institute of Technology (KTH)
// *
// * GVoD is free software; you can redistribute it and/or
// * modify it under the terms of the GNU General Public License
// * as published by the Free Software Foundation; either version 2
// * of the License, or (at your option) any later version.
// *
// * This program is distributed in the hope that it will be useful,
// * but WITHOUT ANY WARRANTY; without even the implied warranty of
// * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// * GNU General Public License for more details.
// *
// * You should have received a reportStatus of the GNU General Public License
// * along with this program; if not, write to the Free Software
// * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
// */
//package se.sics.gvod.stream.torrent;
//
//import se.sics.gvod.stream.util.ConnectionStatus;
//
///**
// * @author Alex Ormenisan <aaor@kth.se>
// */
//public class ConnectionState {
//
//    private static final int MAX_SLOTS = 1000 * 1024;
//    
//    private final double timeoutSlowDownRate;
//    private final double normalSlowDownRate;
//    private final double speedUpRate;
//    
//    private final int startingSlots;
//    private int maxSlots;
//    private int usedSlots;
//    private int successSlots;
//    private int failedSlots;
//
//    private int increaseCounter;
//    private int decreaseCounter;
//
//    private ConnectionState(double timeoutSlowDownRate, double normalSlowDownRate, double speedUpRate,
//            int startingSlots, int usedSlots, int successSlots, int failedSlots) {
//        this.timeoutSlowDownRate = timeoutSlowDownRate;
//        this.normalSlowDownRate = normalSlowDownRate;
//        this.speedUpRate = speedUpRate;
//        
//        this.startingSlots = startingSlots;
//        this.maxSlots = startingSlots;
//        this.usedSlots = usedSlots;
//        this.successSlots = successSlots;
//        this.failedSlots = failedSlots;
//    }
//
//    public ConnectionState(double timeoutSlowDownRate, double normalSlowDownRate, double speedUpRate, int startingSlots) {
//        this(timeoutSlowDownRate, normalSlowDownRate, speedUpRate, startingSlots, 0, 0, 0);
//    }
//
//    /**
//     * @return gain
//     */
//    public int increaseSlots() {
//        if (maxSlots < MAX_SLOTS && increaseCounter == 0) {
//            int aux = (int)(maxSlots * speedUpRate);
//            maxSlots += aux;
//            increaseCounter = usedSlots + maxSlots;
//            return aux;
//        }
//        return 0;
//    }
//
//    /**
//     * @return gain
//     */
//    public int decreaseSlots() {
//        if (maxSlots > startingSlots && decreaseCounter == 0) {
//            int aux = (int)(maxSlots * normalSlowDownRate);
//            maxSlots -= aux;
//            decreaseCounter = usedSlots + maxSlots;
//            return -1 * aux;
//        }
//        return 0;
//    }
//
//    /**
//     * @return gain
//     */
//    public int halveSlots() {
//        if (maxSlots > startingSlots && decreaseCounter == 0) {
//            int aux = (int)(maxSlots * timeoutSlowDownRate);
//            maxSlots -= aux;
//            decreaseCounter = usedSlots + maxSlots;
//            return -1 * aux;
//        }
//        return 0;
//    }
//
//    private void decreaseCounters() {
//        if (increaseCounter != 0) {
//            increaseCounter--;
//        }
//        if (decreaseCounter != 0) {
//            decreaseCounter--;
//        }
//    }
//
//    public boolean available() {
//        return usedSlots < maxSlots;
//    }
//
//    public void useSlot() {
//        decreaseCounters();
//        usedSlots++;
//    }
//
//    public void releaseSlot(boolean used) {
//        usedSlots--;
//        if (used) {
//            successSlots++;
//        } else {
//            failedSlots++;
//        }
//    }
//
//    public ConnectionStatus reportStatus() {
//        return new ConnectionStatus(maxSlots, usedSlots, successSlots, failedSlots);
//    }
//
//    public void reset() {
//        successSlots = 0;
//        failedSlots = 0;
//    }
//
//    public int maxSlots() {
//        return maxSlots;
//    }
//
//    public int usedSlots() {
//        return usedSlots;
//    }
//
//    public int successSlots() {
//        return successSlots;
//    }
//
//    public int failedSlots() {
//        return failedSlots;
//    }
//}
