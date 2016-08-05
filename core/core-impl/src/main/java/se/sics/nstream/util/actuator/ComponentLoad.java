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

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class ComponentLoad {

    public final long maxQueueDelay;
    public final long maxCheckPeriod;
    private final FuzzyState fuzzyState;
    private long checkPeriod;
    private DownloadStates state;
    private long lastQueueDelay;
    private Map<String, Integer> transferSize = new HashMap<>();
    private Map<String, Integer> bufferSize = new HashMap<>();

    public ComponentLoad() {
        this.maxQueueDelay = ComponentLoadConfig.maxQueueDelay;
        this.maxCheckPeriod = ComponentLoadConfig.maxCheckPeriod;
        Random rand = new Random(ComponentLoadConfig.seed);
        double targetLoad = (double) ComponentLoadConfig.targetQueueDelay / maxQueueDelay;
        this.fuzzyState = new FuzzyState(targetLoad, rand);
        this.checkPeriod = maxQueueDelay;
        this.lastQueueDelay = 0;
        this.state = DownloadStates.MAINTAIN;
    }

    public void setBufferSize(String bufferName, int size) {
        bufferSize.put(bufferName, size);
    }

    public void setTransferSize(String fileName, int size) {
        transferSize.put(fileName, size);
    }

    public void adjustState(long queueDelay) {
        lastQueueDelay = queueDelay;

        DownloadStates old = state;
        DownloadStates queue = queue(queueDelay);
        DownloadStates buffer = buffer();
        DownloadStates transfer = transfer();
        if(queue.equals(DownloadStates.SLOW_DOWN) || buffer.equals(DownloadStates.SLOW_DOWN) || transfer.equals(DownloadStates.SLOW_DOWN)) {
            state = DownloadStates.SLOW_DOWN;
        } else if(queue.equals(DownloadStates.MAINTAIN) || buffer.equals(DownloadStates.MAINTAIN) || transfer.equals(DownloadStates.MAINTAIN)) {
            state = DownloadStates.MAINTAIN;
        } else {
            state = DownloadStates.SPEED_UP;
        }
        
        if (!old.equals(state)) {
            checkPeriod = maxQueueDelay;
        } else {
            checkPeriod = maxCheckPeriod;
        }
    }

    private DownloadStates queue(long queueDelay) {
        if (queueDelay > maxQueueDelay) {
            return DownloadStates.SLOW_DOWN;
        }
        return fuzzyState.state((double) queueDelay / maxQueueDelay);
    }
    
    private DownloadStates buffer() {
        int totalBuffer = 0;
        for (Integer bS : bufferSize.values()) {
            totalBuffer += bS;
        }
        double bufferUtilization = (double)totalBuffer / ComponentLoadConfig.maxBuffer;
        if (bufferUtilization >= 1) {
            return DownloadStates.SLOW_DOWN;
        } else if (bufferUtilization > 0.5) {
            return DownloadStates.MAINTAIN;
        } else {
            return DownloadStates.SPEED_UP;
        }
    }

    private DownloadStates transfer() {
        int totalTransfer = 0;
        for (Integer tS : transferSize.values()) {
            totalTransfer += tS;
        }
        double transferUtilization = (double)totalTransfer / ComponentLoadConfig.maxTransfer;
        if (transferUtilization >= 1) {
            return DownloadStates.SLOW_DOWN;
        } else if (transferUtilization > 0.5) {
            return DownloadStates.MAINTAIN;
        } else {
            return DownloadStates.SPEED_UP;
        }
    }
    public DownloadStates state() {
        return state;
    }

    public long checkPeriod() {
        return checkPeriod;
    }

    public void outsideChange() {
        checkPeriod = maxQueueDelay;
    }

    public ComponentLoadReport report() {
        return new ComponentLoadReport(lastQueueDelay, transferSize, bufferSize);
    }
}
