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
import se.sics.kompics.ComponentProxy;
import se.sics.ktoolbox.util.tracking.load.NetworkQueueLoadProxy;
import se.sics.ktoolbox.util.tracking.load.QueueLoadConfig;
import se.sics.ktoolbox.util.tracking.load.util.StatusState;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class ComponentLoadTracking {

    private StatusState state;
    private final NetworkQueueLoadProxy networkQueueLoad;
    private final Map<String, Integer> transferSize = new HashMap<>();
    private final Map<String, Integer> bufferSize = new HashMap<>();

    public ComponentLoadTracking(String componentName, ComponentProxy proxy, QueueLoadConfig queueLoadConfig) {
        this.networkQueueLoad = new NetworkQueueLoadProxy(componentName+"network", proxy, queueLoadConfig);
        Random rand = new Random(ComponentLoadConfig.seed);
        this.state = StatusState.MAINTAIN;
    }
    
    public void startTracking() {
        networkQueueLoad.startTracking();
    }

    public void setBufferSize(String bufferName, int size) {
        bufferSize.put(bufferName, size);
    }

    public void setTransferSize(String fileName, int size) {
        transferSize.put(fileName, size);
    }

    public StatusState state() {
        StatusState queue = networkQueueLoad.state();
        StatusState buffer = buffer();
        StatusState transfer = transfer();
        if(queue.equals(StatusState.SLOW_DOWN) || buffer.equals(StatusState.SLOW_DOWN) || transfer.equals(StatusState.SLOW_DOWN)) {
            state = StatusState.SLOW_DOWN;
        } else if(queue.equals(StatusState.MAINTAIN) || buffer.equals(StatusState.MAINTAIN) || transfer.equals(StatusState.MAINTAIN)) {
            state = StatusState.MAINTAIN;
        } else {
            state = StatusState.SPEED_UP;
        }
        return state;
    }
    
    private StatusState buffer() {
        int totalBuffer = 0;
        for (Integer bS : bufferSize.values()) {
            totalBuffer += bS;
        }
        double bufferUtilization = (double)totalBuffer / ComponentLoadConfig.maxBuffer;
        if (bufferUtilization >= 1) {
            return StatusState.SLOW_DOWN;
        } else if (bufferUtilization > 0.5) {
            return StatusState.MAINTAIN;
        } else {
            return StatusState.SPEED_UP;
        }
    }

    private StatusState transfer() {
        int totalTransfer = 0;
        for (Integer tS : transferSize.values()) {
            totalTransfer += tS;
        }
        double transferUtilization = (double)totalTransfer / ComponentLoadConfig.maxTransfer;
        if (transferUtilization >= 1) {
            return StatusState.SLOW_DOWN;
        } else if (transferUtilization > 0.5) {
            return StatusState.MAINTAIN;
        } else {
            return StatusState.SPEED_UP;
        }
    }

    public ComponentLoadReport report() {
        return new ComponentLoadReport(networkQueueLoad.queueDelay(), transferSize, bufferSize);
    }
}