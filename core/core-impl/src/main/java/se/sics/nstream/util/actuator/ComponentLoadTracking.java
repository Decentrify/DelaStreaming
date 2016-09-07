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
import org.javatuples.Pair;
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
    private final Map<String, Pair<Integer, Integer>> cacheSize = new HashMap<>();
    private long previousTransferSize = 0;
    
    public ComponentLoadTracking(String componentName, ComponentProxy proxy, QueueLoadConfig queueLoadConfig) {
        this.networkQueueLoad = new NetworkQueueLoadProxy(componentName + "network", proxy, queueLoadConfig);
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

    public void setCacheSize(String fileName, int normalCacheSize, int extendedCacheSize) {
        cacheSize.put(fileName, Pair.with(normalCacheSize, extendedCacheSize));
    }

    public StatusState state() {
        StatusState queue = networkQueueLoad.state();
        StatusState buffer = buffer();
        if (queue.equals(StatusState.SLOW_DOWN) || buffer.equals(StatusState.SLOW_DOWN)) {
            state = StatusState.SLOW_DOWN;
        } else if (queue.equals(StatusState.MAINTAIN) || buffer.equals(StatusState.MAINTAIN)) {
            state = StatusState.MAINTAIN;
        } else {
            state = StatusState.SPEED_UP;
        }
        return state;
    }

    private StatusState buffer() {
        int totalTransfer = 0;
        for (Integer tS : transferSize.values()) {
            totalTransfer += tS;
        }
        double transferUtilization = (double) totalTransfer / ComponentLoadConfig.maxTransfer;
        
        StatusState next;
        if (transferUtilization < 0.4) {
            next = StatusState.SPEED_UP;
        } else if (transferUtilization < 0.8) {
            if(transferUtilization < 0.8 * previousTransferSize) {
                next = StatusState.SPEED_UP;
            } else if(transferUtilization > 1.2 * previousTransferSize) {
                next = StatusState.SLOW_DOWN;
            } else {
                next = StatusState.MAINTAIN;
            }
        } else {
             next = StatusState.SLOW_DOWN;
        }
        previousTransferSize = totalTransfer;
        return next;
    }

    public ComponentLoadReport report() {
        return new ComponentLoadReport(networkQueueLoad.queueDelay(), transferSize, bufferSize, cacheSize);
    }
}
