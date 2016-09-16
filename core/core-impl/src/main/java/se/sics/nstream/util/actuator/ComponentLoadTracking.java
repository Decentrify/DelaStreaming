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
import java.util.Iterator;
import java.util.Map;
import org.javatuples.Pair;
import se.sics.kompics.ComponentProxy;
import se.sics.ktoolbox.util.predict.ExpMovingAvg;
import se.sics.ktoolbox.util.predict.STGMAvg;
import se.sics.ktoolbox.util.predict.SimpleSmoothing;
import se.sics.nstream.torrent.util.BufferName;
import se.sics.nutil.tracking.load.QueueLoadConfig;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class ComponentLoadTracking {

    private final STGMAvg avgBufferLoad;
    private int instBufferLoad;

    private final Map<String, Integer> transferSize = new HashMap<>();
    private final Map<String, Pair<BufferName, Integer>> bufferSize = new HashMap<>();
    private final Map<String, Pair<Integer, Integer>> cacheSize = new HashMap<>();

    public ComponentLoadTracking(String componentName, ComponentProxy proxy, QueueLoadConfig queueLoadConfig) {
        this.avgBufferLoad = new STGMAvg(new ExpMovingAvg(), Pair.with(0.0, (double)ComponentLoadConfig.maxTransfer), new SimpleSmoothing());
        this.instBufferLoad = 0;
    }

    public void setBufferSize(BufferName bufferName, int size) {
        bufferSize.put(bufferName.fullName(), Pair.with(bufferName, size));
    }
    
    public int getMaxBufferSize(int fileId) {
        Iterator<Pair<BufferName, Integer>> it = bufferSize.values().iterator();
        int maxSize = -1;
        while(it.hasNext()) {
            Pair<BufferName, Integer> next = it.next();
            if(next.getValue0().fileId != fileId) {
                continue;
            }
            if(maxSize < next.getValue1()) {
                maxSize = next.getValue1();
            }
        }
        return maxSize;
    }
    
    public void setTransferSize(String fileName, int size) {
        transferSize.put(fileName, size);
    }

    public void setCacheSize(String fileName, int normalCacheSize, int extendedCacheSize) {
        cacheSize.put(fileName, Pair.with(normalCacheSize, extendedCacheSize));
    }

    public double adjustment() {
        return bufferAdjustment();
    }

    private double bufferAdjustment() {
        int totalTransfer = 0;
        for (Integer tS : transferSize.values()) {
            totalTransfer += tS;
        }
        instBufferLoad = totalTransfer;
        double adjustment = avgBufferLoad.update(instBufferLoad);
        return adjustment;
    }

    public ComponentLoadReport report() {
        return new ComponentLoadReport(null, Pair.with((int)avgBufferLoad.get(), instBufferLoad));
    }
}
