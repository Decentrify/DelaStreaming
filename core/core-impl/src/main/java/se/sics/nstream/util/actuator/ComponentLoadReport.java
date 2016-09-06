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

import java.util.Map;
import org.javatuples.Pair;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class ComponentLoadReport {

    public final long queueDelay;
    public final int totalTransferSize;
    public final int totalBufferSize;
    public final int totalCacheSize;
    public final int totalExtendedCacheSize;

    public ComponentLoadReport(long lastQueueDelay, Map<String, Integer> transferSize, Map<String, Integer> bufferSize, Map<String, Pair<Integer, Integer>> cacheSize) {
        this.queueDelay = lastQueueDelay;

        int tbs = 0;
        for (Integer bs : bufferSize.values()) {
            tbs += bs;
        }
        this.totalBufferSize = tbs;

        int tts = 0;
        for (Integer ts : transferSize.values()) {
            tts += ts;
        }
        this.totalTransferSize = tts;
        
        int tcs = 0;
        int tecs = 0;
        for (Pair<Integer, Integer> cs : cacheSize.values()) {
            tcs += cs.getValue0();
            tecs += cs.getValue1();
        }
        this.totalCacheSize = tcs;
        this.totalExtendedCacheSize = tecs;
    }

}
