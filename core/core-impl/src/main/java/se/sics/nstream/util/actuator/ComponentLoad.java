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
    
    public ComponentLoad(Random rand, long targetQueueDelay, long maxQueueDelay, long maxCheckPeriod) {
        this.maxQueueDelay = maxQueueDelay;
        this.maxCheckPeriod = maxCheckPeriod;
        this.fuzzyState = new FuzzyState((double)targetQueueDelay/maxQueueDelay, rand);
        this.checkPeriod = maxQueueDelay;
        this.lastQueueDelay = 0;
        this.state = DownloadStates.MAINTAIN;
    }
    
    public void adjustState(long current) {
        lastQueueDelay = current;
        
        DownloadStates old = state;
        if(current > maxQueueDelay) {
            state =  DownloadStates.SLOW_DOWN;
        }
        state = fuzzyState.state((double)current/maxQueueDelay);
        
        if(!old.equals(state)) {
            checkPeriod = maxQueueDelay;
        } else {
            checkPeriod = maxCheckPeriod;
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
    
    public boolean canDownload() {
        return !state.equals(DownloadStates.SLOW_DOWN);
    }
    
    public String report() {
        return "state:" + state + " qD:" + lastQueueDelay;
    }
}
