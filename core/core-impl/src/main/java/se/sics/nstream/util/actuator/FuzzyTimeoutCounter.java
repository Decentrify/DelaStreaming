/*
 * Copyright (C) 2009 Swedish Institute of Computer Science (SICS) Copyright (C)
 * 2009 Royal Institute of Technology (KTH)
 *
 * KompicsToolbox is free software; you can redistribute it and/or
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
public class FuzzyTimeoutCounter {
    private final FuzzyState ta;
    private int success = 0;
    private int timeout = 0;
    
    private FuzzyTimeoutCounter(FuzzyState ta) {
        this.ta = ta;
    }
    
    public void success() {
        success++;
    }
    
    public void timeout() {
        timeout++;
    }
    
    public DownloadStates state() {
        double timeoutPercentage = (double)timeout / (timeout+success);
        success = 0;
        timeout = 0;
        return ta.state(timeoutPercentage);
    }
    
    public static FuzzyTimeoutCounter getInstance(double acceptableTimeouts, Random rand) {
        return new FuzzyTimeoutCounter(new FuzzyState(acceptableTimeouts, rand));
    }
}
