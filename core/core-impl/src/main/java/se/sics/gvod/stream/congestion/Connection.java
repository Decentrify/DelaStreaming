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
package se.sics.gvod.stream.congestion;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import se.sics.gvod.stream.congestion.event.external.PLedbatConnection;
import se.sics.kompics.ComponentProxy;

/**
 * According to LEDBAT RFC 6817 - December 2012
 *
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class Connection {

    public final int HISTORY_SIZE = 20;
    public final long BASE_HISTORY_ROUND_TIME = 60 * 1000 * 1000; //1 minute
    public final long TARGET = 100; //100ms 
    public final double GAIN = 1/100; //set to 1/TARGET

    public final PLedbatConnection.TrackRequest req;
    private final LinkedList<Long> baseHistory = new LinkedList<>();
    private final LinkedList<Long> currentHistory = new LinkedList<>();
    
    public Connection(PLedbatConnection.TrackRequest req) {
        this.req = req;
    }

    public void incoming(long sendingTime, ComponentProxy proxy) {
        long now = System.currentTimeMillis();
        long delay = sendingTime - now;
        updateBaseDelay(delay);
        updateCurrentDelay(delay);
        
        long queuingDelay = latestQueuingDelay() - Collections.min(baseHistory);
        double offTarget = ((double)(TARGET - queuingDelay)) / TARGET;
        if(offTarget >= 0) {
             proxy.answer(req, req.answer(PLedbatConnection.TrackResponse.Status.SPEED_UP));
        } else {
             proxy.answer(req, req.answer(PLedbatConnection.TrackResponse.Status.SLOW_DOWN));
        }
    }

    public void timeout(ComponentProxy proxy) {
        proxy.answer(req, req.answer(PLedbatConnection.TrackResponse.Status.TIMEOUT));
    }

    //Maintain BASE_HISTORY delay-minima
    //Each minimum is measured over a period of BASE_HISTORY_ROUND_TIME
    private void updateBaseDelay(long delay) {
        baseHistory.set(0, Math.min(baseHistory.get(0), delay));
    }

    public void updateBaseDelayRound() {
        baseHistory.addFirst(Long.MAX_VALUE);
        if (baseHistory.size() > HISTORY_SIZE) {
            baseHistory.removeLast();
        }
    }

    private void updateCurrentDelay(long delay) {
        currentHistory.addFirst(delay);
        if (currentHistory.size() > HISTORY_SIZE) {
            currentHistory.removeLast();
        }
    }

    //RFC - filter method
    private long latestQueuingDelay() {
        return (long)Math.floor(mean(currentHistory));
    }

    private double mean(List<Long> values) {
        double avg = 0;
        int t = 1;
        for (double x : values) {
            avg += (x - avg) / t;
            ++t;
        }
        return avg;
    }
}
