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
package se.sics.gvod.stream.torrent.util;

import java.util.LinkedList;
import org.javatuples.Triplet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.silk.supervisor.TorrentSupervisorComp;
import se.sics.gvod.stream.util.ConnectionStatus;

/**
 *
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class ConnectionStateV2 {
    private static final Logger LOG = LoggerFactory.getLogger(TorrentSupervisorComp.class);
    private String logPrefix;

    public static final int HISTORY_SIZE = 100;
    public static final int HISTORY_WINDOW_SIZE = 1000;

    public final ConnectionParam connParam;
    private int maxSlots;
    private int usedSlots = 0;

    private LinkedList<Triplet<Integer, Integer, Integer>> history; //<slow, fast, lost>
    private boolean speedUp = true;
    private double rateChange;
    private int historyWindowSize;

    private int slowPackets = 0;
    private int fastPackets = 0;
    private int lostPackets = 0;

    private ConnectionStatus report = new ConnectionStatus(0, 0, 0, 0);

    public ConnectionStateV2(ConnectionParam connParam) {
        this.connParam = connParam;
        history = new LinkedList<>();
        maxSlots = connParam.connStartingLoad;
        rateChange = connParam.rateChange.getValue0();
        historyWindowSize = HISTORY_WINDOW_SIZE;
    }

    public boolean available() {
        return usedSlots < maxSlots;
    }

    public void useSlot() {
        usedSlots++;
    }

    public void slow() {
        usedSlots--;
        slowPackets++;
        checkHistory();
    }

    public void fast() {
        usedSlots--;
        fastPackets++;
        checkHistory();
    }

    public void timeout() {
        usedSlots--;
        lostPackets++;
        checkHistory();
    }

    private void checkHistory() {
        if (slowPackets + fastPackets + lostPackets == historyWindowSize) {
            adjustSpeed();
            history.addLast(Triplet.with(slowPackets, fastPackets, lostPackets));
            if (history.size() > HISTORY_SIZE) {
                history.removeFirst();
            }
            report = new ConnectionStatus(maxSlots, usedSlots, report.successSlots + slowPackets + fastPackets, report.failedSlots + lostPackets);
            slowPackets = 0;
            fastPackets = 0;
            lostPackets = 0;
        }
    }

    private void adjustSpeed() {
        int totalPackets = slowPackets + fastPackets + lostPackets;
        double lossRate = lossRate();
        if (lossRate > connParam.acceptableLoss && usedSlots < maxSlots) {
            maxSlots -= (1 + maxSlots * connParam.rateChange.getValue1());
            speedUp = false;
        } else if (slowPackets + lostPackets> fastPackets &&  usedSlots < maxSlots) {
            if (speedUp) {
                rateChange = rateChange / 2 < connParam.rateChange.getValue0() ? connParam.rateChange.getValue0() : rateChange / 2;
            } else {
                rateChange = rateChange * 2 > connParam.rateChange.getValue1() ? connParam.rateChange.getValue1() : rateChange * 2;
            }
            speedUp = false;
            maxSlots -= (int) (1 + maxSlots * rateChange);
        } else if (maxSlots < usedSlots + 10) {
            
            if (!speedUp) {
                rateChange = rateChange / 2 < connParam.rateChange.getValue0() ? connParam.rateChange.getValue0() : rateChange / 2;
            } else {
                rateChange = rateChange * 2 > connParam.rateChange.getValue1() ? connParam.rateChange.getValue1() : rateChange * 2;
            }
            speedUp = true;
            maxSlots += (int) (1 + maxSlots * rateChange);
        }
        maxSlots = maxSlots < connParam.connStartingLoad ? connParam.connStartingLoad : maxSlots;
        historyWindowSize = maxSlots + usedSlots;
//        LOG.info("change rate:{} maxSlots:{}", rateChange, maxSlots);
    }

    public ConnectionStatus reportStatus() {
        ConnectionStatus result = report;
        report = new ConnectionStatus(maxSlots, usedSlots, 0, 0);
        return result;
    }
    
    private Double lossRate() {
        long total = 0;
        long lost = 0;
        for (Triplet<Integer, Integer, Integer> window : history) {
            total = total + window.getValue0() + window.getValue1();
            lost = lost + window.getValue2();
        }
        return (double)lost/(total + lost);
    }
}
