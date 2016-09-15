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
package se.sics.nstream.torrent.old;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import org.javatuples.Pair;
import se.sics.ktoolbox.util.identifiable.Identifier;
import se.sics.ktoolbox.util.network.KAddress;
import se.sics.ledbat.core.AppCongestionWindow;
import se.sics.ledbat.core.LedbatConfig;
import se.sics.ledbat.core.util.ThroughputHandler;
import se.sics.ledbat.ncore.msg.LedbatMsg;
import se.sics.nstream.torrent.ConnMngr;
import se.sics.nstream.torrent.ConnMngrConfig;
import se.sics.nstream.torrent.ConnMngrReport;

/**
 *
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class SimpleConnMngr implements ConnMngr {

    private final LedbatConfig ledbatConfig;
    private final ConnMngrConfig connConfig;
    private final Map<Identifier, AppCongestionWindow> seederTracker = new HashMap<>();
    private final Map<Identifier, ThroughputHandler> leecherTracker = new HashMap<>();
    private final LinkedList<KAddress> partners = new LinkedList<>();

    public SimpleConnMngr(ConnMngrConfig connConfig, LedbatConfig ledbatConfig, List<KAddress> partners) {
        this.connConfig = connConfig;
        this.ledbatConfig = ledbatConfig;
        this.partners.addAll(partners);
        for (KAddress partner : partners) {
            seederTracker.put(partner.getId(), new AppCongestionWindow(ledbatConfig, partner.getId()));
        }
    }

    @Override
    public void appState(double adjustment) {
        long now = System.currentTimeMillis();
        for(AppCongestionWindow acw : seederTracker.values()) {
            acw.adjustState(adjustment);
        }
    }
    @Override
    public KAddress randomPartner() {
        KAddress first = partners.removeFirst();
        partners.add(first);
        return first;
    }
    
    @Override
    public Pair<KAddress, Long> availableDownloadSlot() {
        int n = partners.size();
        while (n > 0) {
            n--;
            KAddress next = partners.removeFirst();
            partners.add(next);
            AppCongestionWindow pc = seederTracker.get(next.getId());
            if (pc.canSend()) {
                return Pair.with(next, pc.getRTO());
            }
        }
        return null;
    }

    @Override
    public void useDownloadSlot(KAddress target) {
        AppCongestionWindow pc = seederTracker.get(target.getId());
        if (pc == null) {
            return;
        }
        pc.request(System.currentTimeMillis(), ledbatConfig.mss);
    }

    @Override
    public void successDownloadSlot(KAddress target, LedbatMsg.Response resp) {
        AppCongestionWindow pc = seederTracker.get(target.getId());
        if (pc == null) {
            return;
        }
        pc.success(System.currentTimeMillis(), ledbatConfig.mss, resp);
    }

    @Override
    public void timeoutDownloadSlot(KAddress target) {
        AppCongestionWindow pc = seederTracker.get(target.getId());
        if (pc == null) {
            return;
        }
        pc.timeout(System.currentTimeMillis(), ledbatConfig.mss);
    }

    //******************************UPLOAD**************************************
    @Override
    public boolean availableUploadSlot(KAddress target) {
        ThroughputHandler leecherThroughput = leecherTracker.get(target.getId());
        if(leecherThroughput == null) {
            leecherTracker.put(target.getId(), new ThroughputHandler(target.getId().toString()));
            return true;
        }
        if(connConfig.maxConnUploadSpeed == -1) {
            return true;
        }
        if(leecherThroughput.currentSpeed(System.currentTimeMillis()) < connConfig.maxConnUploadSpeed) {
            return true;
        } else {
            return false;
        }
    }

    @Override
    public void useUploadSlot(KAddress target) {
        ThroughputHandler leecherThroughput = leecherTracker.get(target.getId());
        if(leecherThroughput == null) {
            return;
        }
        leecherThroughput.packetReceived(System.currentTimeMillis(), ledbatConfig.mss);
    }
    //*****************************REPORTING************************************

    @Override
    public ConnMngrReport speed() {
        return ConnMngrReport.transferReport(seederTracker, leecherTracker);
    }
    
    @Override 
    public double totalCwnd() {
        double totalCwnd = 0;
        for(AppCongestionWindow acw : seederTracker.values()) {
            totalCwnd += acw.cwnd();
        }
        return totalCwnd;
    }
}
