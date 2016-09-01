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
package se.sics.nstream.torrent;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import org.javatuples.Pair;
import se.sics.ktoolbox.util.identifiable.Identifier;
import se.sics.ktoolbox.util.network.KAddress;
import se.sics.ledbat.core.LedbatConfig;
import se.sics.ledbat.ncore.PullConnection;
import se.sics.ledbat.ncore.msg.LedbatMsg;

/**
 *
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class RoundRobinConnMngr implements Router {

    private final LedbatConfig ledbatConfig;
    private final Map<Identifier, PullConnection> loadTracker = new HashMap<>();
    private final LinkedList<KAddress> partners = new LinkedList<>();

    public RoundRobinConnMngr(LedbatConfig ledbatConfig, List<KAddress> partners) {
        this.ledbatConfig = ledbatConfig;
        this.partners.addAll(partners);
        for (KAddress partner : partners) {
            loadTracker.put(partner.getId(), new PullConnection(ledbatConfig, partner.getId()));
        }
    }

    @Override
    public KAddress randomPartner() {
        KAddress first = partners.removeFirst();
        partners.add(first);
        return first;
    }

//    @Override
//    public int totalSlots() {
//        return loadTracker.windowSize();
//    }
//
    @Override
    public Pair<KAddress, Long> availableSlot() {
        int n = partners.size();
        while(n > 0) {
            n--;
            KAddress next = partners.removeFirst();
            partners.add(next);
            PullConnection pc = loadTracker.get(next.getId());
            if(pc.canSend() > 0) {
                return Pair.with(next, pc.getRTO());
            }
        }
        return null;
    }

    @Override
    public void useSlot(KAddress target) {
        PullConnection pc = loadTracker.get(target.getId());
        if (pc == null) {
            return;
        }
        pc.request(ledbatConfig.mss);
    }

    @Override
    public void success(KAddress target, LedbatMsg.Response resp) {
        PullConnection pc = loadTracker.get(target.getId());
        if (pc == null) {
            return;
        }
        pc.success(ledbatConfig.mss, resp);
    }

    @Override
    public void timeout(KAddress target) {
        PullConnection pc = loadTracker.get(target.getId());
        if (pc == null) {
            return;
        }
        pc.timeout(ledbatConfig.mss);
    }
//
//    public boolean changed() {
//        return loadTracker.changed();
//    }

    public String report() {
        return "report-dissabled";
//        return loadTracker.report();
    }
}