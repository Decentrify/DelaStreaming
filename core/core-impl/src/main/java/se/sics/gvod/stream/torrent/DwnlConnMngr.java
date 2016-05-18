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
 * You should have received a reportStatus of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
 */
package se.sics.gvod.stream.torrent;

import com.google.common.base.Optional;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import org.javatuples.Pair;
import se.sics.gvod.common.util.VodDescriptor;
import se.sics.gvod.stream.congestion.PLedbatState;
import se.sics.gvod.stream.util.ConnectionStatus;
import se.sics.ktoolbox.util.identifiable.Identifier;
import se.sics.ktoolbox.util.network.KAddress;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class DwnlConnMngr {

    static final int CONNECTION_STARTING_LOAD = 100;

    private final double timeoutSlowDownRate;
    private final double normalSlowDownRate;
    private final double speedUpRate;

    private int usedDownloadSlots;
    private int maxDownloadSlots;

    Map<Identifier, KAddress> partners = new HashMap<>();
    Map<Identifier, ConnectionState> connectionLoad = new HashMap<>();
    Map<Identifier, VodDescriptor> partnerStatus = new HashMap<>();

    public DwnlConnMngr(double timeoutSlowDownRate, double normalSlowDownRate, double speedUpRate) {
        this.timeoutSlowDownRate = timeoutSlowDownRate;
        this.normalSlowDownRate = normalSlowDownRate;
        this.speedUpRate = speedUpRate;
    }

    public void addConnection(KAddress partner, VodDescriptor descriptor) {
        partners.put(partner.getId(), partner);
        connectionLoad.put(partner.getId(), new ConnectionState(timeoutSlowDownRate, normalSlowDownRate, speedUpRate, CONNECTION_STARTING_LOAD));
        partnerStatus.put(partner.getId(), descriptor);
        maxDownloadSlots = CONNECTION_STARTING_LOAD;
        usedDownloadSlots = 0;
    }

    private void removePartner(KAddress partner) {
        partners.remove(partner.getId());
        connectionLoad.remove(partner.getId());
        partnerStatus.remove(partner.getId());
    }

    public KAddress getRandomConnection() {
        Iterator<KAddress> connIt = partners.values().iterator();
        if (!connIt.hasNext()) {
            throw new RuntimeException();
        }
        return connIt.next();
    }

    public int localOverloaded() {
        int aux = 0;
        if (maxDownloadSlots > CONNECTION_STARTING_LOAD) {
            aux = (int) (maxDownloadSlots * normalSlowDownRate);
            maxDownloadSlots -= aux;
        }
        return usedDownloadSlots + maxDownloadSlots;
    }

    public int localUnderloaded() {
        int aux = 0;
//        if (usedDownloadSlots > maxDownloadSlots * 0.9) {
        aux = (int)(maxDownloadSlots * speedUpRate);
        maxDownloadSlots += aux;
//        }
        return usedDownloadSlots + maxDownloadSlots;
    }

    public void timedOut(KAddress partner) {
        ConnectionState conn = connectionLoad.get(partner.getId());
        usedDownloadSlots--;
        conn.releaseSlot(false);
        conn.halveSlots();
    }

    public void completed(KAddress partner, PLedbatState.Status status) {
        ConnectionState conn = connectionLoad.get(partner.getId());
        usedDownloadSlots--;
        conn.releaseSlot(true);

        switch (status) {
            case SPEED_UP:
                conn.increaseSlots();
                break;
            case SLOW_DOWN:

                conn.decreaseSlots();
                break;
            case MAINTAIN:
                break;
            default:
                throw new RuntimeException("missing logic - fix me");
        }
    }

    public Optional<KAddress> download(int downloadPos) {
        for (Map.Entry<Identifier, ConnectionState> conn : connectionLoad.entrySet()) {
            if (conn.getValue().available() && usedDownloadSlots < maxDownloadSlots) {
                VodDescriptor descriptor = partnerStatus.get(conn.getKey());
                if (descriptor.downloadPos == -1 || descriptor.downloadPos >= downloadPos) {
                    conn.getValue().useSlot();
                    usedDownloadSlots++;
                    return Optional.of(partners.get(conn.getKey()));
                }
            }
        }
        return Optional.absent();
    }

    public Pair<Integer, Integer> getLoad() {
        return Pair.with(usedDownloadSlots, maxDownloadSlots);
    }

    public Map<Identifier, ConnectionStatus> reportNReset() {
        Map<Identifier, ConnectionStatus> report = new HashMap<>();
        for (Map.Entry<Identifier, ConnectionState> cs : connectionLoad.entrySet()) {
            report.put(cs.getKey(), cs.getValue().reportStatus());
            cs.getValue().reset();
        }
        return report;
    }
}
