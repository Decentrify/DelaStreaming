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
package se.sics.gvod.stream.torrent;

import com.google.common.base.Optional;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import se.sics.gvod.common.util.VodDescriptor;
import se.sics.gvod.stream.congestion.event.external.PLedbatConnection;
import se.sics.gvod.stream.util.ConnectionStatus;
import se.sics.ktoolbox.util.identifiable.Identifier;
import se.sics.ktoolbox.util.network.KAddress;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class DwnlConnMngr {

    static final int CONNECTION_STARTING_LOAD = 100;
    Map<Identifier, KAddress> partners = new HashMap<>();
    Map<Identifier, ConnectionStatus> connectionLoad = new HashMap<>();
    Map<Identifier, VodDescriptor> partnerStatus = new HashMap<>();

    public void addConnection(KAddress partner, VodDescriptor descriptor) {
        partners.put(partner.getId(), partner);
        connectionLoad.put(partner.getId(), new ConnectionStatus(CONNECTION_STARTING_LOAD));
        partnerStatus.put(partner.getId(), descriptor);
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

    public void timedOut(KAddress partner) {
        //TODO Alex
        ConnectionStatus conn = connectionLoad.get(partner.getId());
        conn.releaseSlot(false);
    }

    public void completed(KAddress partner) {
        ConnectionStatus conn = connectionLoad.get(partner.getId());
        conn.releaseSlot(true);
    }

    public Optional<KAddress> download(int downloadPos) {
        for (Map.Entry<Identifier, ConnectionStatus> conn : connectionLoad.entrySet()) {
            if (conn.getValue().available()) {
                VodDescriptor descriptor = partnerStatus.get(conn.getKey());
                if (descriptor.downloadPos == -1 || descriptor.downloadPos >= downloadPos) {
                    conn.getValue().useSlot();
                    return Optional.of(partners.get(conn.getKey()));
                }
            }
        }
        return Optional.absent();
    }

    public Map<Identifier, ConnectionStatus> reportNReset() {
        Map<Identifier, ConnectionStatus> report = new HashMap<>();
        for (Map.Entry<Identifier, ConnectionStatus> cs : connectionLoad.entrySet()) {
            report.put(cs.getKey(), cs.getValue().copy());
            cs.getValue().reset();
        }
        return report;
    }

    public Integer usedSlots() {
        int usedSlots = 0;

        for (ConnectionStatus conn : connectionLoad.values()) {
            usedSlots += conn.usedSlots();
        }

        return usedSlots;
    }

    public void updateSlots(KAddress partner, PLedbatConnection.TrackResponse.Status status) {
        ConnectionStatus cs = connectionLoad.get(partner.getId());
        if (cs == null) {
            throw new RuntimeException("missing logic - fix me");
        }
        switch (status) {
            case SPEED_UP:
                cs.increaseSlots();
                break;
            case SLOW_DOWN:
                cs.decreaseSlots();
                break;
            case TIMEOUT:
                cs.halveSlots();
                break;
            default:
                throw new RuntimeException("missing logic - fix me");
        }
    }
}
