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

import com.google.common.base.Optional;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.gvod.common.util.VodDescriptor;
import se.sics.gvod.stream.congestion.PLedbatState;
import se.sics.nstream.torrent.tracking.TorrentTrackingComp;
import se.sics.gvod.stream.util.ConnectionStatus;
import se.sics.kompics.id.Identifier;
import se.sics.ktoolbox.util.network.KAddress;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class DwnlConnMngrV2 {

    private static final Logger LOG = LoggerFactory.getLogger(TorrentTrackingComp.class);

    private final HostParam hostParam;
    private final ConnectionParam connParam;
    private final Map<Identifier, KAddress> partners = new HashMap<>();
    private final Map<Identifier, ConnectionStateV2> connectionLoad = new HashMap<>();
    private final Map<Identifier, VodDescriptor> partnerStatus = new HashMap<>();

    private ConnectionStateV2 hostState;

    public DwnlConnMngrV2(HostParam hostParam, ConnectionParam connParam) {
        this.hostParam = hostParam;
        this.connParam = connParam;
        hostState = new ConnectionStateV2(connParam);
    }

    public void addConnection(KAddress partner, VodDescriptor descriptor) {
        partners.put(partner.getId(), partner);
        connectionLoad.put(partner.getId(), new ConnectionStateV2(connParam));
        partnerStatus.put(partner.getId(), descriptor);
    }

    private void removePartner(KAddress partner) {
        partners.remove(partner.getId());
        connectionLoad.remove(partner.getId());
        partnerStatus.remove(partner.getId());
    }

    public KAddress getRandomConnection() {
        //TODO - atm get first connection
        Iterator<KAddress> connIt = partners.values().iterator();
        if (!connIt.hasNext()) {
            throw new RuntimeException();
        }
        return connIt.next();
    }

    public void timedOut(KAddress partner) {
        hostState.slow();
        ConnectionStateV2 conn = connectionLoad.get(partner.getId());
        conn.timeout();
    }

    public void completed(KAddress partner, PLedbatState state) {
        long handlingTime = System.currentTimeMillis();
        long receivedTime = state.getReceivedTime();
        long kQueueDelay = handlingTime - receivedTime;

        LOG.debug("torrent comp queue delay:{}", kQueueDelay);
        if (kQueueDelay < hostParam.minQueueDelay) {
            hostState.fast();
        } else if (hostParam.maxQueueDelay < kQueueDelay) {
            hostState.timeout();
        } else {
            hostState.slow();
        }

        ConnectionStateV2 conn = connectionLoad.get(partner.getId());
        switch (state.getStatus()) {
            case SPEED_UP:
                conn.fast();
                break;
            case SLOW_DOWN:
                LOG.info("status{}", state.getStatus());
                conn.slow();
                break;
            case MAINTAIN:
                conn.fast();
                break;
            default:
                throw new RuntimeException("missing logic - fix me");
        }
    }

    public Optional<KAddress> download(int downloadPos) {
        for (Map.Entry<Identifier, ConnectionStateV2> conn : connectionLoad.entrySet()) {
            if (conn.getValue().available() && hostState.available()) {
                VodDescriptor descriptor = partnerStatus.get(conn.getKey());
                if (descriptor.downloadPos == -1 || descriptor.downloadPos >= downloadPos) {
                    hostState.useSlot();
                    conn.getValue().useSlot();
                    return Optional.of(partners.get(conn.getKey()));
                }
            }
        }
        return Optional.absent();
    }

    public Map<Identifier, ConnectionStatus> reportNReset() {
        Map<Identifier, ConnectionStatus> report = new HashMap<>();
        for (Map.Entry<Identifier, ConnectionStateV2> cs : connectionLoad.entrySet()) {
            report.put(cs.getKey(), cs.getValue().reportStatus());
        }
//        report.put(new IntId(0), hostState.reportStatus());
        return report;
    }
}
