/*
 * 2016 Royal Institute of Technology (KTH)
 *
 * LSelector is free software; you can redistribute it and/or
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
package se.sics.gvod.simulator.torrent;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.gvod.common.util.VodDescriptor;
import se.sics.gvod.core.util.TorrentDetails;
import se.sics.gvod.simulator.TestDriver;
import se.sics.gvod.stream.connection.ConnectionPort;
import se.sics.gvod.stream.connection.StreamConnections;
import se.sics.kompics.ComponentProxy;
import se.sics.kompics.KompicsEvent;
import se.sics.kompics.Start;
import se.sics.kompics.simulator.util.GlobalView;
import se.sics.ktoolbox.util.identifiable.Identifier;
import se.sics.ktoolbox.util.identifiable.basic.IntIdentifier;
import se.sics.ktoolbox.util.identifiable.basic.OverlayIdFactory;
import se.sics.ktoolbox.util.managedStore.core.FileMngr;
import se.sics.ktoolbox.util.managedStore.core.HashMngr;
import se.sics.ktoolbox.util.managedStore.core.impl.TransferMngr;
import se.sics.ktoolbox.util.managedStore.core.util.Torrent;
import se.sics.ktoolbox.util.network.KAddress;
import se.sics.ktoolbox.util.network.basic.BasicAddress;
import se.sics.ktoolbox.util.network.nat.NatAwareAddressImpl;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class ScenarioSetup {

    public static final long scenarioSeed = 1234;
    private static final int appPort = 12345;

    private static final byte overlayOwner = 0x10;
    private static final Identifier overlayId;

    static {
        overlayId = OverlayIdFactory.getId(overlayOwner, OverlayIdFactory.Type.TGRADIENT, new byte[]{0, 0, 1});
    }

    private static final TorrentDetails tD1 = new TorrentDetails() {

        @Override
        public Identifier getOverlayId() {
            return overlayId;
        }

        @Override
        public boolean download() {
            return true;
        }

        @Override
        public boolean hasTorrent() {
            return false;
        }

        @Override
        public Torrent getTorrent() {
            throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        }

        @Override
        public FileMngr fileMngr(Torrent torrent) {
            throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        }

        @Override
        public HashMngr hashMngr(Torrent torrent) {
            throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        }

        @Override
        public TransferMngr transferMngr(Torrent torrent) {
            throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        }
    };

    public final static TorrentDriver torrentDriver1 = new TorrentDriver() {
        private final Logger LOG = LoggerFactory.getLogger(TestDriver.class);

        final KAddress selfAdr = getNodeAdr(1);
        final TorrentDetails torrentDetails = tD1;
        int state = 0;

        @Override
        public KAddress getSelfAdr() {
            return selfAdr;
        }

        @Override
        public TorrentDetails getTorrentDetails() {
            return torrentDetails;
        }

        @Override
        public void next(ComponentProxy proxy, GlobalView gv, KompicsEvent event) {
            if (state == 0 && event instanceof StreamConnections.Request) {
                LOG.info("state1 - publish connections");
                state1(proxy, gv, (StreamConnections.Request)event);
                state++;
                return;
            }
            if (state == 1) {
                LOG.info("state2 - limbo");
                return;
            }
            gv.terminate();
            throw new RuntimeException("state" + state + " illegal event:" + event.getClass().getCanonicalName());
        }

        private void state1(ComponentProxy proxy, GlobalView gv, StreamConnections.Request req) {
            Map<Identifier, KAddress> connections = new HashMap();
            Map<Identifier, VodDescriptor> descriptors = new HashMap();

            KAddress adr2 = getNodeAdr(2);
            connections.put(adr2.getId(), adr2);
            descriptors.put(adr2.getId(), new VodDescriptor(-1));

//            proxy.trigger(new StreamConnections(connections, descriptors), proxy.getPositive(ConnectionPort.class).getPair());
        }
    };

    private static KAddress getNodeAdr(int nodeId) {
        try {
            return NatAwareAddressImpl.open(new BasicAddress(InetAddress.getByName("193.0.0." + nodeId), appPort, new IntIdentifier(nodeId)));
        } catch (UnknownHostException ex) {
            throw new RuntimeException(ex);
        }
    }

    private static long getNodeSeed(int nodeId) {
        return scenarioSeed + nodeId;
    }
}
