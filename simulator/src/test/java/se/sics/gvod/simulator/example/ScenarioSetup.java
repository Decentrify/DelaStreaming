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
package se.sics.gvod.simulator.example;

import java.net.InetAddress;
import java.net.UnknownHostException;
import org.junit.Assert;
import se.sics.gvod.simulator.util.AsyncToSyncCallbackI;
import se.sics.kompics.KompicsEvent;
import se.sics.kompics.simulator.util.GlobalView;
import se.sics.ktoolbox.util.identifiable.basic.IntIdentifier;
import se.sics.ktoolbox.util.network.KAddress;
import se.sics.ktoolbox.util.network.basic.BasicAddress;
import se.sics.ktoolbox.util.network.nat.NatAwareAddressImpl;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class ScenarioSetup {

    public static final long scenarioSeed = 1234;
    public static final int appPort = 12345;
    public static AsyncToSyncCallbackI port1Callback = ScenarioSetup.port1Callback = new AsyncToSyncCallbackI() {
            public int counter = 1;

            @Override
            public void callback(KompicsEvent event, GlobalView gv) {
                switch (counter) {
                    case 1: 
                        System.out.println(counter);
                        counter++;
                        break;
                    case 2:
                        System.out.println(counter);
                        counter++;
                        gv.setValue("simulation.example.termination", true || gv.getValue("simulation.example.termination", Boolean.class));
                        gv.setValue("simulation.example.success", true && gv.getValue("simulation.example.termination", Boolean.class));
                        break;
                    default:
                        Assert.assertTrue("should not get here", false);
                }
            }
        };
    
    public static KAddress getNodeAdr(int nodeId) {
        try {
            return NatAwareAddressImpl.open(new BasicAddress(InetAddress.getByName("193.0.0." + nodeId), appPort, new IntIdentifier(nodeId)));
        } catch (UnknownHostException ex) {
            throw new RuntimeException(ex);
        }
    }
    public static long getNodeSeed(int nodeId) {
        return scenarioSeed + nodeId;
    }
}
