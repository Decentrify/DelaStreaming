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
package se.sics.gvod.simulator.torrent.sim1;

import se.sics.gvod.simulator.torrent.sim1.TorrentDriver;
import se.sics.gvod.simulator.torrent.sim1.TorrentDriverComp;
import se.sics.gvod.simulator.torrent.sim1.TorrentTestHostComp;
import se.sics.gvod.stream.torrent.TorrentComp;
import se.sics.kompics.network.Address;
import se.sics.kompics.simulator.SimulationScenario;
import se.sics.kompics.simulator.adaptor.Operation1;
import se.sics.kompics.simulator.adaptor.distributions.extra.BasicIntSequentialDistribution;
import se.sics.kompics.simulator.events.system.StartNodeEvent;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class ScenarioGen {

    static Operation1<StartNodeEvent, Integer> startDwnlOp = new Operation1<StartNodeEvent, Integer>() {

        @Override
        public StartNodeEvent generate(final Integer nodeId) {
            return new StartNodeEvent() {
                TorrentDriver dwnlTorrentDriver = ScenarioSetup.dwnlTorrentDriver;

                @Override
                public Address getNodeAddress() {
                    return dwnlTorrentDriver.getSelfAdr();
                }

                @Override
                public Class getComponentDefinition() {
                    return TorrentTestHostComp.class;
                }

                @Override
                public TorrentTestHostComp.Init getComponentInit() {
                    TorrentDriverComp.Init dwnlDriverInit = new TorrentDriverComp.Init(dwnlTorrentDriver);
                    TorrentComp.Init dwnlTorrentInit = new TorrentComp.Init(dwnlTorrentDriver.getSelfAdr(), dwnlTorrentDriver.getTorrentDetails());
                    return new TorrentTestHostComp.Init(dwnlTorrentInit, dwnlDriverInit);
                }
            };
        }
    };
    
    static Operation1<StartNodeEvent, Integer> startUpldOp = new Operation1<StartNodeEvent, Integer>() {

        @Override
        public StartNodeEvent generate(final Integer nodeId) {
            return new StartNodeEvent() {
                TorrentDriver upldTorrentDriver = ScenarioSetup.upldTorrentDriver;

                @Override
                public Address getNodeAddress() {
                    return upldTorrentDriver.getSelfAdr();
                }

                @Override
                public Class getComponentDefinition() {
                    return TorrentTestHostComp.class;
                }

                @Override
                public TorrentTestHostComp.Init getComponentInit() {
                    TorrentDriverComp.Init upldDriverInit = new TorrentDriverComp.Init(upldTorrentDriver);
                    TorrentComp.Init upldTorrentInit = new TorrentComp.Init(upldTorrentDriver.getSelfAdr(), upldTorrentDriver.getTorrentDetails());
                    return new TorrentTestHostComp.Init(upldTorrentInit, upldDriverInit);
                }
            };
        }
    };

    public static SimulationScenario simpleBoot() {
        SimulationScenario scen = new SimulationScenario() {
            {
                StochasticProcess startUpld = new StochasticProcess() {
                    {
                        raise(1, startUpldOp, new BasicIntSequentialDistribution(1));
                    }
                };
                
                StochasticProcess startDwnl = new StochasticProcess() {
                    {
                        raise(1, startDwnlOp, new BasicIntSequentialDistribution(1));
                    }
                };

                startUpld.start();
                startDwnl.startAfterTerminationOf(1000, startUpld);
                terminateAfterTerminationOf(1000 * 1000, startDwnl);
            }
        };

        return scen;
    }
}
