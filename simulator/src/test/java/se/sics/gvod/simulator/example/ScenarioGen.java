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

import se.sics.gvod.simulator.example.TestExampleHostComp;
import se.sics.kompics.network.Address;
import se.sics.kompics.simulator.SimulationScenario;
import se.sics.kompics.simulator.adaptor.Operation1;
import se.sics.kompics.simulator.adaptor.distributions.extra.BasicIntSequentialDistribution;
import se.sics.kompics.simulator.events.system.StartNodeEvent;
import se.sics.ktoolbox.util.network.KAddress;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class ScenarioGen {

    static Operation1<StartNodeEvent, Integer> startTestOp = new Operation1<StartNodeEvent, Integer>() {

        @Override
        public StartNodeEvent generate(final Integer nodeId) {
            return new StartNodeEvent() {
                KAddress selfAdr;
                {
                    selfAdr = ScenarioSetup.getNodeAdr(nodeId);
                }

                @Override
                public Address getNodeAddress() {
                    return selfAdr;
                }

                @Override
                public Class getComponentDefinition() {
                    return TestExampleHostComp.class;
                }

                @Override
                public TestExampleHostComp.Init getComponentInit() {
                    return new TestExampleHostComp.Init(ScenarioSetup.port1Callback, 1000, 10);
                }
            };
        }
    };

    public static SimulationScenario simpleBoot() {
        SimulationScenario scen = new SimulationScenario() {
            {
                StochasticProcess startTest = new StochasticProcess() {
                    {
                        raise(1, startTestOp, new BasicIntSequentialDistribution(1));
                    }
                };

                startTest.start();
                terminateAfterTerminationOf(1000*1000, startTest);
            }
        };

        return scen;
    }
}
