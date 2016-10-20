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
package se.sics.gvod.simulator.example;

import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.gvod.simulator.util.AsyncToSyncCallbackI;
import se.sics.gvod.simulator.util.TestSyncIComp;
import se.sics.gvod.simulator.util.TestTerminationComp;
import se.sics.kompics.Channel;
import se.sics.kompics.Component;
import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.Handler;
import se.sics.kompics.KompicsEvent;
import se.sics.kompics.Positive;
import se.sics.kompics.Start;
import se.sics.kompics.network.Network;
import se.sics.kompics.timer.Timer;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class TestExampleHostComp extends ComponentDefinition {
    private static final Logger LOG = LoggerFactory.getLogger(TestExampleHostComp.class);
    private String logPrefix = "";
    
    Positive<Timer> requiredTimerPort = requires(Timer.class);
    Positive<Network> requiredNetworkPort = requires(Network.class);
    
    private Component testExampleComp;
    private Component interceptor1Comp;
    private Component testTerminationComp;
    
    private final Init init;
    
    public TestExampleHostComp(Init init) {
        LOG.info("{}initiating...", logPrefix);
        
        this.init = init;
        
        subscribe(handleStart, control);
    }
    
    Handler handleStart = new Handler<Start>() {
        @Override
        public void handle(Start event) {
            LOG.info("{}starting...", logPrefix);
            
            testTerminationComp = create(TestTerminationComp.class, new TestTerminationComp.Init(init.testRounds, init.roundLength));
            connect(testTerminationComp.getNegative(Timer.class), requiredTimerPort, Channel.TWO_WAY);
            
            testExampleComp = create(TestExampleComp.class, new TestExampleComp.Init());
            List<Class<? extends KompicsEvent>> responseTypes = new ArrayList<>();
            responseTypes.add(TestExampleEvent.Response.class);
            responseTypes.add(TestExampleEvent.Indication.class);
            interceptor1Comp = create(TestSyncIComp.class, 
                    new TestSyncIComp.Init(TestExamplePort.class, responseTypes, init.port1Callback));
            connect(testExampleComp.getPositive(TestExamplePort.class), 
                    interceptor1Comp.getNegative(TestExamplePort.class), Channel.TWO_WAY);
            
            trigger(Start.event, testTerminationComp.control());
            trigger(Start.event, testExampleComp.control());
            trigger(Start.event, interceptor1Comp.control());
        }
    };
    
    public static class Init extends se.sics.kompics.Init<TestExampleHostComp> {
        public final AsyncToSyncCallbackI port1Callback;
        public final long roundLength;
        public final int testRounds;
        
        public Init(AsyncToSyncCallbackI testExamplePortCallback, long roundLength, int testRounds) {
            this.port1Callback = testExamplePortCallback;
            this.roundLength = roundLength;
            this.testRounds = testRounds;
        }
    }
}
