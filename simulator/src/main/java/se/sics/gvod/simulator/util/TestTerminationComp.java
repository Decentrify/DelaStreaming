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
package se.sics.gvod.simulator.util;

import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.Handler;
import se.sics.kompics.Positive;
import se.sics.kompics.Start;
import se.sics.kompics.simulator.util.GlobalView;
import se.sics.kompics.timer.SchedulePeriodicTimeout;
import se.sics.kompics.timer.Timeout;
import se.sics.kompics.timer.Timer;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class TestTerminationComp extends ComponentDefinition {

    private static final Logger LOG = LoggerFactory.getLogger(TestTerminationComp.class);
    private String logPrefix = "";

    //*******************************CONENCTIONS********************************
    //****************************CONNECT_EXTERNALY*****************************
    private Positive<Timer> timerPort = requires(Timer.class);
    //*****************************INTERNAL_STATE*******************************
    private UUID keepAliveTId;
    private long roundLength;
    private int aliveRounds;

    public TestTerminationComp(Init init) {
        LOG.info("{}initiating...", logPrefix);
        subscribe(handleStart, control);
        subscribe(handleCheck, timerPort);
        
        aliveRounds = init.testRounds;
        roundLength = init.roundLength;

        GlobalView gv = config().getValue("simulation.globalview", GlobalView.class);
        gv.setValue("simulation.example.termination", false);
        gv.setValue("simulation.example.success", true);
    }

    Handler handleStart = new Handler<Start>() {
        @Override
        public void handle(Start event) {
            LOG.info("{}starting...", logPrefix);
            scheduleKeepAliveTimeout();
        }
    };

    Handler handleCheck = new Handler<KeepSimulationAlive>() {
        @Override
        public void handle(KeepSimulationAlive event) {
            GlobalView gv = config().getValue("simulation.globalview", GlobalView.class);
            LOG.debug("{}periodic check", logPrefix);
            if(gv.getValue("simulation.example.termination", Boolean.class).equals(Boolean.TRUE)) {
                if(gv.getValue("simulation.example.success", Boolean.class).equals(Boolean.TRUE)) {
                    gv.terminate();
                } else {
                    LOG.error("{}simulation terminated with a fail", logPrefix);
                    gv.terminate();
                    throw new RuntimeException("simulation terminated with a fail");
                }
            }
            aliveRounds--;
            if(aliveRounds == 0) {
                LOG.error("{}simulation did not terminate in time", logPrefix);
                    gv.terminate();
                    throw new RuntimeException("simulation did not terminate in time");
            }
        }
    };

    public static class Init extends se.sics.kompics.Init<TestTerminationComp> {
        public final int testRounds;
        public final long roundLength;
        
        public Init(int testRoundLength, long roundLength) {
            this.testRounds = testRoundLength;
            this.roundLength = roundLength;
        }
    }

    private void scheduleKeepAliveTimeout() {
        SchedulePeriodicTimeout spt = new SchedulePeriodicTimeout(roundLength, roundLength);
        Timeout sc = new KeepSimulationAlive(spt);
        spt.setTimeoutEvent(sc);
        trigger(spt, timerPort);
        keepAliveTId = sc.getTimeoutId();
    }

    private static class KeepSimulationAlive extends Timeout {

        KeepSimulationAlive(SchedulePeriodicTimeout spt) {
            super(spt);
        }
    }
}
