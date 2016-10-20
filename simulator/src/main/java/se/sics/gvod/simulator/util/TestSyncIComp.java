/*
 * Copyright (C) 2016 Swedish Institute of Computer Science (SICS) Copyright (C)
 * 2016 Royal Institute of Technology (KTH)
 *
 * Dozy is free software; you can redistribute it and/or
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

import java.util.List;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.Handler;
import se.sics.kompics.KompicsEvent;
import se.sics.kompics.Negative;
import se.sics.kompics.PortType;
import se.sics.kompics.Positive;
import se.sics.kompics.Start;
import se.sics.kompics.simulator.util.GlobalView;
import se.sics.kompics.timer.CancelTimeout;
import se.sics.kompics.timer.ScheduleTimeout;
import se.sics.kompics.timer.Timeout;
import se.sics.kompics.timer.Timer;
import se.sics.ktoolbox.util.identifiable.Identifier;
import se.sics.ktoolbox.util.identifiable.basic.UUIDIdentifier;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class TestSyncIComp extends ComponentDefinition implements TestSyncI {

    private static final Logger LOG = LoggerFactory.getLogger(TestSyncIComp.class);
    private String logPrefix = "";

    //*******************************CONENCTIONS********************************
    //****************************CONNECT_EXTERNALY*****************************
    private Positive<Timer> timerPort = requires(Timer.class);
    private Positive servicePort;
    //******************************INTERNAL_ONLY*******************************
    private Negative<TestSyncPort> selfPort = provides(TestSyncPort.class);
    //******************************INTERNAL_STATE******************************
    private AsyncToSyncCallbackI portInterceptor;
    //********************************AUX_STATE*********************************

    public TestSyncIComp(Init init) {
        LOG.info("{}initiating...", logPrefix);
        
        portInterceptor = init.portInterceptor;
        servicePort = requires(init.portType);

        subscribe(handleStart, control);

        for (Class<? extends KompicsEvent> responseType : init.responseTypes) {
            LOG.info("{}subscribing handler for:{} on:{}", new Object[]{logPrefix, responseType, servicePort.getPortType().getClass().getCanonicalName()});
            Handler responseHandler = new Handler(responseType) {
                @Override
                public void handle(KompicsEvent event) {
                    LOG.info("{}callback", logPrefix);
                    portInterceptor.callback(event, config().getValue("simulation.globalview", GlobalView.class));
                }
            };
            subscribe(responseHandler, servicePort);
        }
    }

    private Handler handleStart = new Handler<Start>() {
        @Override
        public void handle(Start event) {
            LOG.info("{}starting...", logPrefix);
        }
    };

    public static class Init extends se.sics.kompics.Init<TestSyncIComp> {

        public final Class<? extends PortType> portType;
        public final List<Class<? extends KompicsEvent>> responseTypes;
        public final AsyncToSyncCallbackI portInterceptor;

        public Init(Class<? extends PortType> portType, List<Class<? extends KompicsEvent>> responseTypes, AsyncToSyncCallbackI callback) {
            this.portType = portType;
            this.responseTypes = responseTypes;
            this.portInterceptor = callback;
        }
    }

    private void cancelTimeout(UUID timeoutId) {
        CancelTimeout cpt = new CancelTimeout(timeoutId);
        trigger(cpt, timerPort);

    }

    private UUID scheduleRequestTimeout(long timeout) {
        ScheduleTimeout spt = new ScheduleTimeout(timeout);
        RequestTimeout sc = new RequestTimeout(spt);
        spt.setTimeoutEvent(sc);
        trigger(spt, timerPort);
        return sc.getTimeoutId();
    }

    private static class RequestTimeout extends Timeout {

        RequestTimeout(ScheduleTimeout st) {
            super(st);
        }

        @Override
        public String toString() {
            return "RequestTimeout<" + getId() + ">";
        }

        public Identifier getId() {
            return new UUIDIdentifier(getTimeoutId());
        }
    }
}
