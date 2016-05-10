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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.Handler;
import se.sics.kompics.Negative;
import se.sics.kompics.Start;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class TestExampleComp extends ComponentDefinition {

    private static final Logger LOG = LoggerFactory.getLogger(TestExampleComp.class);
    private String logPrefix = "";
    
    final  Negative<TestExamplePort> testExamplePort = provides(TestExamplePort.class);

    public TestExampleComp(Init init) {
        LOG.info("{}initiating...", logPrefix);
    
        subscribe(handleStart, control);
        subscribe(handleRequest, testExamplePort);
    }
    
    Handler handleStart = new Handler<Start>() {
        @Override
        public void handle(Start event) {
            LOG.info("{}starting...", logPrefix);
            trigger(new TestExampleEvent.Indication(), testExamplePort);
        }
    };
    
    Handler handleRequest = new Handler<TestExampleEvent.Request>() {
        @Override
        public void handle(TestExampleEvent.Request request) {
            LOG.info("{}received:{}", logPrefix, request);
            answer(request, request.answer());
        }
    };
    
    public static class Init extends se.sics.kompics.Init<TestExampleComp> {
    }
}
