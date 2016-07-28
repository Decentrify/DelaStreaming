/*
 * Copyright (C) 2009 Swedish Institute of Computer Science (SICS) Copyright (C)
 * 2009 Royal Institute of Technology (KTH)
 *
 * KompicsToolbox is free software; you can redistribute it and/or
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
package se.sics.nstream.hops.kafka;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.Handler;
import se.sics.kompics.Negative;
import se.sics.kompics.Start;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class KafkaComp extends ComponentDefinition {

    private final static Logger LOG = LoggerFactory.getLogger(KafkaComp.class);
    private String logPrefix = "";

    Negative<KafkaPort> streamPort = provides(KafkaPort.class);
    KafkaProxy kafka;
    
    public KafkaComp(Init init) {
        LOG.info("{}init", logPrefix);
        kafka = new KafkaProxy(proxy, init.kafkaEndpoint);
        proxy.subscribe(handleStart, control);
    }

    //******************************CONTROL*************************************
    Handler handleStart = new Handler<Start>() {
        @Override
        public void handle(Start event) {
            LOG.info("{}starting", logPrefix);
            kafka.start();
        }
    };
    @Override
    public void tearDown() {
        kafka.close();
    }
    //**************************************************************************
    public static class Init extends se.sics.kompics.Init<KafkaComp> {

        public final KafkaEndpoint kafkaEndpoint;

        public Init(KafkaEndpoint kafkaEndpoint) {
            this.kafkaEndpoint = kafkaEndpoint;
        }
    }
}
