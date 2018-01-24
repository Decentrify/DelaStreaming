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

import org.javatuples.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.Handler;
import se.sics.kompics.Negative;
import se.sics.kompics.Positive;
import se.sics.kompics.Start;
import se.sics.kompics.timer.Timer;
import se.sics.kompics.util.Identifier;
import se.sics.nstream.storage.durable.DStoragePort;
import se.sics.nstream.storage.durable.DurableStorageProvider;
import se.sics.nstream.storage.durable.util.StreamEndpoint;
import se.sics.nstream.storage.durable.util.StreamResource;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class KafkaComp extends ComponentDefinition {

    private final static Logger LOG = LoggerFactory.getLogger(KafkaComp.class);
    private String logPrefix = "";

    Positive<Timer> timerPort = requires(Timer.class);
    Negative<DStoragePort> streamPort = provides(DStoragePort.class);
    KafkaProxy kafka;
    
    public KafkaComp(Init init) {
        LOG.info("{}init", logPrefix);
        kafka = new KafkaProxy(proxy, init.endpoint, init.resource);
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

        public final KafkaEndpoint endpoint;
        public final KafkaResource resource;

        public Init(KafkaEndpoint endpoint, KafkaResource resource) {
            this.endpoint = endpoint;
            this.resource = resource;
        }
    }
    
     public static class StorageProvider implements DurableStorageProvider<KafkaComp> {

        public final Identifier self;
        public final KafkaEndpoint endpoint;

        public StorageProvider(Identifier self, KafkaEndpoint endpoint) {
            this.self = self;
            this.endpoint = endpoint;
        }

        @Override
        public Pair<KafkaComp.Init, Long> initiate(StreamResource resource) {
            KafkaResource kafkaResource = (KafkaResource) resource;
            KafkaComp.Init init = new KafkaComp.Init(endpoint, kafkaResource);
            //TODO Alex - any way to find the actual kafka position?
            return Pair.with(init, 0l);
        }

        @Override
        public String getName() {
            return endpoint.getEndpointName();
        }

        @Override
        public Class<KafkaComp> getStorageDefinition() {
            return KafkaComp.class;
        }

        @Override
        public StreamEndpoint getEndpoint() {
            return endpoint;
        }
    }
}
