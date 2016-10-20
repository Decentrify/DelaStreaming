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

import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.kompics.ComponentProxy;
import se.sics.kompics.Handler;
import se.sics.kompics.Negative;
import se.sics.nstream.storage.durable.DStoragePort;
import se.sics.nstream.storage.durable.events.DStorageRead;
import se.sics.nstream.storage.durable.events.DStorageWrite;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class KafkaProxy {

    private final static Logger LOG = LoggerFactory.getLogger(KafkaComp.class);
    private String logPrefix = "";

    private final ComponentProxy proxy;
    Negative<DStoragePort> streamPort;
    //**************************************************************************
    private final KafkaEndpoint kafkaEndpoint;
    private final KafkaResource kafkaResource;
    //<topic, producer>
    private final Map<String, KafkaProducerMngr> producers = new HashMap<>();

    public KafkaProxy(ComponentProxy proxy, KafkaEndpoint kafkaEndpoint, KafkaResource kafkaResource) {
        LOG.info("{}init", logPrefix);
        this.proxy = proxy;
        this.kafkaEndpoint = kafkaEndpoint;
        this.kafkaResource = kafkaResource;
        //proxy specific
        streamPort = proxy.getPositive(DStoragePort.class).getPair();
        //proxy adapted
        proxy.subscribe(handleReadRequest, streamPort);
        proxy.subscribe(handleWriteRequest, streamPort);
    }

    //**************************************************************************
    Handler handleReadRequest = new Handler<DStorageRead.Request>() {
        @Override
        public void handle(DStorageRead.Request req) {
            throw new RuntimeException("Kafka does not support reads");
        }
    };

    Handler handleWriteRequest = new Handler<DStorageWrite.Request>() {
        @Override
        public void handle(DStorageWrite.Request req) {
            LOG.trace("{}received:{}", logPrefix, req);
            KafkaProducerMngr mngr = producers.get(kafkaResource.topicName);
            if (mngr == null) {
                mngr = new KafkaProducerMngr(proxy, kafkaEndpoint, kafkaResource);
                mngr.start();
                producers.put(kafkaResource.topicName, mngr);
            }
            mngr.write(req);
            LOG.debug("{}produced:{}", logPrefix, mngr.producedMsgs);
        }
    };

    public void start() {
    }

    public void close() {
        for (KafkaProducerMngr mngr : producers.values()) {
            mngr.close();
        }
        producers.clear();
    }
}
