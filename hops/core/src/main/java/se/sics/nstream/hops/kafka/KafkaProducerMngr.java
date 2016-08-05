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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.util.LinkedList;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.kompics.ComponentProxy;
import se.sics.ktoolbox.util.result.Result;
import se.sics.nstream.hops.kafka.avro.AvroMsgProducer;
import se.sics.nstream.hops.kafka.avro.AvroParser;
import se.sics.nstream.storage.StorageWrite;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class KafkaProducerMngr {

    private final static Logger LOG = LoggerFactory.getLogger(KafkaComp.class);
    private String logPrefix = "";

    private final ComponentProxy proxy;

    private final AvroMsgProducer producer;
    public int producedMsgs = 0;
    private final List<StorageWrite.Request> waitingOnLeftover = new LinkedList<>();
    private ByteBuf leftover;

    public KafkaProducerMngr(ComponentProxy proxy, KafkaEndpoint kafkaEndpoint, KafkaResource kafkaResource) {
        this.proxy = proxy;
        this.producer = kafkaEndpoint.getProducer(kafkaResource);
        leftover = Unpooled.buffer();
    }

    public void write(StorageWrite.Request req) {
        Schema schema = producer.getSchema();
        leftover.writeBytes(req.value);
        getRidOfLeftovers(schema);
        parseWithLeftovers(schema, req);
    }

    private void getRidOfLeftovers(Schema schema) {
        if (!waitingOnLeftover.isEmpty()) {
            GenericRecord record = AvroParser.blobToAvro(schema, leftover);
            if (record != null) {
                producedMsgs++;
                producer.append(record);
                for (StorageWrite.Request req : waitingOnLeftover) {
                    StorageWrite.Response resp = req.respond(Result.success(true));
                    LOG.trace("{}answering:{}", logPrefix, resp);
                    proxy.answer(req, resp);
                }
                waitingOnLeftover.clear();
            }
        }
    }

    private void parseWithLeftovers(Schema schema, StorageWrite.Request req) {
        while (true) {
            GenericRecord record = AvroParser.blobToAvro(schema, leftover);
            if (record != null) {
                producedMsgs++;
                producer.append(record);
            } else {
                int leftoverSize = leftover.writerIndex() - leftover.readerIndex();
                if (leftoverSize > 0) {
                    LOG.debug("{}leftover:{}", logPrefix, leftoverSize);

                    byte[] newLeftover = new byte[leftoverSize];
                    leftover.readBytes(newLeftover);
                    leftover = Unpooled.buffer();
                    leftover.writeBytes(newLeftover);
                    //confirm write only when all data was written - for the moment there are some leftovers
                    waitingOnLeftover.add(req);
                } else {
                    leftover = Unpooled.buffer();
                    StorageWrite.Response resp = req.respond(Result.success(true));
                    LOG.trace("{}answering:{}", logPrefix, resp);
                    proxy.answer(req, resp);
                }
                break;
            }
        }
    }

    public void start() {
    }

    public void close() {
    }
}
