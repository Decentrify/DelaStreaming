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
package se.sics.gvod.stream.mngr.hops;

import java.util.Random;
import org.apache.avro.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.gvod.stream.mngr.VoDMngrComp;
import se.sics.gvod.stream.mngr.hops.helper.event.HDFSAvroFileCreateEvent;
import se.sics.gvod.stream.mngr.hops.helper.event.HDFSConnectionEvent;
import se.sics.gvod.stream.mngr.hops.helper.event.HDFSFileCreateEvent;
import se.sics.gvod.stream.mngr.hops.helper.event.HDFSFileDeleteEvent;
import se.sics.kompics.ComponentProxy;
import se.sics.kompics.Handler;
import se.sics.kompics.Negative;
import se.sics.ktoolbox.hdfs.HDFSHelper;
import se.sics.ktoolbox.kafka.KafkaHelper;
import se.sics.ktoolbox.kafka.producer.AvroParser;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class HopsHelperMngr {
    private static final Logger LOG = LoggerFactory.getLogger(VoDMngrComp.class);
    private String logPrefix = "";
    
    private final ComponentProxy proxy;
    private final Negative<HopsPort> hdfsPort;
    
    public HopsHelperMngr(ComponentProxy proxy, String logPrefix) {
        this.proxy = proxy;
        this.logPrefix = logPrefix;
        hdfsPort = proxy.getPositive(HopsPort.class).getPair();
    }
    
    public void subscribe() {
        proxy.subscribe(handleHDFSConnection, hdfsPort);
        proxy.subscribe(handleFileCreate, hdfsPort);
        proxy.subscribe(handleAvroFileCreate, hdfsPort);
        proxy.subscribe(handleFileDelete, hdfsPort);
    }
    
    Handler handleHDFSConnection = new Handler<HDFSConnectionEvent.Request>() {
        @Override
        public void handle(HDFSConnectionEvent.Request req) {
            LOG.trace("{}received:{}", logPrefix, req);
            boolean result = HDFSHelper.canConnect(req.connection.hdfsConfig);
            if (result) {
                proxy.answer(req, req.success());
            } else {
                proxy.answer(req, req.fail("cannot connect to hops"));
            }
        }
    };
    
    Handler handleFileDelete = new Handler<HDFSFileDeleteEvent.Request>() {
        @Override
        public void handle(HDFSFileDeleteEvent.Request req) {
            LOG.trace("{}received:{}", logPrefix, req);
            boolean result = HDFSHelper.delete(req.resource);
            if (result) {
                proxy.answer(req, req.success());
            } else {
                proxy.answer(req, req.fail("could not delete file"));
            }
        }
    };

    Handler handleFileCreate = new Handler<HDFSFileCreateEvent.Request>() {
        @Override
        public void handle(HDFSFileCreateEvent.Request req) {
            LOG.trace("{}received:{}", logPrefix, req);
            boolean result = HDFSHelper.create(req.resource, req.fileSize);
            if (result) {
                proxy.answer(req, req.success());
            } else {
                proxy.answer(req, req.fail("could not create file"));
            }
        }
    };
    
    Handler handleAvroFileCreate = new Handler<HDFSAvroFileCreateEvent.Request>() {
        @Override
        public void handle(HDFSAvroFileCreateEvent.Request req) {
            LOG.trace("{}received:{}", logPrefix, req);
            Random rand = new Random(1234);
            boolean result = HDFSHelper.simpleCreate(req.hdfsResource);
            Schema avroSchema = KafkaHelper.getKafkaSchema(req.kafkaResource);
            long filesize = 0;
            for(int i = 0; i < req.nrMsgs / 100; i++) {
                filesize += HDFSHelper.append(req.hdfsResource, AvroParser.nAvroToBlob(avroSchema, 100, rand));
            }
            int leftover = (int)(req.nrMsgs % 100);
            if(leftover != 0) {
                filesize += HDFSHelper.append(req.hdfsResource, AvroParser.nAvroToBlob(avroSchema, leftover, rand));
            } 
            proxy.answer(req, req.success(filesize));
        }
    };
}
