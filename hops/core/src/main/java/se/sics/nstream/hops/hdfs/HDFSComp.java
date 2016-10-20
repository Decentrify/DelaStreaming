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
package se.sics.nstream.hops.hdfs;

import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.security.UserGroupInformation;
import org.javatuples.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.kompics.Component;
import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.Handler;
import se.sics.kompics.Negative;
import se.sics.kompics.Start;
import se.sics.ktoolbox.util.identifiable.Identifier;
import se.sics.ktoolbox.util.network.ports.One2NChannel;
import se.sics.ktoolbox.util.result.Result;
import se.sics.nstream.storage.durable.DStoragePort;
import se.sics.nstream.storage.durable.DurableStorageProvider;
import se.sics.nstream.storage.durable.events.DStorageRead;
import se.sics.nstream.storage.durable.events.DStorageWrite;
import se.sics.nstream.storage.durable.util.StreamEndpoint;
import se.sics.nstream.storage.durable.util.StreamResource;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class HDFSComp extends ComponentDefinition {

    private final static Logger LOG = LoggerFactory.getLogger(HDFSComp.class);
    private String logPrefix = "";

    Negative<DStoragePort> resourcePort = provides(DStoragePort.class);
    One2NChannel resourceChannel;

    private Map<Identifier, Component> components = new HashMap<>();
    private final HDFSEndpoint hdfsEndpoint;
    private final HDFSResource hdfsResource;
    private final UserGroupInformation ugi;
    private long writePos;

    public HDFSComp(Init init) {
        LOG.info("{}init", logPrefix);

        hdfsEndpoint = init.endpoint;
        hdfsResource = init.resource;
        ugi = UserGroupInformation.createRemoteUser(hdfsEndpoint.user);
        writePos = init.streamPos;

        subscribe(handleStart, control);
        subscribe(handleReadRequest, resourcePort);
        subscribe(handleWriteRequest, resourcePort);
    }

    //********************************CONTROL***********************************
    Handler handleStart = new Handler<Start>() {
        @Override
        public void handle(Start event) {
            LOG.info("{}starting", logPrefix);
        }
    };

    @Override
    public void tearDown() {
    }
    //**************************************************************************
    Handler handleReadRequest = new Handler<DStorageRead.Request>() {
        @Override
        public void handle(DStorageRead.Request req) {
            LOG.trace("{}received:{}", logPrefix, req);
            Result<byte[]> readResult = HDFSHelper.read(ugi, hdfsEndpoint, hdfsResource, req.readRange);
            DStorageRead.Response resp = req.respond(readResult);
            LOG.trace("{}answering:{}", logPrefix, resp);
            answer(req, resp);
        }
    };

    Handler handleWriteRequest = new Handler<DStorageWrite.Request>() {
        @Override
        public void handle(DStorageWrite.Request req) {
            LOG.info("{}write:{}", logPrefix, req);
            if (writePos >= req.pos + req.value.length) {
                LOG.info("{}write with pos:{} skipped", logPrefix, req.pos);
                answer(req, req.respond(Result.success(true)));
                return;
            }
            long pos = req.pos;
            byte[] writeValue = req.value;
            if (writePos > req.pos) {
                pos = writePos;
                int sourcePos = (int) (pos - writePos);
                int writeAmount = req.value.length - sourcePos;
                writeValue = new byte[writeAmount];
                System.arraycopy(req.value, sourcePos, writeValue, 0, writeAmount);
                LOG.info("{}convert write pos from:{} to:{} write amount from:{} to:{}",
                        new Object[]{logPrefix, req.pos, pos, req.value.length, writeAmount});
            }

            Result<Boolean> writeResult = HDFSHelper.append(ugi, hdfsEndpoint, hdfsResource, req.value);
            DStorageWrite.Response resp = req.respond(writeResult);

            writePos += writeValue.length;
            answer(req, req.respond(Result.success(true)));
        }
    };

    public static class Init extends se.sics.kompics.Init<HDFSComp> {

        public final HDFSEndpoint endpoint;
        public final HDFSResource resource;
        public final UserGroupInformation ugi;
        public final long streamPos;

        public Init(HDFSEndpoint endpoint, HDFSResource resource, UserGroupInformation ugi, long streamPos) {
            this.endpoint = endpoint;
            this.resource = resource;
            this.ugi = ugi;
            this.streamPos = streamPos;
        }
    }

    public static class StorageProvider implements DurableStorageProvider<HDFSComp> {

        public final Identifier self;
        public final HDFSEndpoint endpoint;

        public StorageProvider(Identifier self, HDFSEndpoint endpoint) {
            this.self = self;
            this.endpoint = endpoint;
        }

        @Override
        public Pair<HDFSComp.Init, Long> initiate(StreamResource resource) {
            HDFSResource hdfsResource = (HDFSResource) resource;
            UserGroupInformation ugi = UserGroupInformation.createRemoteUser(endpoint.user);
            Result<Long> streamPos = HDFSHelper.length(ugi, endpoint, hdfsResource);
            if (!streamPos.isSuccess()) {
                throw new RuntimeException(streamPos.getException());
            }
            if (streamPos.getValue() == -1) {
                HDFSHelper.simpleCreate(ugi, endpoint, hdfsResource);
            }
            HDFSComp.Init init = new HDFSComp.Init(endpoint, hdfsResource, ugi, streamPos.getValue());
            return Pair.with(init, streamPos.getValue());
        }

        @Override
        public String getName() {
            return endpoint.getEndpointName();
        }

        @Override
        public Class<HDFSComp> getStorageDefinition() {
            return HDFSComp.class;
        }

        @Override
        public StreamEndpoint getEndpoint() {
            return endpoint;
        }
    }
}
