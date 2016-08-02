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

import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.Handler;
import se.sics.kompics.Negative;
import se.sics.kompics.Start;
import se.sics.ktoolbox.util.result.Result;
import se.sics.nstream.storage.StorageRead;
import se.sics.nstream.storage.StorageWrite;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class HDFSComp extends ComponentDefinition {
    private final static Logger LOG = LoggerFactory.getLogger(HDFSComp.class);
    private String logPrefix = "";
    
    Negative<HDFSPort> streamPort = provides(HDFSPort.class);
    private final HDFSEndpoint hdfsEndpoint;
    private final UserGroupInformation ugi;
    
    public HDFSComp(Init init) {
        LOG.info("{}init", logPrefix);
        
        hdfsEndpoint = init.hdfsEndpoint;
        ugi = UserGroupInformation.createRemoteUser(hdfsEndpoint.user);
        
        subscribe(handleStart, control);
        subscribe(handleReadRequest, streamPort);
        subscribe(handleWriteRequest, streamPort);
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
    Handler handleReadRequest = new Handler<StorageRead.Request>() {
        @Override
        public void handle(StorageRead.Request req) {
            LOG.trace("{}received:{}", logPrefix, req);
            Result<byte[]> readResult = HDFSHelper.read(ugi, hdfsEndpoint, (HDFSResource)req.resource, req.readRange);
            StorageRead.Response resp = req.respond(readResult);
            LOG.trace("{}answering:{}", logPrefix, resp);
            answer(req, resp);
        }
    };
    
    Handler handleWriteRequest = new Handler<StorageWrite.Request>() {
        @Override
        public void handle(StorageWrite.Request req) {
            LOG.trace("{}received:{}", logPrefix, req);
            if(req.pos == 0) {
                HDFSHelper.simpleCreate(ugi, hdfsEndpoint, (HDFSResource)req.resource);
            }
            Result<Boolean> writeResult = HDFSHelper.append(ugi, hdfsEndpoint, (HDFSResource)req.resource, req.value);
            StorageWrite.Response resp = req.respond(writeResult);
            LOG.trace("{}answering:{}", logPrefix, resp);
            
        }
    };
    
    public static class Init extends se.sics.kompics.Init<HDFSComp> {
        public final HDFSEndpoint hdfsEndpoint;
        
        public Init(HDFSEndpoint hdfsEndpoint) {
            this.hdfsEndpoint = hdfsEndpoint;
        }
    }
}
