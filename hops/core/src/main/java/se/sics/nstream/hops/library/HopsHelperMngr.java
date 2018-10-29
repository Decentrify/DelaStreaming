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
package se.sics.nstream.hops.library;

import java.io.IOException;
import java.util.Random;
import org.apache.avro.Schema;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.kompics.ComponentProxy;
import se.sics.kompics.Handler;
import se.sics.kompics.Negative;
import se.sics.ktoolbox.util.result.Result;
import se.sics.ktoolbox.util.trysf.Try;
import se.sics.ktoolbox.util.trysf.TryHelper;
import se.sics.nstream.hops.storage.hdfs.HDFSHelper;
import se.sics.nstream.hops.kafka.KafkaHelper;
import se.sics.nstream.hops.kafka.avro.AvroParser;
import se.sics.nstream.hops.library.event.helper.HDFSAvroFileCreateEvent;
import se.sics.nstream.hops.library.event.helper.HDFSConnectionEvent;
import se.sics.nstream.hops.library.event.helper.HDFSFileCreateEvent;
import se.sics.nstream.hops.library.event.helper.HDFSFileDeleteEvent;
import se.sics.nstream.library.LibraryMngrComp;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class HopsHelperMngr {

    private static final Logger LOG = LoggerFactory.getLogger(LibraryMngrComp.class);
    private String logPrefix = "";

    private final ComponentProxy proxy;
    private final Negative<HopsHelperPort> hdfsPort;

    public HopsHelperMngr(ComponentProxy proxy,String logPrefix) {
        this.proxy = proxy;
        this.logPrefix = logPrefix;
        hdfsPort = proxy.getPositive(HopsHelperPort.class).getPair();
    }

    public void subscribe() {
        proxy.subscribe(handleHDFSConnection, hdfsPort);
        proxy.subscribe(handleFileCreate, hdfsPort);
        proxy.subscribe(handleAvroFileCreate, hdfsPort);
        proxy.subscribe(handleFileDelete, hdfsPort);
    }
    
    public void start() {
    }

    public void close() {
    }
    
    Handler handleHDFSConnection = new Handler<HDFSConnectionEvent.Request>() {
        @Override
        public void handle(HDFSConnectionEvent.Request req) {
            LOG.trace("{}received:{}", logPrefix, req);
            Try<Boolean> result = HDFSHelper.canConnect(req.connection.hdfsConfig);
            proxy.answer(req, req.answer(convert(result)));
        }
    };

    Handler handleFileDelete = new Handler<HDFSFileDeleteEvent.Request>() {
        @Override
        public void handle(HDFSFileDeleteEvent.Request req) {
            LOG.trace("{}received:{}", logPrefix, req);
            try (DistributedFileSystem dfs = (DistributedFileSystem) FileSystem.get(req.hdfsEndpoint.hdfsConfig)) {
              UserGroupInformation ugi = UserGroupInformation.createRemoteUser(req.hdfsEndpoint.user);
              Try<Boolean> result = HDFSHelper.delete(dfs, ugi, req.hdfsEndpoint, req.hdfsResource);
              proxy.answer(req, req.answer(convert(result)));
            } catch (IOException ex) {
              throw new RuntimeException(ex);
            }
        }
    };

    Handler handleFileCreate = new Handler<HDFSFileCreateEvent.Request>() {
        @Override
        public void handle(HDFSFileCreateEvent.Request req) {
            LOG.trace("{}received:{}", logPrefix, req);
            try (DistributedFileSystem dfs = (DistributedFileSystem) FileSystem.get(req.hdfsEndpoint.hdfsConfig)) {
              UserGroupInformation ugi = UserGroupInformation.createRemoteUser(req.hdfsEndpoint.user);
              Try<Boolean> result 
                = HDFSHelper.createWithLength(dfs, ugi, req.hdfsEndpoint, req.hdfsResource,req.fileSize);
              proxy.answer(req, req.answer(convert(result)));
            } catch (IOException ex) {
              throw new RuntimeException(ex);
            }
        }
    };

    Handler handleAvroFileCreate = new Handler<HDFSAvroFileCreateEvent.Request>() {
        @Override
        public void handle(HDFSAvroFileCreateEvent.Request req) {
            LOG.trace("{}received:{}", logPrefix, req);
            Random rand = new Random(1234);
            try (DistributedFileSystem dfs = (DistributedFileSystem) FileSystem.get(req.hdfsEndpoint.hdfsConfig)) {
              UserGroupInformation ugi = UserGroupInformation.createRemoteUser(req.hdfsEndpoint.user);

              Try<Boolean> createResult = HDFSHelper.simpleCreate(dfs, ugi, req.hdfsEndpoint, req.hdfsResource);
              if (!createResult.isSuccess()) {
                  proxy.answer(req, req.answer(TryHelper.tryError(createResult)));
              }
              Schema avroSchema = KafkaHelper.getKafkaSchemaByTopic(req.kafkaEndpoint, req.kafkaResource);
              long filesize = 0;
              Try<Boolean> appendResult;
              for (int i = 0; i < req.nrMsgs / 10000; i++) {
                  byte[] avroBlob = AvroParser.nAvroToBlob(avroSchema, 10000, rand);
                  filesize += avroBlob.length;
                  appendResult = HDFSHelper.append(dfs, ugi, req.hdfsEndpoint, req.hdfsResource, avroBlob);
                  if (!appendResult.isSuccess()) {
                      proxy.answer(req, req.answer(TryHelper.tryError(appendResult)));
                  }
              }
              int leftover = (int) (req.nrMsgs % 10000);
              if (leftover != 0) {
                  byte[] avroBlob = AvroParser.nAvroToBlob(avroSchema, leftover, rand);
                  filesize += avroBlob.length;
                  appendResult = HDFSHelper.append(dfs, ugi, req.hdfsEndpoint, req.hdfsResource, AvroParser.nAvroToBlob(avroSchema, leftover, rand));
                  if (!appendResult.isSuccess()) {
                      proxy.answer(req, req.answer(TryHelper.tryError(appendResult)));
                  }
              }
              proxy.answer(req, req.answer(filesize));
            } catch (IOException ex) {
              throw new RuntimeException(ex);
            }
        }
    };
    
    private <O> Result<O> convert(Try<O> result) {
      if(result.isSuccess()) {
        return Result.success(result.get());
      } else {
        return Result.internalFailure((Exception)TryHelper.tryError(result));
      }
    }
}
