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

import java.io.IOException;
import se.sics.nstream.hops.storage.hdfs.HDFSHelper;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import java.util.logging.Level;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.security.UserGroupInformation;
import org.javatuples.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.kompics.Component;
import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.Handler;
import se.sics.kompics.Negative;
import se.sics.kompics.Positive;
import se.sics.kompics.Start;
import se.sics.kompics.timer.CancelTimeout;
import se.sics.kompics.timer.SchedulePeriodicTimeout;
import se.sics.kompics.timer.Timeout;
import se.sics.kompics.timer.Timer;
import se.sics.kompics.util.Identifier;
import se.sics.ktoolbox.util.network.ports.One2NChannel;
import se.sics.ktoolbox.util.result.Result;
import se.sics.ktoolbox.util.trysf.Try;
import se.sics.ktoolbox.util.trysf.TryHelper;
import se.sics.nstream.hops.storage.hdfs.HDFSEndpoint;
import se.sics.nstream.hops.storage.hdfs.HDFSResource;
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

  Positive<Timer> timerPort = requires(Timer.class);
  Negative<DStoragePort> resourcePort = provides(DStoragePort.class);
  One2NChannel resourceChannel;

  private Map<Identifier, Component> components = new HashMap<>();
  private final HDFSEndpoint hdfsEndpoint;
  private final HDFSResource hdfsResource;
  private final DistributedFileSystem dfs;
  private final UserGroupInformation ugi;
  private long writePos;
  private final TreeMap<Long, DStorageWrite.Request> pending = new TreeMap<>();
  //
  private boolean progressed = false;
  private UUID flushTimer;
  private int forceFlushCounter;

  public HDFSComp(Init init) {
    LOG.info("{}init", logPrefix);

    hdfsEndpoint = init.endpoint;
    hdfsResource = init.resource;
    dfs = init.dfs;
    ugi = init.ugi;
    writePos = init.streamPos;
    forceFlushCounter = forceFlushCounter();

    subscribe(handleStart, control);
    subscribe(handleFlush, timerPort);
    subscribe(handleReadRequest, resourcePort);
    subscribe(handleWriteRequest, resourcePort);
  }

  private int forceFlushCounter() {
    Try<Long> hdfsBlockSize = HDFSHelper.blockSize(dfs, ugi, hdfsEndpoint, hdfsResource);
    long forceFlushDataSize
      = Math.max(HardCodedConfig.minForceFlushDataSize, (hdfsBlockSize.isSuccess() ? hdfsBlockSize.get() : 0));
    int counter = (int) (forceFlushDataSize / HardCodedConfig.delaBlockSize);
    return counter;
  }

  //********************************CONTROL***********************************
  Handler handleStart = new Handler<Start>() {
    @Override
    public void handle(Start event) {
      LOG.info("{}starting", logPrefix);
      schedulePeriodicCheck();
    }
  };

  @Override
  public void tearDown() {
    cancelPeriodicCheck();
  }
  //**************************************************************************
  Handler handleReadRequest = new Handler<DStorageRead.Request>() {
    @Override
    public void handle(DStorageRead.Request req) {
      LOG.trace("{}received:{}", logPrefix, req);
      Try<byte[]> readResult = HDFSHelper.read(dfs, ugi, hdfsEndpoint, hdfsResource, req.readRange);
      DStorageRead.Response resp = req.respond(convert(readResult));
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
      } else {
        writeValue = req.value;
      }

      Try<Boolean> writeResult = HDFSHelper.append(dfs, ugi, hdfsEndpoint, hdfsResource, writeValue);
      if (writeResult.isSuccess()) {
        writePos += writeValue.length;
        pending.put(writePos, req);
        if (pending.size() > forceFlushCounter) {
          inspectHDFSFile();
        }
      } else {
        DStorageWrite.Response resp = req.respond(convert(writeResult));
        answer(req, req.respond(Result.success(true)));
      }
    }
  };

  Handler handleFlush = new Handler<FlushTimeout>() {

    @Override
    public void handle(FlushTimeout event) {
      if (pending.isEmpty() || progressed) {
        progressed = false;
        return;
      }
      HDFSHelper.flush(dfs, ugi, hdfsEndpoint, hdfsResource);
      inspectHDFSFile();
      progressed = false;
    }
  };

  private void inspectHDFSFile() {
    Try<Long> currentFilePos = HDFSHelper.length(dfs, ugi, hdfsEndpoint, hdfsResource);
    if (!currentFilePos.isSuccess()) {
      return;
    }
    Iterator<Map.Entry<Long, DStorageWrite.Request>> it = pending.entrySet().iterator();
    while (it.hasNext()) {
      Map.Entry<Long, DStorageWrite.Request> next = it.next();
      if (next.getKey() <= currentFilePos.get()) {
        answer(next.getValue(), next.getValue().respond(Result.success(true)));
        it.remove();
        progressed = true;
      } else {
        break;
      }
    }
  }

  private void schedulePeriodicCheck() {
    if (flushTimer != null) {
      return;
    }
    SchedulePeriodicTimeout spt = new SchedulePeriodicTimeout(HardCodedConfig.flushPeriod, HardCodedConfig.flushPeriod);
    FlushTimeout sc = new FlushTimeout(spt);
    spt.setTimeoutEvent(sc);
    flushTimer = sc.getTimeoutId();
    trigger(spt, timerPort);
  }

  private void cancelPeriodicCheck() {
    if (flushTimer == null) {
      return;
    }
    CancelTimeout cpt = new CancelTimeout(flushTimer);
    flushTimer = null;
    trigger(cpt, timerPort);
  }

  public static class Init extends se.sics.kompics.Init<HDFSComp> {

    public final HDFSEndpoint endpoint;
    public final HDFSResource resource;
    public final UserGroupInformation ugi;
    public final DistributedFileSystem dfs;
    public final long streamPos;

    public Init(DistributedFileSystem dfs, UserGroupInformation ugi, HDFSEndpoint endpoint, HDFSResource resource, 
      long streamPos) {
      this.endpoint = endpoint;
      this.resource = resource;
      this.ugi = ugi;
      this.dfs = dfs;
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
      DistributedFileSystem dfs;
      try {
        dfs = (DistributedFileSystem)FileSystem.get(endpoint.hdfsConfig);
      } catch (IOException ex) {
        throw new RuntimeException(ex);
      }
      HDFSResource hdfsResource = (HDFSResource) resource;
      UserGroupInformation ugi = UserGroupInformation.createRemoteUser(endpoint.user);
      Try<Long> streamPos = HDFSHelper.length(dfs, ugi, endpoint, hdfsResource);
      if (!streamPos.isSuccess()) {
        throw new RuntimeException(TryHelper.tryError(streamPos));
      }
      if (streamPos.get() == -1) {
        Try<Boolean> simpleCreate = HDFSHelper.simpleCreate(dfs, ugi, endpoint, hdfsResource);
        if(simpleCreate.isFailure()) {
          throw new RuntimeException(TryHelper.tryError(simpleCreate));
        }
      }
      HDFSComp.Init init = new HDFSComp.Init(dfs, ugi, endpoint, hdfsResource, streamPos.get());
      return Pair.with(init, streamPos.get());
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
  private <O> Result<O> convert(Try<O> result) {
    if (result.isSuccess()) {
      return Result.success(result.get());
    } else {
      return Result.internalFailure((Exception) TryHelper.tryError(result));
    }
  }

  public static class FlushTimeout extends Timeout {

    public FlushTimeout(SchedulePeriodicTimeout spt) {
      super(spt);
    }
  }

  public static class HardCodedConfig {

    public static final long flushPeriod = 1000;
    public static final long delaBlockSize = 10 * 1024 * 1024; //10MB
    public static final long minForceFlushDataSize = 100 * 1024 * 1024; //100MB
  }
}
