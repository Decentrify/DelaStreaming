///*
// * Copyright (C) 2009 Swedish Institute of Computer Science (SICS) Copyright (C)
// * 2009 Royal Institute of Technology (KTH)
// *
// * KompicsToolbox is free software; you can redistribute it and/or
// * modify it under the terms of the GNU General Public License
// * as published by the Free Software Foundation; either version 2
// * of the License, or (at your option) any later version.
// *
// * This program is distributed in the hope that it will be useful,
// * but WITHOUT ANY WARRANTY; without even the implied warranty of
// * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// * GNU General Public License for more details.
// *
// * You should have received a copy of the GNU General Public License
// * along with this program; if not, write to the Free Software
// * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
// */
//package se.sics.dela.storage.common;
//
//import java.io.IOException;
//import java.util.HashMap;
//import java.util.Iterator;
//import java.util.Map;
//import java.util.TreeMap;
//import java.util.UUID;
//import org.apache.hadoop.fs.FileSystem;
//import org.apache.hadoop.hdfs.DistributedFileSystem;
//import org.javatuples.Pair;
//import org.slf4j.Logger;
//import se.sics.dela.storage.StorageEndpoint;
//import se.sics.dela.storage.StorageResource;
//import se.sics.dela.storage.hdfs.HDFSEndpoint;
//import se.sics.dela.storage.hdfs.HDFSHelper;
//import se.sics.dela.storage.hdfs.HDFSResource;
//import se.sics.dela.storage.operation.StreamStorageOpPort;
//import se.sics.dela.storage.operation.events.StreamStorageOpRead;
//import se.sics.dela.storage.operation.events.StreamStorageOpWrite;
//import se.sics.kompics.Component;
//import se.sics.kompics.ComponentDefinition;
//import se.sics.kompics.Handler;
//import se.sics.kompics.Negative;
//import se.sics.kompics.Positive;
//import se.sics.kompics.Start;
//import se.sics.kompics.timer.CancelTimeout;
//import se.sics.kompics.timer.SchedulePeriodicTimeout;
//import se.sics.kompics.timer.Timeout;
//import se.sics.kompics.timer.Timer;
//import se.sics.kompics.util.Identifier;
//import se.sics.ktoolbox.util.network.ports.One2NChannel;
//import se.sics.ktoolbox.util.result.Result;
//import se.sics.ktoolbox.util.trysf.Try;
//
///**
// * @author Alex Ormenisan <aaor@kth.se>
// */
//public class DelaStorageComp extends ComponentDefinition {
//
//  private String logPrefix = "";
//
//  Positive<Timer> timerPort = requires(Timer.class);
//  Negative<StreamStorageOpPort> resourcePort = provides(StreamStorageOpPort.class);
//  One2NChannel resourceChannel;
//
//  private Map<Identifier, Component> components = new HashMap<>();
//  private final StorageOp ops;
//  private final DelaStorageProvider storage;
//  private long writePos;
//  private final TreeMap<Long, StreamStorageOpWrite.Request> pending = new TreeMap<>();
//
//  public HDFSComp(Init init) {
//    logger.info("{}init", logPrefix);
//
//    storage = init.storage;
//    writePos = init.streamPos;
//
//    subscribe(handleStart, control);
//    subscribe(handleReadRequest, resourcePort);
//    subscribe(handleWriteRequest, resourcePort);
//  }
//
//  //********************************CONTROL***********************************
//  Handler handleStart = new Handler<Start>() {
//    @Override
//    public void handle(Start event) {
//      logger.info("{}starting", logPrefix);
//      schedulePeriodicCheck();
//    }
//  };
//
//  //**************************************************************************
//  Handler handleReadRequest = new Handler<StreamStorageOpRead.Request>() {
//    @Override
//    public void handle(StreamStorageOpRead.Request req) {
//      logger.trace("{}received:{}", logPrefix, req);
//      Try<byte[]> readResult = doAs.perform(ops.read(req.readRange));
//      StreamStorageOpRead.Response resp = req.respond(readResult);
//      logger.trace("{}answering:{}", logPrefix, resp);
//      answer(req, resp);
//    }
//  };
//
//  Handler handleWriteRequest = new Handler<StreamStorageOpWrite.Request>() {
//    @Override
//    public void handle(StreamStorageOpWrite.Request req) {
//      logger.info("{}write:{}", logPrefix, req);
//      if (writePos >= req.pos + req.value.length) {
//        logger.info("{}write with pos:{} skipped", logPrefix, req.pos);
//        answer(req, req.respond(Result.success(true)));
//        return;
//      }
//      long pos = req.pos;
//      byte[] writeValue = req.value;
//      if (writePos > req.pos) {
//        pos = writePos;
//        int sourcePos = (int) (pos - writePos);
//        int writeAmount = req.value.length - sourcePos;
//        writeValue = new byte[writeAmount];
//        System.arraycopy(req.value, sourcePos, writeValue, 0, writeAmount);
//        logger.info("{}convert write pos from:{} to:{} write amount from:{} to:{}",
//          new Object[]{logPrefix, req.pos, pos, req.value.length, writeAmount});
//      } else {
//        writeValue = req.value;
//      }
//
//      Try<Boolean> writeResult = doAs.perform(ops.append(writeValue));
//      if (writeResult.isSuccess()) {
//        writePos += writeValue.length;
//        pending.put(writePos, req);
//        if (pending.size() > forceFlushCounter) {
//          inspectHDFSFile();
//        }
//      } else {
//        StreamStorageOpWrite.Response resp = req.respond(writeResult);
//        answer(req, req.respond(Result.success(true)));
//      }
//    }
//  };
//
//  private void schedulePeriodicCheck() {
//    if (flushTimer != null) {
//      return;
//    }
//    SchedulePeriodicTimeout spt = new SchedulePeriodicTimeout(HardCodedConfig.flushPeriod, HardCodedConfig.flushPeriod);
//    FlushTimeout sc = new FlushTimeout(spt);
//    spt.setTimeoutEvent(sc);
//    flushTimer = sc.getTimeoutId();
//    trigger(spt, timerPort);
//  }
//
//  private void cancelPeriodicCheck() {
//    if (flushTimer == null) {
//      return;
//    }
//    CancelTimeout cpt = new CancelTimeout(flushTimer);
//    flushTimer = null;
//    trigger(cpt, timerPort);
//  }
//
//  public static class Init extends se.sics.kompics.Init<HDFSComp> {
//
//    public final HDFSHelper.StorageProvider storage;
//    public final HDFSHelper.DoAs doAs;
//    public final long streamPos;
//
//    public Init(HDFSHelper.StorageProvider storage, HDFSHelper.DoAs doAs, long streamPos) {
//      this.storage = storage;
//      this.doAs = doAs;
//      this.streamPos = streamPos;
//    }
//  }
//
//  public static class StorageProvider implements se.sics.dela.storage.mngr.StorageProvider<HDFSComp> {
//
//    public final Identifier self;
//    public final HDFSEndpoint endpoint;
//
//    public StorageProvider(Identifier self, HDFSEndpoint endpoint) {
//      this.self = self;
//      this.endpoint = endpoint;
//    }
//
//    @Override
//    public Pair<HDFSComp.Init, Long> initiate(StorageResource resource, Logger logger) {
//      HDFSResource hdfsResource = (HDFSResource) resource;
//      DistributedFileSystem dfs;
//      try {
//        dfs = (DistributedFileSystem)FileSystem.get(endpoint.hdfsConfig);
//      } catch (IOException ex) {
//        throw new RuntimeException(ex);
//      }
//      HDFSHelper.StorageProvider storage = new HDFSHelper.StorageProvider(endpoint, hdfsResource, dfs);
//      StorageOp ops = new StorageOp(storage);
//      HDFSHelper.DoAs doAs = HDFSHelper.doAs(endpoint.user).get();
//      Try<Long> streamPos = doAs.perform(ops.fileSize());
//      if (!streamPos.isSuccess()) {
//        try {
//          ((Try.Failure) streamPos).checkedGet();
//        } catch (Throwable t) {
//          throw new RuntimeException(t);
//        }
//      }
//      if (streamPos.get() == -1) {
//        doAs.perform(ops.createFile());
//      }
//      HDFSComp.Init init = new HDFSComp.Init(storage, doAs, streamPos.get());
//      return Pair.with(init, streamPos.get());
//    }
//
//    @Override
//    public String getName() {
//      return endpoint.getEndpointName();
//    }
//
//    @Override
//    public Class<HDFSComp> getStorageDefinition() {
//      return HDFSComp.class;
//    }
//
//    @Override
//    public StorageEndpoint getEndpoint() {
//      return endpoint;
//    }
//  }
//
//  public static class FlushTimeout extends Timeout {
//
//    public FlushTimeout(SchedulePeriodicTimeout spt) {
//      super(spt);
//    }
//  }
//
//  public static class HardCodedConfig {
//
//    public static final long flushPeriod = 1000;
//    public static final long delaBlockSize = 10 * 1024 * 1024; //10MB
//    public static final long minForceFlushDataSize = 100 * 1024 * 1024; //100MB
//  }
//}
