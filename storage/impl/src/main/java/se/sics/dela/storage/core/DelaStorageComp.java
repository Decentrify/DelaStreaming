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
package se.sics.dela.storage.core;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;
import se.sics.dela.storage.common.DelaStorageProvider;
import se.sics.dela.storage.hdfs.HDFS.AppendSession;
import se.sics.dela.storage.hdfs.HDFS.ReadSession;
import se.sics.dela.storage.operation.StreamStorageOpPort;
import se.sics.dela.storage.operation.events.StreamStorageOpRead;
import se.sics.dela.storage.operation.events.StreamStorageOpWrite;
import se.sics.dela.util.TimerProxyImpl;
import se.sics.kompics.Component;
import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.Handler;
import se.sics.kompics.Negative;
import se.sics.kompics.Positive;
import se.sics.kompics.Start;
import se.sics.kompics.timer.Timer;
import se.sics.kompics.util.Identifier;
import se.sics.ktoolbox.util.network.ports.One2NChannel;
import se.sics.ktoolbox.util.result.Result;
import se.sics.ktoolbox.util.trysf.Try;
import se.sics.ktoolbox.util.trysf.TryHelper;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class DelaStorageComp extends ComponentDefinition {

  private String logPrefix = "";

  Positive<Timer> timerPort = requires(Timer.class);
  Negative<StreamStorageOpPort> resourcePort = provides(StreamStorageOpPort.class);
  One2NChannel resourceChannel;
  private final TimerProxyImpl timerProxy;

  private Map<Identifier, Component> components = new HashMap<>();
  private final DelaStorageProvider storage;
  private Optional<AppendSession> appendSession;
  private Optional<ReadSession> readSession;
  private long pendingPos;
  private long confirmedPos;
  private final Map<Identifier, StreamStorageOpRead.Request> pendingReads = new HashMap<>();
  private final Map<Identifier, StreamStorageOpWrite.Request> pendingWrites = new HashMap<>();

  public DelaStorageComp(Init init) {
    logger.info("{}init", logPrefix);

    timerProxy = new TimerProxyImpl();
    timerProxy.setup(proxy);
    storage = init.storage;
    pendingPos = init.pos;
    confirmedPos = pendingPos;

    subscribe(handleStart, control);
    subscribe(handleReadRequest, resourcePort);
    subscribe(handleWriteRequest, resourcePort);
    subscribe(handleReadComplete, resourcePort);
    subscribe(handleWriteComplete, resourcePort);
  }

  //********************************CONTROL***********************************
  Handler handleStart = new Handler<Start>() {
    @Override
    public void handle(Start event) {
      logger.info("{}starting", logPrefix);
    }
  };

  //**************************************************************************
  Handler handleReadRequest = new Handler<StreamStorageOpRead.Request>() {
    @Override
    public void handle(StreamStorageOpRead.Request req) {
      logger.trace("{}received:{}", logPrefix, req);
      if (!readSession.isPresent()) {
        setupReadSession();
      }
      pendingReads.put(req.getId(), req);
      readSession.get().read(req.readRange, readCallback(req));
    }
  };

  private void setupReadSession() {
    Try<ReadSession> rs = storage.readSession(timerProxy);
    if (rs.isSuccess()) {
      readSession = Optional.of(rs.get());
    } else {
      throw new RuntimeException(TryHelper.tryError(rs));
    }
  }

  private Consumer<Try<byte[]>> readCallback(StreamStorageOpRead.Request req) {
    return (Try<byte[]> result) -> {
      pendingReads.remove(req.getId());
      StreamStorageOpRead.Response resp = req.respond(result);
      logger.trace("{}answering:{}", logPrefix, resp);
      answer(req, resp);
    };
  }

  Handler handleWriteRequest = new Handler<StreamStorageOpWrite.Request>() {
    @Override
    public void handle(StreamStorageOpWrite.Request req) {
      logger.info("{}write:{}", logPrefix, req);
      if (!appendSession.isPresent()) {
        setupAppendSession();
      }
      if (pendingPos >= req.pos + req.value.length) {
        logger.info("{}write with pos:{} skipped", logPrefix, req.pos);
        answer(req, req.respond(Result.success(true)));
        return;
      }
      long pos = req.pos;
      byte[] writeValue = prepareAppendVal(req);
      pendingWrites.put(req.getId(), req);
      appendSession.get().append(writeValue, appendCallback(req));
    }
  };

  private void setupAppendSession() {
    Try<AppendSession> rs = storage.appendSession(timerProxy);
    if (rs.isSuccess()) {
      appendSession = Optional.of(rs.get());
    } else {
      throw new RuntimeException(TryHelper.tryError(rs));
    }
  }

  private byte[] prepareAppendVal(StreamStorageOpWrite.Request req) {
    byte[] appendVal = req.value;
    if (pendingPos > req.pos) {
      int sourcePos = (int) (pendingPos - req.pos);
      int writeAmount = req.value.length - sourcePos;
      appendVal = new byte[writeAmount];
      System.arraycopy(req.value, sourcePos, appendVal, 0, writeAmount);
      logger.info("{}convert write pos from:{} to:{} write amount from:{} to:{}",
        new Object[]{logPrefix, req.pos, pendingPos, req.value.length, writeAmount});
    }
    return appendVal;
  }

  private Consumer<Try<Boolean>> appendCallback(StreamStorageOpWrite.Request req) {
    return (Try<Boolean> result) -> {
      pendingWrites.remove(req.getId());
      if (confirmedPos < req.pos + req.value.length) {
        confirmedPos = req.pos + req.value.length;
      }
      StreamStorageOpWrite.Response resp = req.respond(result);
      logger.trace("{}answering:{}", logPrefix, resp);
      answer(req, resp);
    };
  }
  
  Handler handleReadComplete = new Handler<StreamStorageOpRead.Complete>() {
    @Override
    public void handle(StreamStorageOpRead.Complete event) {
      if(readSession.isPresent()) {
        readSession.get().close();
      }
    }
  };
  
  Handler handleWriteComplete = new Handler<StreamStorageOpWrite.Complete>() {
    @Override
    public void handle(StreamStorageOpWrite.Complete event) {
      if(appendSession.isPresent()) {
        appendSession.get().close();
      }
    }
  };

  public static class Init extends se.sics.kompics.Init<DelaStorageComp> {

    public final DelaStorageProvider storage;
    public final long pos;

    public Init(DelaStorageProvider storage, long pos) {
      this.storage = storage;
      this.pos = pos;
    }
  }
}
