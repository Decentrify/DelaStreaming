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
 * along with this program; if not, append to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
 */
package se.sics.dela.storage.core;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.logging.Level;
import java.util.logging.Logger;
import se.sics.dela.storage.common.DelaReadStream;
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
import se.sics.dela.storage.common.DelaAppendStream;
import se.sics.dela.storage.common.DelaFileHandler;
import se.sics.dela.storage.common.DelaStorageException;

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
  private final DelaFileHandler file;
  private Optional<DelaAppendStream> appendSession;
  private Optional<DelaReadStream> readSession;
  private long pendingPos;
  private long confirmedPos;
  private Consumer<Try<Boolean>> appendCompleted;
  private final Map<Identifier, StreamStorageOpRead.Request> pendingReads = new HashMap<>();
  private final Map<Identifier, StreamStorageOpWrite.Request> pendingWrites = new HashMap<>();

  public DelaStorageComp(Init init) {
    logger.info("{}init", logPrefix);

    timerProxy = new TimerProxyImpl();
    timerProxy.setup(proxy);
    file = init.storage;
    pendingPos = init.pos;
    confirmedPos = pendingPos;
    file.setTimerProxy(timerProxy);

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
    Try<DelaReadStream> rs = file.readStream();
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
      Try<byte[]> writeValue = prepareAppend(req);
      if (writeValue.isFailure()) {
        answer(req, req.respond((Try.Failure) writeValue));
      } else if (writeValue.get().length == 0) {
        answer(req, req.respond(Result.success(false)));
      } else {
        pendingWrites.put(req.getId(), req);
        appendSession.get().write(pendingPos, writeValue.get(), appendCallback(req));
        pendingPos += writeValue.get().length;
      }
    }
  };

  private void setupAppendSession() {
    appendCompleted = (Try<Boolean> result) -> {
      if(result.isFailure()) {
        throw new RuntimeException(TryHelper.tryError(result));
      }
      try {
        appendSession.get().close();
      } catch (IOException ex) {
        throw new RuntimeException(ex);
      }
      appendSession = Optional.empty();
    };
    Try<DelaAppendStream> rs = file.size()
      .flatMap(TryHelper.tryFSucc1((Long fileSize) -> file.appendStream(fileSize, appendCompleted)));
    if (rs.isSuccess()) {
      appendSession = Optional.of(rs.get());
    } else {
      throw new RuntimeException(TryHelper.tryError(rs));
    }
  }

  private Try<byte[]> prepareAppend(StreamStorageOpWrite.Request req) {
    if (pendingPos >= req.pos + req.value.length) {
      logger.info("{}write with pos:{} skipped", logPrefix, req.pos);
      return new Try.Success(new byte[0]);
    } else if (pendingPos > req.pos) {
      byte[] appendVal = req.value;
      int sourcePos = (int) (pendingPos - req.pos);
      int writeAmount = req.value.length - sourcePos;
      appendVal = new byte[writeAmount];
      System.arraycopy(req.value, sourcePos, appendVal, 0, writeAmount);
      logger.info("{}convert write pos from:{} to:{} write amount from:{} to:{}",
        new Object[]{logPrefix, req.pos, pendingPos, req.value.length, writeAmount});
      return new Try.Success(appendVal);
    } else if (pendingPos == req.pos) {
      return new Try.Success(req.value);
    } else {
      String msg = "Storage can only append"
        + " - writePos:" + pendingPos
        + " - reqPos:" + req.pos;
      return new Try.Failure(new DelaStorageException(msg, file.storageType()));
    }
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
      if (readSession.isPresent()) {
        try {
          readSession.get().close();
        } catch (IOException ex) {
          throw new RuntimeException(ex);
        }
      }
    }
  };

  Handler handleWriteComplete = new Handler<StreamStorageOpWrite.Complete>() {
    @Override
    public void handle(StreamStorageOpWrite.Complete event) {
      if (appendSession.isPresent()) {
        try {
          appendSession.get().close();
        } catch (IOException ex) {
          throw new RuntimeException(ex);
        }
      }
    }
  };

  public static class Init extends se.sics.kompics.Init<DelaStorageComp> {

    public final DelaFileHandler storage;
    public final long pos;

    public Init(DelaFileHandler storage, long pos) {
      this.storage = storage;
      this.pos = pos;
    }
  }
}
