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
package se.sics.dela.storage.common;

import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import se.sics.dela.storage.operation.StreamStorageOpPort;
import se.sics.dela.storage.operation.events.StreamStorageOpRead;
import se.sics.dela.storage.operation.events.StreamStorageOpWrite;
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

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class DelaStorageComp extends ComponentDefinition {

  private String logPrefix = "";

  Positive<Timer> timerPort = requires(Timer.class);
  Negative<StreamStorageOpPort> resourcePort = provides(StreamStorageOpPort.class);
  One2NChannel resourceChannel;

  private Map<Identifier, Component> components = new HashMap<>();
  private final DelaStorageProvider storage;
  private long writePos;
  private final TreeMap<Long, StreamStorageOpWrite.Request> pending = new TreeMap<>();

  public DelaStorageComp(Init init) {
    logger.info("{}init", logPrefix);

    storage = init.storage;
    writePos = init.pos;

    subscribe(handleStart, control);
    subscribe(handleReadRequest, resourcePort);
    subscribe(handleWriteRequest, resourcePort);
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
      Try<byte[]> readResult = storage.read(req.readRange);
      StreamStorageOpRead.Response resp = req.respond(readResult);
      logger.trace("{}answering:{}", logPrefix, resp);
      answer(req, resp);
    }
  };

  Handler handleWriteRequest = new Handler<StreamStorageOpWrite.Request>() {
    @Override
    public void handle(StreamStorageOpWrite.Request req) {
      logger.info("{}write:{}", logPrefix, req);
      if (writePos >= req.pos + req.value.length) {
        logger.info("{}write with pos:{} skipped", logPrefix, req.pos);
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
        logger.info("{}convert write pos from:{} to:{} write amount from:{} to:{}",
          new Object[]{logPrefix, req.pos, pos, req.value.length, writeAmount});
      } else {
        writeValue = req.value;
      }

      Try<Boolean> writeResult = storage.append(writeValue);
      if (!writeResult.isSuccess()) {
        StreamStorageOpWrite.Response resp = req.respond(writeResult);
        answer(req, resp);
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
