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
package se.sics.dela.storage.disk;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import org.javatuples.Pair;
import org.slf4j.Logger;
import se.sics.dela.storage.operation.StreamOpPort;
import se.sics.dela.storage.operation.events.StorageStreamOpRead;
import se.sics.dela.storage.operation.events.StorageStreamOpWrite;
import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.Handler;
import se.sics.kompics.Negative;
import se.sics.kompics.Positive;
import se.sics.kompics.Start;
import se.sics.kompics.timer.Timer;
import se.sics.kompics.util.Identifier;
import se.sics.ktoolbox.util.result.Result;
import se.sics.dela.storage.StorageEndpoint;
import se.sics.dela.storage.StorageResource;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class DiskComp extends ComponentDefinition {

  private String logPrefix = "";

  Positive<Timer> timerPort = requires(Timer.class);
  private final Negative storagePort = provides(StreamOpPort.class);
  //**************************************************************************
  private final Identifier self;
  //**************************************************************************
  private RandomAccessFile raf;
  private long writePos;

  public DiskComp(Init init) {
    self = init.self;
    logPrefix = "<nid:" + self.toString() + ">disk:" + init.filePath + " ";
    logger.info("{}init", logPrefix);

    raf = init.raf;
    writePos = init.writePos;

    subscribe(handleStart, control);
    subscribe(handleRead, storagePort);
    subscribe(handleWrite, storagePort);
  }

  Handler handleStart = new Handler<Start>() {
    @Override
    public void handle(Start event) {
      logger.info("{}starting", logPrefix);
    }
  };

  @Override
  public void tearDown() {
    logger.info("{}tearing down", logPrefix);
  }

  Handler handleRead = new Handler<StorageStreamOpRead.Request>() {
    @Override
    public void handle(StorageStreamOpRead.Request req) {
      logger.debug("{}read:{}", logPrefix, req);
      int readLength = (int) (req.readRange.upperAbsEndpoint() - req.readRange.lowerAbsEndpoint() + 1);
      byte[] readVal = new byte[readLength];
      int readPos = (int) req.readRange.lowerAbsEndpoint();
      try {
        logger.debug("{}reading at pos:{} amount:{}", new Object[]{logPrefix, readPos, readLength});
        raf.seek(readPos);
        raf.readFully(readVal);
      } catch (IOException ex) {
        answer(req, req.respond(Result.internalFailure(ex)));
      }
      answer(req, req.respond(Result.success(readVal)));
    }
  };

  Handler handleWrite = new Handler<StorageStreamOpWrite.Request>() {
    @Override
    public void handle(StorageStreamOpWrite.Request req) {
      logger.debug("{}write:{}", logPrefix, req);
      if (writePos >= req.pos + req.value.length) {
        logger.debug("{}write with pos:{} skipped", logPrefix, req.pos);
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
        logger.debug("{}convert write pos from:{} to:{} write amount from:{} to:{}",
          new Object[]{logPrefix, req.pos, pos, req.value.length, writeAmount});
      }
      try {
        raf.seek(req.pos);
        raf.write(req.value);
      } catch (IOException ex) {
        answer(req, req.respond(Result.internalFailure(ex)));
      }
      writePos += writeValue.length;
      answer(req, req.respond(Result.success(true)));
    }
  };

  public static class Init extends se.sics.kompics.Init<DiskComp> {

    public final Identifier self;
    public final String filePath;
    public final RandomAccessFile raf;
    public final long writePos;

    public Init(Identifier self, String filePath, RandomAccessFile raf, long writePos) {
      this.self = self;
      this.filePath = filePath;
      this.raf = raf;
      this.writePos = writePos;
    }
  }

  public static class StorageProvider implements se.sics.dela.storage.mngr.StorageProvider<DiskComp> {

    public final Identifier self;
    public final DiskEndpoint endpoint = new DiskEndpoint();

    public StorageProvider(Identifier self) {
      this.self = self;
    }

    @Override
    public Pair<Init, Long> initiate(StorageResource resource, Logger logger) {
      DiskResource diskResource = (DiskResource) resource;
      String filePath = diskResource.dirPath + File.separator + diskResource.fileName;
      RandomAccessFile raf;
      long filePos;
      try {
        raf = new RandomAccessFile(filePath, "rw");
        filePos = raf.length();
      } catch (FileNotFoundException ex) {
        throw new RuntimeException(ex);
      } catch (IOException ex) {
        throw new RuntimeException(ex);
      }

      Init init = new Init(self, filePath, raf, filePos);
      return Pair.with(init, filePos);
    }

    @Override
    public String getName() {
      return endpoint.getEndpointName();
    }

    @Override
    public Class<DiskComp> getStorageDefinition() {
      return DiskComp.class;
    }

    @Override
    public StorageEndpoint getEndpoint() {
      return endpoint;
    }
  }
}