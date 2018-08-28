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
package se.sics.nstream.gcp;

import se.sics.nstream.hops.storage.gcp.GCPHelper;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.ReadChannel;
import com.google.cloud.WriteChannel;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Storage;
import java.io.IOException;
import org.javatuples.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.Handler;
import se.sics.kompics.Negative;
import se.sics.kompics.Positive;
import se.sics.kompics.Start;
import se.sics.kompics.timer.Timer;
import se.sics.kompics.util.Identifier;
import se.sics.ktoolbox.util.result.Result;
import se.sics.ktoolbox.util.trysf.Try;
import se.sics.nstream.hops.storage.gcp.GCPEndpoint;
import se.sics.nstream.hops.storage.gcp.GCPResource;
import se.sics.nstream.storage.durable.DStoragePort;
import se.sics.nstream.storage.durable.DurableStorageProvider;
import se.sics.nstream.storage.durable.events.DStorageRead;
import se.sics.nstream.storage.durable.events.DStorageWrite;
import se.sics.nstream.storage.durable.util.StreamEndpoint;
import se.sics.nstream.storage.durable.util.StreamResource;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class GCPComp extends ComponentDefinition {

  private final static Logger LOG = LoggerFactory.getLogger(GCPComp.class);
  private String logPrefix = "";

  Positive<Timer> timerPort = requires(Timer.class);
  private final Negative storagePort = provides(DStoragePort.class);
  //**************************************************************************
  private final Identifier self;
  private final String projectName;
  private final BlobId blobId;
  private final GoogleCredentials credentials;
  private final WriteChannel writer;
  private ReadChannel reader;
  //object are immutable in GCP - you cannot append once the writechannel is closed - always start from 0
  private long writePos = 0;

  public GCPComp(Init init) {
    self = init.self;
    projectName = init.projectName;
    blobId = init.blobId;
    credentials = init.credentials;
    writer = GCPHelper.writeChannel(credentials, projectName, blobId);
    reader = GCPHelper.readChannel(credentials, projectName, blobId);

    logPrefix = "<nid:" + self.toString() + ">gcp:" + projectName + "/" + init.blobId + " ";
    LOG.info("{}init", logPrefix);

    subscribe(handleStart, control);
    subscribe(handleRead, storagePort);
    subscribe(handleReadComplete, storagePort);
    subscribe(handleWrite, storagePort);
    subscribe(handleWriteComplete, storagePort);
  }

  Handler handleStart = new Handler<Start>() {
    @Override
    public void handle(Start event) {
      LOG.info("{}starting", logPrefix);
    }
  };

  @Override
  public void tearDown() {
    LOG.info("{}tearing down", logPrefix);
    if (reader.isOpen()) {
      reader.close();
    }
    if (writer.isOpen()) {
      try {
        writer.close();
      } catch (IOException ex) {
        throw new RuntimeException(ex);
      }
    }
  }

  Handler handleRead = new Handler<DStorageRead.Request>() {
    @Override
    public void handle(DStorageRead.Request req) {
      if (!reader.isOpen()) {
        reader = GCPHelper.readChannel(credentials, projectName, blobId);
      }
      LOG.debug("{}read:{}", logPrefix, req);
      int readLength = (int) (req.readRange.upperAbsEndpoint() - req.readRange.lowerAbsEndpoint() + 1);
      int readPos = (int) req.readRange.lowerAbsEndpoint();
      LOG.debug("{}reading at pos:{} amount:{}", new Object[]{logPrefix, readPos, readLength});
      Try<byte[]> read = new Try.Success(true)
        .flatMap(GCPHelper.readFromBlob(reader, readPos, readLength));
      try {
        answer(req, req.respond(Result.success(read.checkedGet())));
      } catch (Throwable t) {
        answer(req, req.respond(Result.internalFailure((Exception) t)));
      }
    }
  };

  Handler handleReadComplete = new Handler<DStorageRead.Complete>() {
    @Override
    public void handle(DStorageRead.Complete event) {
      if (reader.isOpen()) {
        reader.close();
      }
    }
  };

  Handler handleWrite = new Handler<DStorageWrite.Request>() {
    @Override
    public void handle(DStorageWrite.Request req) {
      LOG.debug("{}write:{}", logPrefix, req);
      Try<Integer> write = skipExistingBytes(req)
        .flatMap(GCPHelper.writeToBlob(writer));
      try {
        long fromWritePos = writePos;
        writePos += write.checkedGet();
        LOG.debug("{}write from:{} to:{}", new Object[]{logPrefix, fromWritePos, writePos});
        answer(req, req.respond(Result.success(true)));
      } catch (Throwable t) {
        answer(req, req.respond(Result.internalFailure((Exception) t)));
      }
    }
  };

  private Try<byte[]> skipExistingBytes(DStorageWrite.Request req) {
    if (writePos >= req.pos + req.value.length) {
      LOG.debug("{}write with pos:{} skipped", logPrefix, req.pos);
      answer(req, req.respond(Result.success(true)));
      return new Try.Success(new byte[0]);
    } else if (writePos > req.pos) {
      long pos = writePos;
      int sourcePos = (int) (pos - req.pos);
      int writeAmount = req.value.length - sourcePos;
      byte[] writeValue = new byte[writeAmount];
      System.arraycopy(req.value, sourcePos, writeValue, 0, writeAmount);
      LOG.debug("{}convert write pos from:{} to:{} write amount from:{} to:{}",
        new Object[]{logPrefix, req.pos, pos, req.value.length, writeAmount});
      return new Try.Success(writeValue);
    } else if (writePos == req.pos) {
      return new Try.Success(req.value);
    } else {
      String cause = "GCPComp can only append"
        + " - writePos:" + writePos
        + " - reqPos:" + req.pos;
      return new Try.Failure(new IllegalArgumentException(cause));
    }
  }

  Handler handleWriteComplete = new Handler<DStorageWrite.Complete>() {
    @Override
    public void handle(DStorageWrite.Complete event) {
      if (writer.isOpen()) {
        try {
          writer.close();
        } catch (IOException ex) {
          throw new RuntimeException(ex);
        }
      }
    }
  };

  public static class Init extends se.sics.kompics.Init<GCPComp> {

    public final Identifier self;
    public final String projectName;
    public final BlobId blobId;
    public final GoogleCredentials credentials;

    public Init(Identifier self, String projectName, BlobId blobId, GoogleCredentials credentials) {
      this.self = self;
      this.projectName = projectName;
      this.blobId = blobId;
      this.credentials = credentials;
    }
  }

  public static class StorageProvider implements DurableStorageProvider<GCPComp> {

    public final Identifier self;
    public final GCPEndpoint endpoint;

    public StorageProvider(Identifier self, GCPEndpoint endpoint) {
      this.self = self;
      this.endpoint = endpoint;
    }

    @Override
    public Pair<GCPComp.Init, Long> initiate(StreamResource resource) {
      GCPResource gcpResource = (GCPResource) resource;
      BlobId blobId = gcpResource.getBlobId();
      Blob blob = checkCreateBlob(blobId);
      GCPComp.Init init = new GCPComp.Init(self, endpoint.projectName, blobId, endpoint.credentials);
      //object are immutable in GCP - you cannot append once the writechannel is closed - always start from 0
      long filePos = 0;
      return Pair.with(init, filePos);
    }

    private Blob checkCreateBlob(BlobId blobId) {
      Storage storage = GCPHelper.getStorage(endpoint.credentials, endpoint.projectName);
      Try<Blob> blob = new Try.Success(storage)
        .flatMap(GCPHelper.getBlob(blobId))
        .recoverWith(GCPHelper.rCreateBlob(blobId));
      try {
        return blob.checkedGet();
      } catch (Throwable ex) {
        throw new RuntimeException(ex);
      }
    }

    @Override
    public String getName() {
      return endpoint.getEndpointName();
    }

    @Override
    public Class<GCPComp> getStorageDefinition() {
      return GCPComp.class;
    }

    @Override
    public StreamEndpoint getEndpoint() {
      return endpoint;
    }
  }
}
