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
package se.sics.dela.storage.hdfs;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.UUID;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.security.UserGroupInformation;
import org.javatuples.Pair;
import org.slf4j.Logger;
import se.sics.dela.storage.StorageEndpoint;
import se.sics.dela.storage.StorageResource;
import se.sics.dela.storage.common.DelaAppendSession;
import se.sics.dela.storage.common.DelaReadSession;
import se.sics.dela.storage.core.DelaStorageComp;
import se.sics.dela.storage.common.DelaStorageException;
import se.sics.dela.storage.common.DelaStorageProvider;
import se.sics.dela.storage.hdfs.HDFSEndpoint;
import se.sics.dela.storage.hdfs.HDFSHelper;
import se.sics.dela.storage.hdfs.HDFSResource;
import static se.sics.dela.storage.hdfs.HDFSHelper.doAs;
import static se.sics.dela.storage.hdfs.HDFSHelper.doAsOp;
import se.sics.dela.util.TimerProxy;
import se.sics.kompics.util.Identifier;
import se.sics.ktoolbox.util.trysf.Try;
import se.sics.nstream.util.range.KRange;

/**
 *
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class HDFS {
  public static class StorageCompProvider implements se.sics.dela.storage.mngr.StorageProvider<DelaStorageComp> {

    public final Identifier self;
    public final HDFSEndpoint endpoint;

    public StorageCompProvider(Identifier self, HDFSEndpoint endpoint) {
      this.self = self;
      this.endpoint = endpoint;
    }

    @Override
    public Pair<DelaStorageComp.Init, Long> initiate(StorageResource resource, Logger logger) {
      HDFSResource hdfsResource = (HDFSResource) resource;
      DistributedFileSystem dfs;
      try {
        dfs = (DistributedFileSystem) FileSystem.get(endpoint.hdfsConfig);
      } catch (IOException ex) {
        throw new RuntimeException(ex);
      }
      StorageProvider storage = new StorageProvider(endpoint, hdfsResource, dfs);
      long streamPos = fileInit(storage);

      DelaStorageComp.Init init = new DelaStorageComp.Init(storage, streamPos);
      return Pair.with(init, streamPos);
    }

    private long fileInit(StorageProvider storage) {
      Try<Long> streamPos = storage.fileSize();
      if (!streamPos.isSuccess()) {
        try {
          ((Try.Failure) streamPos).checkedGet();
        } catch (Throwable t) {
          throw new RuntimeException(t);
        }
      }
      if (streamPos.get() == -1) {
        storage.createFile();
        return 0l;
      }
      return streamPos.get();
    }

    @Override
    public String getName() {
      return endpoint.getEndpointName();
    }

    @Override
    public Class<DelaStorageComp> getStorageDefinition() {
      return DelaStorageComp.class;
    }

    @Override
    public StorageEndpoint getEndpoint() {
      return endpoint;
    }
  }

  public static class DoAs {

    private final UserGroupInformation ugi;

    public DoAs(UserGroupInformation ugi) {
      this.ugi = ugi;
    }

    public <O> Try<O> perform(Supplier<Try<O>> action) {
      return HDFSHelper.doAs(ugi, action);
    }

    public <I, O> BiFunction<I, Throwable, Try<O>> wrapOp(Supplier<Try<O>> action) {
      return HDFSHelper.doAsOp(ugi, action);
    }
  }

  public static class StorageProvider implements DelaStorageProvider<HDFSEndpoint, HDFSResource> {

    public final HDFSEndpoint endpoint;
    public final HDFSResource resource;
    public final DistributedFileSystem dfs;
    private final DoAs doAs;

    public StorageProvider(HDFSEndpoint endpoint, HDFSResource resource, DistributedFileSystem dfs) {
      this.endpoint = endpoint;
      this.resource = resource;
      this.dfs = dfs;
      this.doAs = doAs(endpoint.user).get();
    }

    @Override
    public HDFSEndpoint getEndpoint() {
      return endpoint;
    }

    @Override
    public HDFSResource getResource() {
      return resource;
    }

    @Override
    public Try createPath() {
      return doAs.perform(HDFSHelper.createPathOp(dfs, endpoint, resource));
    }

    @Override
    public Try<Boolean> fileExists() {
      return doAs.perform(HDFSHelper.fileExistsOp(dfs, endpoint, resource));
    }

    @Override
    public Try createFile() {
      return doAs.perform(HDFSHelper.createFileOp(dfs, endpoint, resource));
    }

    @Override
    public Try deleteFile() {
      return doAs.perform(HDFSHelper.deleteFileOp(dfs, endpoint, resource));
    }

    @Override
    public Try<Long> fileSize() {
      return doAs.perform(HDFSHelper.fileSizeOp(dfs, endpoint, resource));
    }

    @Override
    public Try<byte[]> read(KRange range) {
      return doAs.perform(HDFSHelper.readOp(dfs, endpoint, resource, range));
    }

    @Override
    public Try<byte[]> readAllFile() {
      return doAs.perform(HDFSHelper.readFullyOp(dfs, endpoint, resource));
    }

    @Override
    public Try append(byte[] data) {
      return doAs.perform(HDFSHelper.appendOp(dfs, endpoint, resource, data));
    }

    @Override
    public Try<DelaReadSession> readSession(TimerProxy timer) {
      try {
        String filePath = resource.dirPath + Path.SEPARATOR + resource.fileName;
        FSDataInputStream in = dfs.open(new Path(filePath));
        Try<Long> size = HDFSHelper.fileSize(dfs, endpoint, resource);
        if (!size.isSuccess()) {
          return (Try.Failure) size;
        }
        return new Try.Success(new ReadSession(dfs, in, doAs));
      } catch (IOException ex) {
        return new Try.Failure(ex);
      }
    }

    @Override
    public Try<DelaAppendSession> appendSession(TimerProxy timer) {
      try {
        String filePath = resource.dirPath + Path.SEPARATOR + resource.fileName;
        FSDataOutputStream out = dfs.append(new Path(filePath));
        Try<Long> size = HDFSHelper.fileSize(dfs, filePath);
        if (!size.isSuccess()) {
          return (Try.Failure) size;
        }
        AppendSession session = new AppendSession(endpoint, resource, dfs, out, doAs, size.get());
        session.setup(timer);
        return new Try.Success(session);
      } catch (IOException ex) {
        return new Try.Failure(ex);
      }
    }
  }

  public static class AppendSession implements DelaAppendSession {

    public static final Integer FLUSH_COUNTER_MB = 50;
    public static final Long FLUSH_PERIOD = 1000l;
    private final HDFSEndpoint endpoint;
    private final HDFSResource resource;

    private final DistributedFileSystem dfs;
    private final FSDataOutputStream out;
    private final DoAs doAs;

    private long confirmedPos;
    private long pendingPos;
    private final Map<Pair<Long, Long>, Consumer<Try<Boolean>>> pending = new HashMap<>();
    private TimerProxy timer;
    private UUID flushTimer;

    public AppendSession(HDFSEndpoint endpoint, HDFSResource resource,
      DistributedFileSystem dfs, FSDataOutputStream out, DoAs doAs, long pos) {
      this.endpoint = endpoint;
      this.resource = resource;
      this.dfs = dfs;
      this.out = out;
      this.doAs = doAs;
      this.confirmedPos = pos;
    }

    private void setup(TimerProxy timer) {
      this.timer = timer;
      flushTimer = timer.schedulePeriodicTimer(FLUSH_PERIOD, FLUSH_PERIOD, timeout());
    }

    @Override
    public void append(byte[] data, Consumer<Try<Boolean>> callback) {
      Try<Boolean> r = doAs.perform(HDFSHelper.appendOp(out, data));
      if (r.isSuccess()) {
        pending.put(Pair.with(pendingPos, pendingPos + data.length), callback);
        pendingPos += data.length;
        if (flushCounter()) {
          checkPendingAppends();
        }
      } else {
        callback.accept((Try.Failure) r);
      }
    }

    private Consumer<Boolean> timeout() {
      return (Boolean _in) -> {
        if (!pending.isEmpty()) {
          checkPendingAppends();
        }
      };
    }

    private boolean flushCounter() {
      return (pendingPos - confirmedPos) > FLUSH_COUNTER_MB * 1024 * 1024;
    }

    private void checkPendingAppends() {
      Try<Long> size = doAs.perform(HDFSHelper.fileSizeOp(dfs, endpoint, resource));
      if (!size.isSuccess()) {
        pending.values().forEach((callback) -> callback.accept((Try.Failure) size));
      }
      Iterator<Map.Entry<Pair<Long, Long>, Consumer<Try<Boolean>>>> it = pending.entrySet().iterator();
      while (it.hasNext()) {
        Map.Entry<Pair<Long, Long>, Consumer<Try<Boolean>>> next = it.next();
        long startWrite = next.getKey().getValue0();
        long endWrite = next.getKey().getValue1();
        Consumer<Try<Boolean>> callback = next.getValue();
        if (startWrite <= size.get() && size.get() <= endWrite) {
          callback.accept(new Try.Success(true));
          it.remove();
          confirmedPos = endWrite;
        } else {
          break;
        }
      }
    }

    @Override
    public Try<Boolean> close() {
      try {
        out.close();
        timer.cancelPeriodicTimer(flushTimer);
        flushTimer = null;
        return new Try.Success(true);
      } catch (IOException ex) {
        return new Try.Failure(ex);
      }
    }
  }

  public static class ReadSession implements DelaReadSession {

    private final DistributedFileSystem dfs;
    private final FSDataInputStream in;
    private final DoAs doAs;

    public ReadSession(DistributedFileSystem dfs, FSDataInputStream in, DoAs doAs) {
      this.dfs = dfs;
      this.in = in;
      this.doAs = doAs;
    }

    @Override
    public void read(KRange range, Consumer<Try<byte[]>> callback) {
      Try<byte[]> val = doAs.perform(HDFSHelper.readOp(in, range));
      callback.accept(val);
    }

    @Override
    public Try<Boolean> close() {
      try {
        in.close();
        return new Try.Success(true);
      } catch (IOException ex) {
        return new Try.Failure(ex);
      }
    }
  }
  
  public static Try<DoAs> doAs(String user) {
    try {
      UserGroupInformation ugi = UserGroupInformation.createProxyUser(user, UserGroupInformation.getLoginUser());
      return new Try.Success(new DoAs(ugi));
    } catch (IOException ex) {
      String msg = "could not create proxy user:" + user;
      return new Try.Failure(new DelaStorageException(msg, ex));
    }
  }
}
