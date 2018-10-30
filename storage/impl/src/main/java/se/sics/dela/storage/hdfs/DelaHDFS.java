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
package se.sics.dela.storage.hdfs;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.security.PrivilegedAction;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.UUID;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.logging.Level;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.security.UserGroupInformation;
import org.javatuples.Pair;
import org.slf4j.Logger;
import se.sics.dela.storage.StorageEndpoint;
import se.sics.dela.storage.StorageResource;
import se.sics.dela.storage.common.DelaStorageException;
import se.sics.dela.storage.common.DelaStorageProvider;
import se.sics.dela.storage.core.DelaStorageComp;
import se.sics.dela.util.TimerProxy;
import se.sics.kompics.util.Identifier;
import se.sics.ktoolbox.util.trysf.Try;
import se.sics.ktoolbox.util.trysf.TryHelper;
import se.sics.nstream.util.range.KRange;
import se.sics.dela.storage.common.DelaReadStream;
import se.sics.dela.storage.common.DelaAppendStream;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class DelaHDFS {

  public static final String HOPS_URL = "fs.defaultFS";
  public static final String DATANODE_FAILURE_POLICY = "dfs.client.block.write.replace-datanode-on-failure.policy";

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
      return DelaHDFS.doAs(ugi, action);
    }

    public <I, O> BiFunction<I, Throwable, Try<O>> wrapOp(Supplier<Try<O>> action) {
      return DelaHDFS.doAsOp(ugi, action);
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
      return doAs.perform(DelaHDFS.createPathOp(dfs, endpoint, resource));
    }

    @Override
    public Try<Boolean> fileExists() {
      return doAs.perform(DelaHDFS.fileExistsOp(dfs, endpoint, resource));
    }

    @Override
    public Try createFile() {
      return doAs.perform(DelaHDFS.createFileOp(dfs, endpoint, resource));
    }

    @Override
    public Try deleteFile() {
      return doAs.perform(DelaHDFS.deleteFileOp(dfs, endpoint, resource));
    }

    @Override
    public Try<Long> fileSize() {
      return doAs.perform(DelaHDFS.fileSizeOp(dfs, endpoint, resource));
    }

    @Override
    public Try<byte[]> read(KRange range) {
      return doAs.perform(DelaHDFS.readOp(dfs, endpoint, resource, range));
    }

    @Override
    public Try<byte[]> readAllFile() {
      return doAs.perform(DelaHDFS.readFullyOp(dfs, endpoint, resource));
    }

    @Override
    public Try append(long pos, byte[] data) {
      return doAs.perform(DelaHDFS.writeOp(dfs, endpoint, resource, pos, data));
    }

    @Override
    public Try<DelaReadStream> readSession(TimerProxy timer) {
      try {
        String filePath = resource.dirPath + Path.SEPARATOR + resource.fileName;
        FSDataInputStream in = dfs.open(new Path(filePath));
        Try<Long> size = DelaHDFS.fileSize(dfs, endpoint, resource);
        if (!size.isSuccess()) {
          return (Try.Failure) size;
        }
        return new Try.Success(new ReadSession(dfs, in, doAs));
      } catch (IOException ex) {
        return new Try.Failure(ex);
      }
    }

    @Override
    public Try<DelaAppendStream> appendSession(TimerProxy timer) {
      try {
        String filePath = resource.dirPath + Path.SEPARATOR + resource.fileName;
        FSDataOutputStream out = dfs.append(new Path(filePath));
        Try<Long> size = DelaHDFS.fileSize(dfs, filePath);
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

  public static class AppendSession implements DelaAppendStream {

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
    public void write(long pos, byte[] data, Consumer<Try<Boolean>> callback) {
      if(pendingPos != pos) {
        String msg = "HDFS supports append only - pending pos:" + pendingPos + " write pos:" + pos;
        callback.accept(new Try.Failure(new DelaStorageException(msg)));
      }
      Try<Boolean> r = doAs.perform(DelaHDFS.appendOp(out, data));
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
      Try<Long> size = doAs.perform(DelaHDFS.fileSizeOp(dfs, endpoint, resource));
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

  public static class ReadSession implements DelaReadStream {

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
      Try<byte[]> val = doAs.perform(DelaHDFS.readOp(in, range));
      callback.accept(val);
    }

    @Override
    public Try<Boolean> close() {
      try {
        in.close();
        return new Try.Success(true);
      } catch (IOException ex) {
        String msg = "closing file";
        return new Try.Failure(new DelaStorageException(msg, ex));
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
  
  public static Try<Boolean> canConnect(Configuration hdfsConfig) {
    try {
      FileSystem fs = FileSystem.get(hdfsConfig);
      FsStatus status = fs.getStatus();
      return new Try.Success(true);
    } catch (IOException ex) {
      return new Try.Failure(ex);
    }
  }

  public static <I, O> Try<O> doAs(UserGroupInformation ugi, Supplier<Try<O>> action) {
    return ugi.doAs((PrivilegedAction<Try<O>>) () -> action.get());
  }

  public static <I, O> BiFunction<I, Throwable, Try<O>> doAsOp(UserGroupInformation ugi, Supplier<Try<O>> action) {
    return TryHelper.tryFSucc0(() -> {
      return ugi.doAs((PrivilegedAction<Try<O>>) () -> action.get());
    });
  }

  public static Try<Configuration> fixConfig(Configuration config) {
    try {
      DistributedFileSystem dfs = (DistributedFileSystem) FileSystem.get(config);
      if (dfs.getDataNodeStats().length == 1) {
        dfs.close();
        config.set(DATANODE_FAILURE_POLICY, "NEVER");
      }
      return new Try.Success(config);
    } catch (IOException ex) {
      String msg = "could not contact filesystem";
      return new Try.Failure(new DelaStorageException(msg, ex));
    }
  }

  public static Try<Boolean> createPath(DistributedFileSystem dfs, HDFSEndpoint endpoint, HDFSResource resource) {
    String filePath = resource.dirPath + Path.SEPARATOR + resource.fileName;
    try {
      if (!dfs.isDirectory(new Path(resource.dirPath))) {
        dfs.mkdirs(new Path(resource.dirPath));
        return new Try.Success(true);
      }
      return new Try.Success(false);
    } catch (IOException ex) {
      String msg = "dir op - could not create path:" + filePath;
      return new Try.Failure(new DelaStorageException(msg, ex));
    }
  }

  public static Supplier<Try<Boolean>> createPathOp(DistributedFileSystem dfs,
    HDFSEndpoint endpoint, HDFSResource resource) {
    return () -> createPath(dfs, endpoint, resource);
  }

  public static Try<Boolean> fileExists(DistributedFileSystem dfs, HDFSEndpoint endpoint, HDFSResource resource) {
    Path filePath = new Path(resource.dirPath + Path.SEPARATOR + resource.fileName);
    try {
      if (dfs.exists(filePath)) {
        if (dfs.isFile(filePath)) {
          return new Try.Success(true);
        } else {
          return new Try.Failure(new DelaStorageException("path exists, and it is not a file"));
        }
      } else {
        return new Try.Success(false);
      }
    } catch (IOException ex) {
      String msg = "dir op - could not check exists:" + filePath;
      return new Try.Failure(new DelaStorageException(msg, ex));
    }
  }

  public static Supplier<Try<Boolean>> fileExistsOp(DistributedFileSystem dfs,
    HDFSEndpoint endpoint, HDFSResource resource) {
    return () -> fileExists(dfs, endpoint, resource);
  }

  public static Try<Boolean> createFile(DistributedFileSystem dfs, HDFSEndpoint endpoint, HDFSResource resource) {
    String filePath = resource.dirPath + Path.SEPARATOR + resource.fileName;
    try {
      if (dfs.isFile(new Path(filePath))) {
        return new Try.Success(false);
      }
      try (FSDataOutputStream out = dfs.create(new Path(filePath))) {
        return new Try.Success(true);
      }
    } catch (IOException ex) {
      String msg = "dir op - could not create:" + filePath;
      return new Try.Failure(new DelaStorageException(msg, ex));
    }
  }

  public static Supplier<Try<Boolean>> createFileOp(DistributedFileSystem dfs,
    HDFSEndpoint endpoint, HDFSResource resource) {
    return () -> createFile(dfs, endpoint, resource);
  }

  public static Supplier<Try<Boolean>> deleteFileOp(DistributedFileSystem dfs,
    HDFSEndpoint endpoint, HDFSResource resource) {
    return () -> deleteFile(dfs, endpoint, resource);
  }

  public static Try<Boolean> deleteFile(DistributedFileSystem dfs, HDFSEndpoint endpoint, HDFSResource resource) {
    String filePath = resource.dirPath + Path.SEPARATOR + resource.fileName;
    try {
      if (!dfs.exists(new Path(filePath))) {
        return new Try.Success(false);
      }
      dfs.delete(new Path(filePath), true);
      return new Try.Success(true);
    } catch (IOException ex) {
      String msg = "dir op- could not delete:" + filePath;
      return new Try.Failure(new DelaStorageException(msg, ex));
    }
  }

  public static Supplier<Try<Long>> fileSizeOp(DistributedFileSystem dfs,
    HDFSEndpoint endpoint, HDFSResource resource) {
    return () -> fileSize(dfs, endpoint, resource);
  }

  public static Try<Long> fileSize(DistributedFileSystem dfs, HDFSEndpoint endpoint, HDFSResource resource) {
    final String filePath = resource.dirPath + Path.SEPARATOR + resource.fileName;
    return fileSize(dfs, filePath);
  }

  public static Try<Long> fileSize(DistributedFileSystem dfs, String filePath) {
    try {
      FileStatus status = dfs.getFileStatus(new Path(filePath));
      if (status.isFile()) {
        return new Try.Success(status.getLen());
      } else {
        return new Try.Success(-1l);
      }
    } catch (FileNotFoundException ex) {
      return new Try.Success(-1l);
    } catch (IOException ex) {
      String msg = "file meta - could not get size of file:" + filePath;
      return new Try.Failure(new DelaStorageException(msg, ex));
    }
  }

  public static Try<Boolean> append(DistributedFileSystem dfs,
    HDFSEndpoint endpoint, HDFSResource resource, byte[] data) {
    String filePath = resource.dirPath + Path.SEPARATOR + resource.fileName;
    try (FSDataOutputStream out = dfs.append(new Path(filePath))) {
      return append(out, data);
    } catch (IOException ex) {
      String msg = "file op - could not append to file:" + filePath;
      return new Try.Failure(new DelaStorageException(msg, ex));
    }
  }

  public static Supplier<Try<Boolean>> appendOp(DistributedFileSystem dfs,
    HDFSEndpoint endpoint, HDFSResource resource, byte[] data) {
    return () -> append(dfs, endpoint, resource, data);
  }
  
  public static Supplier<Try<Boolean>> writeOp(DistributedFileSystem dfs,
    HDFSEndpoint endpoint, HDFSResource resource, long pos, byte[] data) {
    return () -> {
      return new Try.Success(true)
        .flatMap(TryHelper.tryFSucc0(DelaHDFS.fileSizeOp(dfs, endpoint, resource)))
        .flatMap(TryHelper.tryFSucc1((Long fileSize) -> {
          if(fileSize != pos) {
            String msg = "HDFS supports append only - pending pos:" + fileSize + " write pos:" + pos;
            return new Try.Failure(new DelaStorageException(msg));
          } else {
            return new Try.Success(true);
          }
        }))
        .flatMap(TryHelper.tryFSucc0(appendOp(dfs, endpoint, resource, data)));
    };
  }

  public static Try<Boolean> append(FSDataOutputStream out, byte[] data) {
    try {
      out.write(data);
      return new Try.Success(true);
    } catch (IOException ex) {
      String msg = "file op - could not append";
      return new Try.Failure(new DelaStorageException(msg, ex));
    }
  }

  public static Supplier<Try<Boolean>> appendOp(FSDataOutputStream out, byte[] data) {
    return () -> append(out, data);
  }

  public static Supplier<Try<byte[]>> readOp(DistributedFileSystem dfs,
    HDFSEndpoint endpoint, HDFSResource resource, KRange range) {
    return () -> read(dfs, endpoint, resource, range);
  }

  public static Try<byte[]> read(DistributedFileSystem dfs,
    HDFSEndpoint endpoint, HDFSResource resource, KRange range) {
    String filePath = resource.dirPath + Path.SEPARATOR + resource.fileName;
    try (FSDataInputStream in = dfs.open(new Path(filePath))) {
      return read(in, range);
    } catch (IOException ex) {
      String msg = "file op - could not read file:" + filePath;
      return new Try.Failure(new DelaStorageException(msg, ex));
    }
  }

  public static Try<byte[]> read(FSDataInputStream in, KRange range) {
    try {
      int readLength = (int) (range.upperAbsEndpoint() - range.lowerAbsEndpoint() + 1);
      byte[] byte_read = new byte[readLength];
      in.readFully(range.lowerAbsEndpoint(), byte_read);
      return new Try.Success(byte_read);
    } catch (IOException ex) {
      String msg = "file op - could not read";
      return new Try.Failure(new DelaStorageException(msg, ex));
    }
  }

  public static Supplier<Try<byte[]>> readOp(FSDataInputStream in, KRange range) {
    return () -> read(in, range);
  }

  public static Try<byte[]> readFully(DistributedFileSystem dfs, HDFSEndpoint endpoint, HDFSResource resource) {
    String filePath = resource.dirPath + Path.SEPARATOR + resource.fileName;
    try (FSDataInputStream in = dfs.open(new Path(filePath))) {
      FileStatus status = dfs.getFileStatus(new Path(filePath));
      byte[] allBytes = new byte[(int) status.getLen()];
      in.readFully(allBytes);
      return new Try.Success(allBytes);
    } catch (IOException ex) {
      String msg = "file op - could not read file:" + filePath;
      return new Try.Failure(new DelaStorageException(msg, ex));
    }
  }

  public static Supplier<Try<byte[]>> readFullyOp(DistributedFileSystem dfs,
    HDFSEndpoint endpoint, HDFSResource resource) {
    return () -> readFully(dfs, endpoint, resource);
  }

  public static Try<Boolean> flush(DistributedFileSystem dfs, HDFSEndpoint endpoint, HDFSResource resource) {
    String filePath = resource.dirPath + Path.SEPARATOR + resource.fileName;
    try (FSDataOutputStream out = dfs.append(new Path(filePath))) {
      out.hflush();
      return new Try.Success(true);
    } catch (IOException ex) {
      String msg = "file op - could not flush to file:{}" + filePath;
      return new Try.Failure(new DelaStorageException(msg, ex));
    }
  }

  public static Try<Long> blockSize(DistributedFileSystem dfs, HDFSEndpoint endpoint, HDFSResource resource) {
    String filePath = resource.dirPath + Path.SEPARATOR + resource.fileName;
    try {
      FileStatus status = dfs.getFileStatus(new Path(filePath));
      long hdfsBlockSize = dfs.getDefaultBlockSize();
      if (status.isFile()) {
        hdfsBlockSize = status.getBlockSize();
      }
      return new Try.Success(hdfsBlockSize);
    } catch (IOException ex) {
      String msg = "file meta - could not get block size for file" + filePath;
      return new Try.Failure(new DelaStorageException(msg, ex));
    }
  }

  public static Supplier<Try<Long>> blockSizeOp(DistributedFileSystem dfs,
    HDFSEndpoint endpoint, HDFSResource resource) {
    return () -> blockSize(dfs, endpoint, resource);
  }
}
