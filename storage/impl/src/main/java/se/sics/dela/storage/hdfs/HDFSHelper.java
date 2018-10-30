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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.security.PrivilegedAction;
import java.util.function.BiFunction;
import java.util.function.Supplier;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.security.UserGroupInformation;
import se.sics.dela.storage.common.DelaStorageException;
import se.sics.ktoolbox.util.trysf.Try;
import se.sics.ktoolbox.util.trysf.TryHelper;
import se.sics.nstream.util.range.KRange;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class HDFSHelper {

  public static final String HOPS_URL = "fs.defaultFS";
  public static final String DATANODE_FAILURE_POLICY = "dfs.client.block.write.replace-datanode-on-failure.policy";

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
