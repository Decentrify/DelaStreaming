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
package se.sics.nstream.hops.storage.hdfs;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.security.PrivilegedAction;
import java.security.PrivilegedExceptionAction;
import java.util.Random;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.ktoolbox.util.result.Result;
import se.sics.ktoolbox.util.trysf.Try;
import se.sics.ktoolbox.util.trysf.TryHelper;
import se.sics.nstream.hops.storage.hops.ManifestHelper;
import se.sics.nstream.hops.storage.hops.ManifestJSON;
import se.sics.nstream.util.range.KRange;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class HDFSHelper {

  private final static Logger LOG = LoggerFactory.getLogger(HDFSHelper.class);
  private static String logPrefix = "";

  public static final String HOPS_URL = "fs.defaultFS";
  public static final String DATANODE_FAILURE_POLICY = "dfs.client.block.write.replace-datanode-on-failure.policy";

  public static <O> Try<O> doAs(UserGroupInformation ugi, Supplier<Try<O>> action) {
    return ugi.doAs((PrivilegedAction<Try<O>>) () -> action.get());
  }

  public static Try<Configuration> fixConfig(Configuration config) {
    try (DistributedFileSystem fs = (DistributedFileSystem) FileSystem.newInstance(config)) {
      if (fs.getDataNodeStats().length == 1) {
        fs.close();
        config.set(DATANODE_FAILURE_POLICY, "NEVER");
      }
      return new Try.Success(config);
    } catch (IOException ex) {
      String msg = "could not contact filesystem";
      return new Try.Failure(new HDFSException(msg, ex));
    }
  }

  public static Result<Boolean> canConnect(final Configuration hdfsConfig) {
    LOG.debug("{}testing hdfs connection", logPrefix);
    try (FileSystem fs = FileSystem.newInstance(hdfsConfig)) {
      LOG.debug("{}getting status", logPrefix);
      FsStatus status = fs.getStatus();
      LOG.debug("{}got status", logPrefix);
      return Result.success(true);
    } catch (IOException ex) {
      LOG.info("{}could not connect:{}", logPrefix, ex.getMessage());
      return Result.success(false);
    }
  }

  public static Try<Long> length(DistributedFileSystem fs, UserGroupInformation ugi,
    HDFSEndpoint endpoint, HDFSResource resource) {
    return doAs(ugi, length(fs, endpoint, resource));
  }

  public static Supplier<Try<Long>> length(DistributedFileSystem fs,
    HDFSEndpoint endpoint, HDFSResource resource) {
    return () -> {
      String filePath = resource.dirPath + Path.SEPARATOR + resource.fileName;
      try {
        FileStatus status = fs.getFileStatus(new Path(filePath));
        if (status.isFile()) {
          return new Try.Success(status.getLen());
        } else {
          return new Try.Success(-1l);
        }
      } catch (FileNotFoundException ex) {
        return new Try.Success(-1l);
      } catch (IOException ex) {
        LOG.warn("{}could not get size of file:{}", logPrefix, ex.getMessage());
        return new Try.Failure(new HDFSException("hdfs file length", ex));
      }
    };
  }

  public static Result<Long> length(UserGroupInformation ugi, final HDFSEndpoint endpoint, HDFSResource resource) {
    final String filePath = resource.dirPath + Path.SEPARATOR + resource.fileName;
    LOG.debug("{}getting length of file:{}", new Object[]{logPrefix, filePath});

    try {
      Result<Long> result = ugi.doAs(new PrivilegedExceptionAction<Result<Long>>() {
        @Override
        public Result<Long> run() {
          long length = -1;
          try (DistributedFileSystem fs = (DistributedFileSystem) FileSystem.newInstance(endpoint.hdfsConfig)) {
            FileStatus status = fs.getFileStatus(new Path(filePath));
            if (status.isFile()) {
              length = status.getLen();
            }
            return Result.success(length);
          } catch (FileNotFoundException ex) {
            return Result.success(length);
          } catch (IOException ex) {
            LOG.warn("{}could not get size of file:{}", logPrefix, ex.getMessage());
            return Result.externalSafeFailure(new HDFSException("hdfs file length", ex));
          }
        }
      });
      LOG.trace("{}op completed", new Object[]{logPrefix});
      return result;
    } catch (IOException | InterruptedException ex) {
      LOG.error("{}unexpected exception:{}", logPrefix, ex);
      return Result.externalSafeFailure(new HDFSException("hdfs file length", ex));
    }
  }

  public static Supplier<Try<Boolean>> delete(DistributedFileSystem fs, HDFSEndpoint endpoint, HDFSResource resource) {
    return () -> {
      try {
        String filePath = resource.dirPath + Path.SEPARATOR + resource.fileName;
        fs.delete(new Path(filePath), false);
        return new Try.Success(true);
      } catch (IOException ex) {
        LOG.warn("{}could not delete file:{}", logPrefix, ex.getMessage());
        return new Try.Failure(new HDFSException("hdfs file delete", ex));
      }
    };
  }

  public static Try<Boolean> delete(DistributedFileSystem fs, UserGroupInformation ugi,
    HDFSEndpoint endpoint, HDFSResource resource) {
    return doAs(ugi, delete(fs, endpoint, resource));
  }

  public static Result<Boolean> delete(UserGroupInformation ugi, final HDFSEndpoint endpoint, HDFSResource resource) {
    final String filePath = resource.dirPath + Path.SEPARATOR + resource.fileName;
    LOG.info("{}deleting file:{}", new Object[]{logPrefix, filePath});
    try {
      Result<Boolean> result = ugi.doAs(new PrivilegedExceptionAction<Result<Boolean>>() {
        @Override
        public Result<Boolean> run() {
          try (FileSystem fs = FileSystem.newInstance(endpoint.hdfsConfig)) {
            fs.delete(new Path(filePath), false);
            return Result.success(true);
          } catch (IOException ex) {
            LOG.warn("{}could not delete file:{}", logPrefix, ex.getMessage());
            return Result.externalUnsafeFailure(new HDFSException("hdfs file delete", ex));
          }
        }
      });
      LOG.trace("{}op completed", new Object[]{logPrefix});
      return result;
    } catch (IOException | InterruptedException ex) {
      LOG.error("{}unexpected exception:{}", logPrefix, ex);
      return Result.externalUnsafeFailure(new HDFSException("hdfs file delete", ex));
    }
  }

  public static Try<Boolean> simpleCreate(DistributedFileSystem fs, UserGroupInformation ugi,
    HDFSEndpoint endpoint, HDFSResource resource) {
    return doAs(ugi, simpleCreate(fs, endpoint, resource));
  }

  public static Supplier<Try<Boolean>> simpleCreate(DistributedFileSystem fs,
    HDFSEndpoint endpoint, HDFSResource resource) {
    return () -> {
      try {
        String filePath = resource.dirPath + Path.SEPARATOR + resource.fileName;
        if (!fs.isDirectory(new Path(resource.dirPath))) {
          fs.mkdirs(new Path(resource.dirPath));
        }
        if (fs.isFile(new Path(filePath))) {
          return new Try.Success(false);
        }
        try (FSDataOutputStream out = fs.create(new Path(filePath), (short) 1)) {
          return new Try.Success(true);
        }
      } catch (IOException ex) {
        LOG.warn("{}could not write file:{}", logPrefix, ex.getMessage());
        return new Try.Failure(new HDFSException("hdfs file simpleCreate", ex));
      }
    };
  }

  public static Result<Boolean> simpleCreate(UserGroupInformation ugi, final HDFSEndpoint endpoint,
    final HDFSResource resource) {
    final String filePath = resource.dirPath + Path.SEPARATOR + resource.fileName;
    LOG.info("{}creating file:{}", new Object[]{logPrefix, filePath});
    try {
      Result<Boolean> result = ugi.doAs(new PrivilegedExceptionAction<Result<Boolean>>() {
        @Override
        public Result<Boolean> run() {
          try (FileSystem fs = FileSystem.newInstance(endpoint.hdfsConfig)) {
            if (!fs.isDirectory(new Path(resource.dirPath))) {
              fs.mkdirs(new Path(resource.dirPath));
            }
            if (fs.isFile(new Path(filePath))) {
              return Result.success(false);
            }
            try (FSDataOutputStream out = fs.create(new Path(filePath), (short) 1)) {
              return Result.success(true);
            }
          } catch (IOException ex) {
            LOG.warn("{}could not write file:{}", logPrefix, ex.getMessage());
            return Result.externalUnsafeFailure(new HDFSException("hdfs file simpleCreate", ex));
          }
        }
      });
      LOG.trace("{}op completed", new Object[]{logPrefix});
      return result;
    } catch (IOException | InterruptedException ex) {
      LOG.error("{}unexpected exception:{}", logPrefix, ex);
      return Result.externalUnsafeFailure(new HDFSException("hdfs file simpleCreate", ex));
    }
  }

  public static Try<Boolean> createWithLength(DistributedFileSystem fs, UserGroupInformation ugi,
    HDFSEndpoint endpoint, HDFSResource resource, long fileSize) {
    return doAs(ugi, createWithLength(fs, endpoint, resource, fileSize));
  }

  public static Supplier<Try<Boolean>> createWithLength(DistributedFileSystem fs,
    HDFSEndpoint endpoint, HDFSResource resource, long fileSize) {
    return () -> {
      try {
        String filePath = resource.dirPath + Path.SEPARATOR + resource.fileName;
        if (!fs.isDirectory(new Path(resource.dirPath))) {
          fs.mkdirs(new Path(resource.dirPath));
        }
        if (fs.isFile(new Path(filePath))) {
          return new Try.Success(false);
        }
        Random rand = new Random(1234);
        try (FSDataOutputStream out = fs.create(new Path(filePath))) {
          for (int i = 0; i < fileSize / 1024; i++) {
            byte[] data = new byte[1024];
            rand.nextBytes(data);
            out.write(data);
            out.flush();
          }
          if (fileSize % 1024 != 0) {
            byte[] data = new byte[(int) (fileSize % 1024)];
            rand.nextBytes(data);
            out.write(data);
            out.flush();
          }
          return new Try.Success(true);
        }
      } catch (IOException ex) {
        LOG.warn("{}could not create file:{}", logPrefix, ex.getMessage());
        return new Try.Failure(new HDFSException("hdfs file createWithLength", ex));
      }
    };
  }

  public static Result<Boolean> createWithLength(UserGroupInformation ugi, final HDFSEndpoint endpoint,
    final HDFSResource resource, final long fileSize) {
    final String filePath = resource.dirPath + Path.SEPARATOR + resource.fileName;
    LOG.debug("{}creating file:{}", new Object[]{logPrefix, filePath});
    try {
      Result<Boolean> result = ugi.doAs(new PrivilegedExceptionAction<Result<Boolean>>() {
        @Override
        public Result<Boolean> run() {
          try (FileSystem fs = FileSystem.newInstance(endpoint.hdfsConfig)) {
            if (!fs.isDirectory(new Path(resource.dirPath))) {
              fs.mkdirs(new Path(resource.dirPath));
            }
            if (fs.isFile(new Path(filePath))) {
              return Result.success(false);
            }
            Random rand = new Random(1234);
            try (FSDataOutputStream out = fs.create(new Path(filePath))) {
              for (int i = 0; i < fileSize / 1024; i++) {
                byte[] data = new byte[1024];
                rand.nextBytes(data);
                out.write(data);
                out.flush();
              }
              if (fileSize % 1024 != 0) {
                byte[] data = new byte[(int) (fileSize % 1024)];
                rand.nextBytes(data);
                out.write(data);
                out.flush();
              }
              return Result.success(true);
            }
          } catch (IOException ex) {
            LOG.warn("{}could not create file:{}", logPrefix, ex.getMessage());
            return Result.externalUnsafeFailure(new HDFSException("hdfs file createWithLength", ex));
          }
        }
      });
      LOG.trace("{}op completed", new Object[]{logPrefix});
      return result;
    } catch (IOException | InterruptedException ex) {
      LOG.error("{}unexpected exception:{}", logPrefix, ex);
      return Result.externalUnsafeFailure(new HDFSException("hdfs file createWithLength", ex));
    }
  }

  public static Try<byte[]> read(DistributedFileSystem fs, UserGroupInformation ugi,
    HDFSEndpoint endpoint, HDFSResource resource, KRange readRange) {
    return doAs(ugi, read(fs, endpoint, resource, readRange));
  }

  public static Supplier<Try<byte[]>> read(DistributedFileSystem fs,
    HDFSEndpoint endpoint, HDFSResource resource, KRange readRange) {
    return () -> {
      String filePath = resource.dirPath + Path.SEPARATOR + resource.fileName;
      try (FSDataInputStream in = fs.open(new Path(filePath))) {
        int readLength = (int) (readRange.upperAbsEndpoint() - readRange.lowerAbsEndpoint() + 1);
        byte[] byte_read = new byte[readLength];
        in.readFully(readRange.lowerAbsEndpoint(), byte_read);
        return new Try.Success(byte_read);
      } catch (IOException ex) {
        LOG.warn("{}could not read file:{} ex:{}", new Object[]{logPrefix, filePath, ex.getMessage()});
        return new Try.Failure(new HDFSException("hdfs file read", ex));
      }
    };
  }

  public static Result<byte[]> read(UserGroupInformation ugi, final HDFSEndpoint endpoint, HDFSResource resource,
    final KRange readRange) {
    final String filePath = resource.dirPath + Path.SEPARATOR + resource.fileName;
    LOG.debug("{}reading from file:{}", new Object[]{logPrefix, filePath});
    try {
      Result<byte[]> result = ugi.doAs(new PrivilegedExceptionAction<Result<byte[]>>() {
        @Override
        public Result<byte[]> run() {
          try (DistributedFileSystem fs = (DistributedFileSystem) FileSystem.newInstance(endpoint.hdfsConfig);
            FSDataInputStream in = fs.open(new Path(filePath))) {
            int readLength = (int) (readRange.upperAbsEndpoint() - readRange.lowerAbsEndpoint() + 1);
            byte[] byte_read = new byte[readLength];
            in.readFully(readRange.lowerAbsEndpoint(), byte_read);
            return Result.success(byte_read);
          } catch (IOException ex) {
            LOG.warn("{}could not read file:{} ex:{}", new Object[]{logPrefix, filePath, ex.getMessage()});
            return Result.externalSafeFailure(new HDFSException("hdfs file read", ex));
          }
        }
      });
      LOG.trace("{}op completed", new Object[]{logPrefix});
      return result;
    } catch (IOException | InterruptedException ex) {
      LOG.error("{}unexpected exception:{}", logPrefix, ex);
      return Result.externalSafeFailure(new HDFSException("hdfs file read", ex));
    }
  }

  public static Try<Boolean> append(DistributedFileSystem fs, UserGroupInformation ugi,
    HDFSEndpoint endpoint, HDFSResource resource, byte[] data) {
    return doAs(ugi, append(fs, endpoint, resource, data));
  }

  public static Supplier<Try<Boolean>> append(DistributedFileSystem fs,
    HDFSEndpoint endpoint, HDFSResource resource, byte[] data) {
    return () -> {
      String filePath = resource.dirPath + Path.SEPARATOR + resource.fileName;
      try (FSDataOutputStream out = fs.append(new Path(filePath))) {
        out.write(data);
        return new Try.Success(true);
      } catch (IOException ex) {
        LOG.warn("{}could not append to file:{} ex:{}", new Object[]{logPrefix, filePath, ex.getMessage()});
        return new Try.Failure(new HDFSException("hdfs file append", ex));
      }
    };
  }

  public static Result<Boolean> append(UserGroupInformation ugi, final HDFSEndpoint hdfsEndpoint, HDFSResource resource,
    final byte[] data) {
    final String filePath = resource.dirPath + Path.SEPARATOR + resource.fileName;
    LOG.debug("{}appending to file:{}", new Object[]{logPrefix, filePath});
    try {
      Result<Boolean> result = ugi.doAs(new PrivilegedExceptionAction<Result<Boolean>>() {
        @Override
        public Result<Boolean> run() {
          try (DistributedFileSystem fs = (DistributedFileSystem) FileSystem.newInstance(hdfsEndpoint.hdfsConfig);
            FSDataOutputStream out = fs.append(new Path(filePath))) {
            out.write(data);
            return Result.success(true);
          } catch (IOException ex) {
            LOG.warn("{}could not append to file:{} ex:{}", new Object[]{logPrefix, filePath, ex.getMessage()});
            return Result.externalUnsafeFailure(new HDFSException("hdfs file append", ex));
          }
        }
      });
      LOG.trace("{}op completed", new Object[]{logPrefix});
      return result;
    } catch (IOException | InterruptedException ex) {
      LOG.error("{}unexpected exception:{}", logPrefix, ex);
      return Result.externalUnsafeFailure(new HDFSException("hdfs file append", ex));
    }
  }

  public static Try<Boolean> flush(DistributedFileSystem fs, UserGroupInformation ugi,
    HDFSEndpoint endpoint, HDFSResource resource) {
    return doAs(ugi, flush(fs, endpoint, resource));
  }

  public static Supplier<Try<Boolean>> flush(DistributedFileSystem fs,
    HDFSEndpoint endpoint, HDFSResource resource) {
    return () -> {
      String filePath = resource.dirPath + Path.SEPARATOR + resource.fileName;
      try (FSDataOutputStream out = fs.append(new Path(filePath))) {
        out.hflush();
        return new Try.Success(true);
      } catch (IOException ex) {
        LOG.warn("{}could not append to file:{} ex:{}", new Object[]{logPrefix, filePath, ex.getMessage()});
        return new Try.Failure(new HDFSException("hdfs file append", ex));
      }
    };
  }

  public static Result<Boolean> flush(UserGroupInformation ugi, final HDFSEndpoint hdfsEndpoint, HDFSResource resource) {
    final String filePath = resource.dirPath + Path.SEPARATOR + resource.fileName;
    LOG.debug("{}flushing file:{}", new Object[]{logPrefix, filePath});
    try {
      Result<Boolean> result = ugi.doAs(new PrivilegedExceptionAction<Result<Boolean>>() {
        @Override
        public Result<Boolean> run() {
          try (DistributedFileSystem fs = (DistributedFileSystem) FileSystem.newInstance(hdfsEndpoint.hdfsConfig);
            FSDataOutputStream out = fs.append(new Path(filePath))) {
            out.hflush();
            return Result.success(true);
          } catch (IOException ex) {
            LOG.warn("{}could not append to file:{} ex:{}", new Object[]{logPrefix, filePath, ex.getMessage()});
            return Result.externalUnsafeFailure(new HDFSException("hdfs file append", ex));
          }
        }
      ;
      });
      LOG.trace("{}op completed", new Object[]{logPrefix});
      return result;
    } catch (IOException | InterruptedException ex) {
      LOG.error("{}unexpected exception:{}", logPrefix, ex);
      return Result.externalUnsafeFailure(new HDFSException("hdfs file append", ex));
    }
  }

  public static Try<Long> blockSize(DistributedFileSystem fs, UserGroupInformation ugi,
    HDFSEndpoint endpoint, HDFSResource resource) {
    return doAs(ugi, blockSize(fs, endpoint, resource));
  }

  public static Supplier<Try<Long>> blockSize(DistributedFileSystem fs,
    HDFSEndpoint endpoint, HDFSResource resource) {
    return () -> {
      String filePath = resource.dirPath + Path.SEPARATOR + resource.fileName;
      try {
        FileStatus status = fs.getFileStatus(new Path(filePath));
        long hdfsBlockSize = fs.getDefaultBlockSize();
        if (status.isFile()) {
          hdfsBlockSize = status.getBlockSize();
        }
        return new Try.Success(hdfsBlockSize);
      } catch (IOException ex) {
        LOG.warn("{}could not append to file:{} ex:{}", new Object[]{logPrefix, filePath, ex.getMessage()});
        return new Try.Failure(new HDFSException("hdfs file append", ex));
      }
    };
  }

  public static Result<Long> blockSize(UserGroupInformation ugi, final HDFSEndpoint hdfsEndpoint, HDFSResource resource) {
    final String filePath = resource.dirPath + Path.SEPARATOR + resource.fileName;
    LOG.debug("{}block size for file:{}", new Object[]{logPrefix, filePath});
    try {
      Result<Long> result = ugi.doAs(new PrivilegedExceptionAction<Result<Long>>() {
        @Override
        public Result<Long> run() {
          try (DistributedFileSystem fs = (DistributedFileSystem) FileSystem.newInstance(hdfsEndpoint.hdfsConfig)) {
            FileStatus status = fs.getFileStatus(new Path(filePath));
            long hdfsBlockSize = fs.getDefaultBlockSize();
            if (status.isFile()) {
              hdfsBlockSize = status.getBlockSize();
            }
            return Result.success(hdfsBlockSize);
          } catch (IOException ex) {
            LOG.warn("{}could not append to file:{} ex:{}", new Object[]{logPrefix, filePath, ex.getMessage()});
            return Result.externalUnsafeFailure(new HDFSException("hdfs file append", ex));
          }
        }
      ;
      });
      LOG.trace("{}op completed", new Object[]{logPrefix});
      return result;
    } catch (IOException | InterruptedException ex) {
      LOG.error("{}unexpected exception:{}", logPrefix, ex);
      return Result.externalUnsafeFailure(new HDFSException("hdfs file append", ex));
    }
  }

  public static Try<ManifestJSON> readManifest(DistributedFileSystem fs, UserGroupInformation ugi,
    HDFSEndpoint endpoint, HDFSResource resource) {
    return doAs(ugi, readManifest(fs, endpoint, resource));
  }

  public static Supplier<Try<ManifestJSON>> readManifest(DistributedFileSystem fs,
    HDFSEndpoint endpoint, HDFSResource resource) {
    return () -> {
      String filePath = resource.dirPath + Path.SEPARATOR + resource.fileName;
      try {
        if (!fs.isFile(new Path(filePath))) {
          LOG.warn("{}file does not exist", new Object[]{logPrefix, filePath});
          return new Try.Failure(new HDFSException("hdfs file read"));
        }
        try (FSDataInputStream in = fs.open(new Path(filePath))) {
          long manifestLength = fs.getLength(new Path(filePath));
          byte[] manifestByte = new byte[(int) manifestLength];
          in.readFully(manifestByte);
          ManifestJSON manifest = ManifestHelper.getManifestJSON(manifestByte);
          return new Try.Success(manifest);
        }
      } catch (IOException ex) {
        LOG.warn("{}could not read file:{} ex:{}", new Object[]{logPrefix, filePath, ex.getMessage()});
        return new Try.Failure(new HDFSException("hdfs file read", ex));
      }
    };
  }

  public static Result<ManifestJSON> readManifest(UserGroupInformation ugi, final HDFSEndpoint endpoint,
    HDFSResource resource) {
    final String filePath = resource.dirPath + Path.SEPARATOR + resource.fileName;
    LOG.debug("{}reading manifest:{}", new Object[]{logPrefix, filePath});
    try {
      Result<ManifestJSON> result = ugi.doAs(new PrivilegedExceptionAction<Result<ManifestJSON>>() {
        @Override
        public Result<ManifestJSON> run() {
          try (DistributedFileSystem fs = (DistributedFileSystem) FileSystem.newInstance(endpoint.hdfsConfig)) {
            if (!fs.isFile(new Path(filePath))) {
              LOG.warn("{}file does not exist", new Object[]{logPrefix, filePath});
              return Result.externalSafeFailure(new HDFSException("hdfs file read"));
            }
            try (FSDataInputStream in = fs.open(new Path(filePath))) {
              long manifestLength = fs.getLength(new Path(filePath));
              byte[] manifestByte = new byte[(int) manifestLength];
              in.readFully(manifestByte);
              ManifestJSON manifest = ManifestHelper.getManifestJSON(manifestByte);
              return Result.success(manifest);
            }
          } catch (IOException ex) {
            LOG.warn("{}could not read file:{} ex:{}", new Object[]{logPrefix, filePath, ex.getMessage()});
            return Result.externalSafeFailure(new HDFSException("hdfs file read", ex));
          }
        }
      });
      LOG.trace("{}op completed", new Object[]{logPrefix});
      return result;
    } catch (IOException | InterruptedException ex) {
      LOG.error("{}unexpected exception:{}", logPrefix, ex);
      return Result.externalSafeFailure(new HDFSException("hdfs file read", ex));
    }
  }

  public static Try<Boolean> writeManifest(DistributedFileSystem fs, UserGroupInformation ugi,
    HDFSEndpoint endpoint, HDFSResource resource, ManifestJSON manifest) {
    return doAs(ugi, writeManifest(fs, endpoint, resource, manifest));
  }

  public static Supplier<Try<Boolean>> writeManifest(DistributedFileSystem fs,
    HDFSEndpoint endpoint, HDFSResource resource, ManifestJSON manifest) {
    return () -> {
      try {
        String filePath = resource.dirPath + Path.SEPARATOR + resource.fileName;
        if (!fs.isDirectory(new Path(resource.dirPath))) {
          fs.mkdirs(new Path(resource.dirPath));
        }
        if (fs.isFile(new Path(filePath))) {
          return new Try.Success(false);
        }
        try (FSDataOutputStream out = fs.create(new Path(filePath))) {
          byte[] manifestByte = ManifestHelper.getManifestByte(manifest);
          out.write(manifestByte);
          out.flush();
          return new Try.Success(true);
        }
      } catch (IOException ex) {
        LOG.warn("{}could not create file:{}", logPrefix, ex.getMessage());
        return new Try.Failure(new HDFSException("hdfs file createWithLength", ex));
      }
    };
  }

  public static Result<Boolean> writeManifest(UserGroupInformation ugi, final HDFSEndpoint endpoint,
    final HDFSResource resource, final ManifestJSON manifest) {
    final String filePath = resource.dirPath + Path.SEPARATOR + resource.fileName;
    LOG.debug("{}writing manifest:{}", new Object[]{logPrefix, filePath});
    try {
      Result<Boolean> result = ugi.doAs(new PrivilegedExceptionAction<Result<Boolean>>() {
        @Override
        public Result<Boolean> run() {
          try (FileSystem fs = FileSystem.newInstance(endpoint.hdfsConfig)) {
            if (!fs.isDirectory(new Path(resource.dirPath))) {
              fs.mkdirs(new Path(resource.dirPath));
            }
            if (fs.isFile(new Path(filePath))) {
              return Result.success(false);
            }
            try (FSDataOutputStream out = fs.create(new Path(filePath))) {
              byte[] manifestByte = ManifestHelper.getManifestByte(manifest);
              out.write(manifestByte);
              out.flush();
              return Result.success(true);
            }
          } catch (IOException ex) {
            LOG.warn("{}could not create file:{}", logPrefix, ex.getMessage());
            return Result.externalUnsafeFailure(new HDFSException("hdfs file createWithLength", ex));
          }
        }
      });
      LOG.trace("{}op completed", new Object[]{logPrefix});
      return result;
    } catch (IOException | InterruptedException ex) {
      LOG.error("{}unexpected exception:{}", logPrefix, ex);
      return Result.externalUnsafeFailure(new HDFSException("hdfs file createWithLength", ex));
    }
  }
}
