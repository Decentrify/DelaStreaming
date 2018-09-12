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
import java.security.PrivilegedExceptionAction;
import java.util.Random;
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
import se.sics.ktoolbox.util.result.Result;
import se.sics.nstream.util.range.KRange;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class HDFSHelper {


  public static Result<Boolean> canConnect(final Configuration hdfsConfig, Logger logger) {
    logger.debug("testing hdfs connection");
    try (FileSystem fs = FileSystem.get(hdfsConfig)) {
      logger.debug("{}getting status");
      FsStatus status = fs.getStatus();
      logger.debug("got status");
      return Result.success(true);
    } catch (IOException ex) {
      logger.info("could not connect:{}", ex.getMessage());
      return Result.success(false);
    }
  }

  public static Result<Long> length(UserGroupInformation ugi, final HDFSEndpoint hdfsEndpoint, HDFSResource resource,
    Logger logger) {
    final String filePath = resource.dirPath + Path.SEPARATOR + resource.fileName;
    logger.debug("{}getting length of file:{}", new Object[]{filePath});

    try {
      Result<Long> result = ugi.doAs(new PrivilegedExceptionAction<Result<Long>>() {
        @Override
        public Result<Long> run() {
          long length = -1;
          try (DistributedFileSystem fs = (DistributedFileSystem) FileSystem.get(hdfsEndpoint.hdfsConfig)) {
            FileStatus status = fs.getFileStatus(new Path(filePath));
            if (status.isFile()) {
              length = status.getLen();
            }
            return Result.success(length);
          } catch (FileNotFoundException ex) {
            return Result.success(length);
          } catch (IOException ex) {
            logger.warn("could not get size of file:{}", ex.getMessage());
            return Result.externalSafeFailure(new HDFSException("hdfs file length", ex));
          }
        }
      });
      logger.trace("op completed");
      return result;
    } catch (IOException | InterruptedException ex) {
      logger.error("unexpected exception:{}", ex);
      return Result.externalSafeFailure(new HDFSException("hdfs file length", ex));
    }
  }
  
  public static Result<Boolean> delete(UserGroupInformation ugi, final HDFSEndpoint hdfsEndpoint, HDFSResource resource, 
    Logger logger) {
    final String filePath = resource.dirPath + Path.SEPARATOR + resource.fileName;
    logger.info("deleting file:{}", new Object[]{filePath});
    try {
      Result<Boolean> result = ugi.doAs(new PrivilegedExceptionAction<Result<Boolean>>() {
        @Override
        public Result<Boolean> run() {
          try (FileSystem fs = FileSystem.get(hdfsEndpoint.hdfsConfig)) {
            fs.delete(new Path(filePath), false);
            return Result.success(true);
          } catch (IOException ex) {
            logger.warn("could not delete file:{}", ex.getMessage());
            return Result.externalUnsafeFailure(new HDFSException("hdfs file delete", ex));
          }
        }
      });
      logger.trace("op completed");
      return result;
    } catch (IOException | InterruptedException ex) {
      logger.error("unexpected exception:{}", ex);
      return Result.externalUnsafeFailure(new HDFSException("hdfs file delete", ex));
    }
  }

  public static Result<Boolean> simpleCreate(UserGroupInformation ugi, final HDFSEndpoint hdfsEndpoint,
    final HDFSResource hdfsResource, Logger logger) {
    final String filePath = hdfsResource.dirPath + Path.SEPARATOR + hdfsResource.fileName;
    logger.info("creating file:{}", new Object[]{filePath});
    try {
      Result<Boolean> result = ugi.doAs(new PrivilegedExceptionAction<Result<Boolean>>() {
        @Override
        public Result<Boolean> run() {
          try (FileSystem fs = FileSystem.get(hdfsEndpoint.hdfsConfig)) {
            if (!fs.isDirectory(new Path(hdfsResource.dirPath))) {
              fs.mkdirs(new Path(hdfsResource.dirPath));
            }
            if (fs.isFile(new Path(filePath))) {
              return Result.success(false);
            }
            try (FSDataOutputStream out = fs.create(new Path(filePath), (short) 1)) {
              return Result.success(true);
            }
          } catch (IOException ex) {
            logger.warn("could not write file:{}", ex.getMessage());
            return Result.externalUnsafeFailure(new HDFSException("hdfs file simpleCreate", ex));
          }
        }
      });
      logger.trace("op completed");
      return result;
    } catch (IOException | InterruptedException ex) {
      logger.error("unexpected exception:{}", ex);
      return Result.externalUnsafeFailure(new HDFSException("hdfs file simpleCreate", ex));
    }
  }

  public static Result<Boolean> createWithLength(UserGroupInformation ugi, final HDFSEndpoint hdfsEndpoint,
    final HDFSResource hdfsResource, final long fileSize, Logger logger) {
    final String filePath = hdfsResource.dirPath + Path.SEPARATOR + hdfsResource.fileName;
    logger.debug("creating file:{}", new Object[]{filePath});
    try {
      Result<Boolean> result = ugi.doAs(new PrivilegedExceptionAction<Result<Boolean>>() {
        @Override
        public Result<Boolean> run() {
          try (FileSystem fs = FileSystem.get(hdfsEndpoint.hdfsConfig)) {
            if (!fs.isDirectory(new Path(hdfsResource.dirPath))) {
              fs.mkdirs(new Path(hdfsResource.dirPath));
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
            logger.warn("could not create file:{}", ex.getMessage());
            return Result.externalUnsafeFailure(new HDFSException("hdfs file createWithLength", ex));
          }
        }
      });
      logger.trace("op completed");
      return result;
    } catch (IOException | InterruptedException ex) {
      logger.error("unexpected exception:{}", ex);
      return Result.externalUnsafeFailure(new HDFSException("hdfs file createWithLength", ex));
    }
  }

  public static Result<byte[]> read(UserGroupInformation ugi, final HDFSEndpoint hdfsEndpoint, HDFSResource resource,
    final KRange readRange, Logger logger) {
    final String filePath = resource.dirPath + Path.SEPARATOR + resource.fileName;
    logger.debug("reading from file:{}", new Object[]{filePath});
    try {
      Result<byte[]> result = ugi.doAs(new PrivilegedExceptionAction<Result<byte[]>>() {
        @Override
        public Result<byte[]> run() {
          try (DistributedFileSystem fs = (DistributedFileSystem) FileSystem.get(hdfsEndpoint.hdfsConfig);
            FSDataInputStream in = fs.open(new Path(filePath))) {
            int readLength = (int) (readRange.upperAbsEndpoint() - readRange.lowerAbsEndpoint() + 1);
            byte[] byte_read = new byte[readLength];
            in.readFully(readRange.lowerAbsEndpoint(), byte_read);
            return Result.success(byte_read);
          } catch (IOException ex) {
            logger.warn("could not read file:{} ex:{}", new Object[]{filePath, ex.getMessage()});
            return Result.externalSafeFailure(new HDFSException("hdfs file read", ex));
          }
        }
      });
      logger.trace("op completed");
      return result;
    } catch (IOException | InterruptedException ex) {
      logger.error("unexpected exception:{}", ex);
      return Result.externalSafeFailure(new HDFSException("hdfs file read", ex));
    }
  }

  public static Result<Boolean> append(UserGroupInformation ugi, final HDFSEndpoint hdfsEndpoint, HDFSResource resource,
    final byte[] data, Logger logger) {
    final String filePath = resource.dirPath + Path.SEPARATOR + resource.fileName;
    logger.debug("appending to file:{}", new Object[]{filePath});
    try {
      Result<Boolean> result = ugi.doAs(new PrivilegedExceptionAction<Result<Boolean>>() {
        @Override
        public Result<Boolean> run() {
          try (DistributedFileSystem fs = (DistributedFileSystem) FileSystem.get(hdfsEndpoint.hdfsConfig);
            FSDataOutputStream out = fs.append(new Path(filePath))) {
            out.write(data);
            return Result.success(true);
          } catch (IOException ex) {
            logger.warn("could not append to file:{} ex:{}", new Object[]{filePath, ex.getMessage()});
            return Result.externalUnsafeFailure(new HDFSException("hdfs file append", ex));
          }
        }
      });
      logger.trace("op completed");
      return result;
    } catch (IOException | InterruptedException ex) {
      logger.error("unexpected exception:{}", ex);
      return Result.externalUnsafeFailure(new HDFSException("hdfs file append", ex));
    }
  }
  
  public static Result<Boolean> flush(UserGroupInformation ugi, final HDFSEndpoint hdfsEndpoint, HDFSResource resource, 
    Logger logger) {
    final String filePath = resource.dirPath + Path.SEPARATOR + resource.fileName;
    logger.debug("flushing file:{}", new Object[]{filePath});
    try {
      Result<Boolean> result = ugi.doAs(new PrivilegedExceptionAction<Result<Boolean>>() {
        @Override
        public Result<Boolean> run() {
          try (DistributedFileSystem fs = (DistributedFileSystem) FileSystem.get(hdfsEndpoint.hdfsConfig);
            FSDataOutputStream out = fs.append(new Path(filePath))) {
            out.hflush();
            return Result.success(true);
          } catch (IOException ex) {
            logger.warn("could not append to file:{} ex:{}", new Object[]{filePath, ex.getMessage()});
            return Result.externalUnsafeFailure(new HDFSException("hdfs file append", ex));
          }
        };
      });
      logger.trace("op completed");
      return result;
    } catch (IOException | InterruptedException ex) {
      logger.error("unexpected exception:{}", ex);
      return Result.externalUnsafeFailure(new HDFSException("hdfs file append", ex));
    }
  }
  
  public static Result<Long> blockSize(UserGroupInformation ugi, final HDFSEndpoint hdfsEndpoint, HDFSResource resource,
    Logger logger) {
    final String filePath = resource.dirPath + Path.SEPARATOR + resource.fileName;
    logger.debug("block size for file:{}", new Object[]{filePath});
    try {
      Result<Long> result = ugi.doAs(new PrivilegedExceptionAction<Result<Long>>() {
        @Override
        public Result<Long> run() {
          try (DistributedFileSystem fs = (DistributedFileSystem) FileSystem.get(hdfsEndpoint.hdfsConfig)) {
            FileStatus status = fs.getFileStatus(new Path(filePath));
            long hdfsBlockSize = fs.getDefaultBlockSize();
            if (status.isFile()) {
              hdfsBlockSize = status.getBlockSize();
            }
            return Result.success(hdfsBlockSize);
          } catch (IOException ex) {
            logger.warn("could not append to file:{} ex:{}", new Object[]{filePath, ex.getMessage()});
            return Result.externalUnsafeFailure(new HDFSException("hdfs file append", ex));
          }
        };
      });
      logger.trace("op completed");
      return result;
    } catch (IOException | InterruptedException ex) {
      logger.error("unexpected exception:{}", ex);
      return Result.externalUnsafeFailure(new HDFSException("hdfs file append", ex));
    }
  }
}
