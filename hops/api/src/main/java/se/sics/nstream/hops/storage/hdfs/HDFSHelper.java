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
import se.sics.ktoolbox.util.trysf.Try;
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

  public static Try<Boolean> canConnect(final Configuration hdfsConfig) {
    LOG.debug("{}testing hdfs connection", logPrefix);
    try (FileSystem fs = FileSystem.newInstance(hdfsConfig)) {
      LOG.debug("{}getting status", logPrefix);
      FsStatus status = fs.getStatus();
      LOG.debug("{}got status", logPrefix);
      return new Try.Success(true);
    } catch (IOException ex) {
      LOG.info("{}could not connect:{}", logPrefix, ex.getMessage());
      return new Try.Success(false);
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
}
