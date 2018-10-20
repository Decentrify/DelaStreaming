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
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.security.UserGroupInformation;
import se.sics.ktoolbox.util.trysf.Try;
import se.sics.ktoolbox.util.trysf.TryHelper;
import se.sics.nstream.util.range.KRange;
import se.sics.dela.storage.common.DelaStorageProvider;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class HDFSHelper {

  public static final String HOPS_URL = "fs.defaultFS";
  public static final String DATANODE_FAILURE_POLICY = "dfs.client.block.write.replace-datanode-on-failure.policy";

  public static class DoAs {

    private final UserGroupInformation ugi;

    public DoAs(UserGroupInformation ugi) {
      this.ugi = ugi;
    }

    public <O> Try<O> perform(Supplier<Try<O>> action) {
      return doAs(ugi, action);
    }

    public <I, O> BiFunction<I, Throwable, Try<O>> wrapOp(Supplier<Try<O>> action) {
      return doAsOp(ugi, action);
    }
  }

  public static class StorageProvider implements DelaStorageProvider<HDFSEndpoint, HDFSResource> {

    @Override
    public Try createPath(HDFSEndpoint endpoint, HDFSResource resource) {
      return HDFSHelper.createPath(endpoint, resource);
    }

    @Override
    public Try<Boolean> fileExists(HDFSEndpoint endpoint, HDFSResource resource) {
      return HDFSHelper.fileExists(endpoint, resource);
    }

    @Override
    public Try createFile(HDFSEndpoint endpoint, HDFSResource resource) {
      return HDFSHelper.createFile(endpoint, resource);
    }
    
    @Override
    public Try deleteFile(HDFSEndpoint endpoint, HDFSResource resource) {
      return HDFSHelper.delete(endpoint, resource);
    }
    
    @Override
    public Try<Long> fileSize(HDFSEndpoint endpoint, HDFSResource resource) {
      return HDFSHelper.fileSize(endpoint, resource);
    }

    @Override
    public Try<byte[]> read(HDFSEndpoint endpoint, HDFSResource resource, KRange range) {
      return HDFSHelper.read(endpoint, resource, range);
    }
    
    @Override
    public Try<byte[]> readAllFile(HDFSEndpoint endpoint, HDFSResource resource) {
      return HDFSHelper.readFully(endpoint, resource);
    }

    @Override
    public Try append(HDFSEndpoint endpoint, HDFSResource resource, byte[] data) {
      return HDFSHelper.append(endpoint, resource, data);
    }
  }
  
//  public static Try<Boolean> canConnect(final Configuration hdfsConfig, Logger logger) {
//    logger.debug("testing hdfs connection");
//    try (FileSystem fs = FileSystem.get(hdfsConfig)) {
//      logger.debug("{}getting status");
//      FsStatus status = fs.getStatus();
//      logger.debug("got status");
//      return Result.success(true);
//    } catch (IOException ex) {
//      logger.info("could not connect:{}", ex.getMessage());
//      return Result.success(false);
//    }
//  }
  public static Try<DoAs> doAs(String user) {
    try {
      UserGroupInformation ugi = UserGroupInformation.createProxyUser(user, UserGroupInformation.getLoginUser());
      return new Try.Success(new DoAs(ugi));
    } catch (IOException ex) {
      String msg = "could not create proxy user:" + user;
      return new Try.Failure(new HDFSClientException(msg, ex));
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
    try (DistributedFileSystem fs = (DistributedFileSystem) FileSystem.get(config)) {
      if (fs.getDataNodeStats().length == 1) {
        fs.close();
        config.set(DATANODE_FAILURE_POLICY, "NEVER");
      }
      return new Try.Success(config);
    } catch (IOException ex) {
      String msg = "could not contact filesystem";
      return new Try.Failure(new HDFSClientException(msg, ex));
    }
  }

  public static Try<Boolean> createPath(HDFSEndpoint endpoint, HDFSResource resource) {
    String filePath = resource.dirPath + Path.SEPARATOR + resource.fileName;
    try (FileSystem fs = FileSystem.get(endpoint.hdfsConfig)) {
      if (!fs.isDirectory(new Path(resource.dirPath))) {
        fs.mkdirs(new Path(resource.dirPath));
        return new Try.Success(true);
      }
      return new Try.Success(false);
    } catch (IOException ex) {
      String msg = "dir op - could not create path:" + filePath;
      return new Try.Failure(new HDFSClientException(msg, ex));
    }
  }

  public static Supplier<Try<Boolean>> createPathOp(HDFSEndpoint endpoint, HDFSResource resource) {
    return () -> createPath(endpoint, resource);
  }
  public static Try<Boolean> fileExists(HDFSEndpoint endpoint, HDFSResource resource) {
    Path filePath = new Path(resource.dirPath + Path.SEPARATOR + resource.fileName);
    try (FileSystem fs = FileSystem.get(endpoint.hdfsConfig)) {
      if (fs.exists(filePath)) {
        if (fs.isFile(filePath)) {
          return new Try.Success(true);
        } else {
          return new Try.Failure(new HDFSClientException("path exists, and it is not a file"));
        }
      } else {
        return new Try.Success(false);
      }
    } catch (IOException ex) {
      String msg = "dir op - could not check exists:" + filePath;
      return new Try.Failure(new HDFSClientException(msg, ex));
    }
  }

  public static Supplier<Try<Boolean>> fileExistsOp(HDFSEndpoint endpoint, HDFSResource resource) {
    return () -> fileExists(endpoint, resource);
  }

  public static Try<Boolean> createFile(HDFSEndpoint endpoint, HDFSResource resource) {
    String filePath = resource.dirPath + Path.SEPARATOR + resource.fileName;
    try (FileSystem fs = FileSystem.get(endpoint.hdfsConfig)) {
      if (fs.isFile(new Path(filePath))) {
        return new Try.Success(false);
      }
      try (FSDataOutputStream out = fs.create(new Path(filePath))) {
        return new Try.Success(true);
      }
    } catch (IOException ex) {
      String msg = "dir op - could not create:" + filePath;
      return new Try.Failure(new HDFSClientException(msg, ex));
    }
  }

  public static Supplier<Try<Boolean>> createOp(HDFSEndpoint endpoint, HDFSResource resource) {
    return () -> createFile(endpoint, resource);
  }

  public static Try<Boolean> delete(HDFSEndpoint endpoint, HDFSResource resource) {
    String filePath = resource.dirPath + Path.SEPARATOR + resource.fileName;
    try (FileSystem fs = FileSystem.get(endpoint.hdfsConfig)) {
      if (!fs.exists(new Path(filePath))) {
        return new Try.Success(false);
      }
      fs.delete(new Path(filePath), true);
      return new Try.Success(true);
    } catch (IOException ex) {
      String msg = "dir op- could not delete:" + filePath;
      return new Try.Failure(new HDFSClientException(msg, ex));
    }
  }

  public static Supplier<Try<Long>> fileSizeOp(HDFSEndpoint endpoint, HDFSResource resource) {
    return () -> fileSize(endpoint, resource);
  }

  public static Try<Long> fileSize(HDFSEndpoint endpoint, HDFSResource resource) {
    final String filePath = resource.dirPath + Path.SEPARATOR + resource.fileName;
    try (DistributedFileSystem fs = (DistributedFileSystem) FileSystem.get(endpoint.hdfsConfig)) {
      FileStatus status = fs.getFileStatus(new Path(filePath));
      if (status.isFile()) {
        return new Try.Success(status.getLen());
      } else {
        return new Try.Success(-1l);
      }
    } catch (FileNotFoundException ex) {
      return new Try.Success(-1l);
    } catch (IOException ex) {
      String msg = "file meta - could not get size of file:" + filePath;
      return new Try.Failure(new HDFSClientException(msg, ex));
    }
  }

  public static Try<Boolean> append(HDFSEndpoint endpoint, HDFSResource resource, byte[] data) {
    String filePath = resource.dirPath + Path.SEPARATOR + resource.fileName;
    try (DistributedFileSystem fs = (DistributedFileSystem) FileSystem.get(endpoint.hdfsConfig);
      FSDataOutputStream out = fs.append(new Path(filePath))) {
      out.write(data);
      return new Try.Success(true);
    } catch (IOException ex) {
      String msg = "file op - could not append to file:" + filePath;
      return new Try.Failure(new HDFSClientException(msg, ex));
    }
  }

  public static Supplier<Try<Boolean>> appendOp(HDFSEndpoint endpoint, HDFSResource resource, byte[] data) {
    return () -> append(endpoint, resource, data);
  }
  
  public static Try<Boolean> append(FSDataOutputStream out, byte[] data) {
    try {
      out.write(data);
      return new Try.Success(true);
    } catch (IOException ex) {
      String msg = "file op - could not append";
      return new Try.Failure(new HDFSClientException(msg, ex));
    }
  }

  public static Try<byte[]> read(HDFSEndpoint endpoint, HDFSResource resource, KRange range) {
    String filePath = resource.dirPath + Path.SEPARATOR + resource.fileName;
    try (DistributedFileSystem fs = (DistributedFileSystem) FileSystem.get(endpoint.hdfsConfig);
      FSDataInputStream in = fs.open(new Path(filePath))) {
      int readLength = (int) (range.upperAbsEndpoint() - range.lowerAbsEndpoint() + 1);
      byte[] byte_read = new byte[readLength];
      in.readFully(range.lowerAbsEndpoint(), byte_read);
      return new Try.Success(byte_read);
    } catch (IOException ex) {
      String msg = "file op - could not read file:" + filePath;
      return new Try.Failure(new HDFSClientException(msg, ex));
    }
  }

  public static Try<byte[]> readFully(HDFSEndpoint endpoint, HDFSResource resource) {
    String filePath = resource.dirPath + Path.SEPARATOR + resource.fileName;
    try (DistributedFileSystem fs = (DistributedFileSystem) FileSystem.get(endpoint.hdfsConfig);
      FSDataInputStream in = fs.open(new Path(filePath))) {
      FileStatus status = fs.getFileStatus(new Path(filePath));
      byte[] allBytes = new byte[(int) status.getLen()];
      in.readFully(allBytes);
      return new Try.Success(allBytes);
    } catch (IOException ex) {
      String msg = "file op - could not read file:" + filePath;
      return new Try.Failure(new HDFSClientException(msg, ex));
    }
  }

  public static Supplier<Try<byte[]>> readFullyOp(HDFSEndpoint endpoint, HDFSResource resource) {
    return () -> readFully(endpoint, resource);
  }

  public static Try<Boolean> flush(HDFSEndpoint endpoint, HDFSResource resource) {
    String filePath = resource.dirPath + Path.SEPARATOR + resource.fileName;
    try (DistributedFileSystem fs = (DistributedFileSystem) FileSystem.get(endpoint.hdfsConfig);
      FSDataOutputStream out = fs.append(new Path(filePath))) {
      out.hflush();
      return new Try.Success(true);
    } catch (IOException ex) {
      String msg = "file op - could not flush to file:{}" + filePath;
      return new Try.Failure(new HDFSClientException(msg, ex));
    }
  }

  public static Try<Long> blockSize(HDFSEndpoint endpoint, HDFSResource resource) {
    String filePath = resource.dirPath + Path.SEPARATOR + resource.fileName;
    try (DistributedFileSystem fs = (DistributedFileSystem) FileSystem.get(endpoint.hdfsConfig)) {
      FileStatus status = fs.getFileStatus(new Path(filePath));
      long hdfsBlockSize = fs.getDefaultBlockSize();
      if (status.isFile()) {
        hdfsBlockSize = status.getBlockSize();
      }
      return new Try.Success(hdfsBlockSize);
    } catch (IOException ex) {
      String msg = "file meta - could not get block size for file" + filePath;
      return new Try.Failure(new HDFSClientException(msg, ex));
    }
  }
  
  public static Supplier<Try<Long>> blockSizeOp(HDFSEndpoint endpoint, HDFSResource resource) {
    return () -> blockSize(endpoint, resource);
  }

  public static class HDFSClientException extends Exception {

    public HDFSClientException(String msg, Throwable cause) {
      super(msg, cause);
    }

    public HDFSClientException(String msg) {
      super(msg);
    }
  }
}
