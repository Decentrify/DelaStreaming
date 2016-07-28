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
package se.sics.nstream.hops.hdfs;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.security.PrivilegedExceptionAction;
import java.util.Random;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.LoggerFactory;
import se.sics.ktoolbox.util.result.Result;
import se.sics.nstream.hops.manifest.ManifestJSON;
import se.sics.nstream.util.range.KRange;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class HDFSHelper {

    private final static org.slf4j.Logger LOG = LoggerFactory.getLogger(HDFSHelper.class);
    private static String logPrefix = "";

    public static Result<Boolean> canConnect(final Configuration hdfsConfig) {
        LOG.debug("{}testing hdfs connection", logPrefix);
        try (FileSystem fs = FileSystem.get(hdfsConfig)) {
            LOG.debug("{}getting status", logPrefix);
            FsStatus status = fs.getStatus();
            LOG.debug("{}got status", logPrefix);
            return Result.success(true);
        } catch (IOException ex) {
            LOG.info("{}could not connect:{}", logPrefix, ex.getMessage());
            return Result.success(false);
        }
    }

    public static Result<Long> length(UserGroupInformation ugi, final HDFSEndpoint hdfsEndpoint, HDFSResource resource) {
        final String filePath = resource.dirPath + Path.SEPARATOR + resource.fileName;
        LOG.debug("{}getting length of file:{}", new Object[]{logPrefix, filePath});

        try {
            Result<Long> result = ugi.doAs(new PrivilegedExceptionAction<Result<Long>>() {
                @Override
                public Result<Long> run() {
                    try (FileSystem fs = FileSystem.get(hdfsEndpoint.hdfsConfig)) {
                        long length = -1;
                        if (fs.isFile(new Path(filePath))) {
                            length = fs.getLength(new Path(filePath));
                        }
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

    public static Result<Boolean> delete(UserGroupInformation ugi, final HDFSEndpoint hdfsEndpoint, HDFSResource resource) {
        final String filePath = resource.dirPath + Path.SEPARATOR + resource.fileName;
        LOG.debug("{}deleting file:{}", new Object[]{logPrefix, filePath});
        try {
            Result<Boolean> result = ugi.doAs(new PrivilegedExceptionAction<Result<Boolean>>() {
                @Override
                public Result<Boolean> run() {
                    try (FileSystem fs = FileSystem.get(hdfsEndpoint.hdfsConfig)) {
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

    public static Result<Boolean> simpleCreate(UserGroupInformation ugi, final HDFSEndpoint hdfsEndpoint, final HDFSResource hdfsResource) {
        final String filePath = hdfsResource.dirPath + Path.SEPARATOR + hdfsResource.fileName;
        LOG.debug("{}creating file:{}", new Object[]{logPrefix, filePath});
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

    public static Result<Boolean> createWithLength(UserGroupInformation ugi, final HDFSEndpoint hdfsEndpoint, final HDFSResource hdfsResource, final long fileSize) {
        final String filePath = hdfsResource.dirPath + Path.SEPARATOR + hdfsResource.fileName;
        LOG.debug("{}creating file:{}", new Object[]{logPrefix, filePath});
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

    public static Result<byte[]> read(UserGroupInformation ugi, final HDFSEndpoint hdfsEndpoint, HDFSResource resource, final KRange readRange) {
        final String filePath = resource.dirPath + Path.SEPARATOR + resource.fileName;
        LOG.debug("{}reading from file:{}", new Object[]{logPrefix, filePath});
        try {
            Result<byte[]> result = ugi.doAs(new PrivilegedExceptionAction<Result<byte[]>>() {
                @Override
                public Result<byte[]> run() {
                    try (DistributedFileSystem fs = (DistributedFileSystem) FileSystem.get(hdfsEndpoint.hdfsConfig);
                            FSDataInputStream in = fs.open(new Path(filePath))) {
                        int readLength = (int) (readRange.upperAbsEndpoint() - readRange.lowerAbsEndpoint());
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

    public static Result<Boolean> append(UserGroupInformation ugi, final HDFSEndpoint hdfsEndpoint, HDFSResource resource, final byte[] data) {
        final String filePath = resource.dirPath + Path.SEPARATOR + resource.fileName;
        LOG.debug("{}appending to file:{}", new Object[]{logPrefix, filePath});
        try {
            Result<Boolean> result = ugi.doAs(new PrivilegedExceptionAction<Result<Boolean>>() {
                @Override
                public Result<Boolean> run() {
                    try (DistributedFileSystem fs = (DistributedFileSystem) FileSystem.get(hdfsEndpoint.hdfsConfig);
                            FSDataOutputStream out = fs.append(new Path(filePath))) {
                        out.write(data);
                        out.flush();
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

    public static Result<ManifestJSON> readManifest(UserGroupInformation ugi, final HDFSEndpoint hdfsEndpoint, HDFSResource hdfsResource) {
        final String filePath = hdfsResource.dirPath + Path.SEPARATOR + hdfsResource.fileName;
        LOG.debug("{}reading manifest:{}", new Object[]{logPrefix, filePath});
        try {
            Result<ManifestJSON> result = ugi.doAs(new PrivilegedExceptionAction<Result<ManifestJSON>>() {
                @Override
                public Result<ManifestJSON> run() {
                    try (DistributedFileSystem fs = (DistributedFileSystem) FileSystem.get(hdfsEndpoint.hdfsConfig)) {
                        if (!fs.isFile(new Path(filePath))) {
                            LOG.warn("{}file does not exist", new Object[]{logPrefix, filePath});
                            return Result.externalSafeFailure(new HDFSException("hdfs file read"));
                        }
                        try (FSDataInputStream in = fs.open(new Path(filePath))) {
                            long manifestLength = fs.getLength(new Path(filePath));
                            byte[] manifestByte = new byte[(int) manifestLength];
                            in.readFully(manifestByte);
                            ManifestJSON manifest = getManifestJSON(manifestByte);
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

    public static Result<Boolean> writeManifest(UserGroupInformation ugi, final HDFSEndpoint hdfsEndpoint, final HDFSResource hdfsResource, final ManifestJSON manifest) {
        final String filePath = hdfsResource.dirPath + Path.SEPARATOR + hdfsResource.fileName;
        LOG.debug("{}writing manifest:{}", new Object[]{logPrefix, filePath});
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
                        try (FSDataOutputStream out = fs.create(new Path(filePath))) {
                            byte[] manifestByte = getManifestByte(manifest);
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

    public static ManifestJSON getManifestJSON(byte[] jsonByte) {
        String jsonString;
        try {
            jsonString = new String(jsonByte, "UTF-8");
        } catch (UnsupportedEncodingException ex) {
            throw new RuntimeException(ex);
        }
        Gson gson = new GsonBuilder().create();
        ManifestJSON manifest = gson.fromJson(jsonString, ManifestJSON.class);
        return manifest;
    }

    public static byte[] getManifestByte(ManifestJSON manifest) {
        Gson gson = new GsonBuilder().create();
        String jsonString = gson.toJson(manifest);
        byte[] jsonByte;
        try {
            jsonByte = jsonString.getBytes("UTF-8");
        } catch (UnsupportedEncodingException ex) {
            throw new RuntimeException(ex);
        }
        if (jsonByte.length > 1200) {
            throw new RuntimeException("manifest is too long");
        }
        return jsonByte;
    }
}
