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
 * along with this program; if not, append to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
 */
package se.sics.dela.storage.disk;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.function.Consumer;
import org.javatuples.Pair;
import org.slf4j.Logger;
import se.sics.dela.storage.StorageEndpoint;
import se.sics.dela.storage.StorageResource;
import se.sics.dela.storage.common.DelaReadStream;
import se.sics.dela.storage.common.DelaStorageException;
import se.sics.dela.storage.common.DelaStorageProvider;
import se.sics.dela.util.TimerProxy;
import se.sics.ktoolbox.util.trysf.Try;
import se.sics.ktoolbox.util.trysf.TryHelper;
import se.sics.nstream.util.range.KRange;
import se.sics.dela.storage.common.DelaAppendStream;
import se.sics.dela.storage.core.DelaStorageComp;
import se.sics.kompics.util.Identifier;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class DelaDisk {

  public static class StorageCompProvider implements se.sics.dela.storage.mngr.StorageProvider<DelaStorageComp> {

    public final Identifier self;
    public final DiskEndpoint endpoint = new DiskEndpoint();

    public StorageCompProvider(Identifier self) {
      this.self = self;
    }

    @Override
    public Pair<DelaStorageComp.Init, Long> initiate(StorageResource resource, Logger logger) {
      DiskResource diskResource = (DiskResource) resource;
      DelaStorageProvider storage = new StorageProvider(endpoint, diskResource);
      Try<Long> pos = DelaDisk.fileSize(endpoint, diskResource);
      if (pos.isFailure()) {
        throw new RuntimeException(TryHelper.tryError(pos));
      }
      DelaStorageComp.Init init = new DelaStorageComp.Init(storage, pos.get());
      return Pair.with(init, pos.get());
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

  public static class StorageProvider implements DelaStorageProvider<DiskEndpoint, DiskResource> {

    public final DiskEndpoint endpoint;
    public final DiskResource resource;

    public StorageProvider(DiskEndpoint endpoint, DiskResource resource) {
      this.endpoint = endpoint;
      this.resource = resource;
    }

    @Override
    public DiskEndpoint getEndpoint() {
      return endpoint;
    }

    @Override
    public DiskResource getResource() {
      return resource;
    }

    @Override
    public Try<Boolean> createPath() {
      File dir = new File(resource.dirPath);
      if (dir.isFile()) {
        String msg = "resource parent dir:" + dir.getAbsolutePath() + " is a file";
        return new Try.Failure(new DelaStorageException(msg));
      }
      if (!dir.exists()) {
        dir.mkdirs();
        return new Try.Success(true);
      } else {
        return new Try.Success(false);
      }
    }

    @Override
    public Try<Boolean> fileExists() {
      File f = new File(resource.dirPath, resource.fileName);
      return new Try.Success(f.exists());
    }

    @Override
    public Try<Boolean> createFile() {
      File f = new File(resource.dirPath, resource.fileName);
      try {
        return new Try.Success(f.createNewFile());
      } catch (IOException ex) {
        String msg = "could not create file:" + f.getAbsolutePath();
        return new Try.Failure(new DelaStorageException(msg));
      }
    }

    @Override
    public Try<Boolean> deleteFile() {
      File f = new File(resource.dirPath, resource.fileName);
      return new Try.Success(f.delete());
    }

    @Override
    public Try<Long> fileSize() {
      return DelaDisk.fileSize(endpoint, resource);
    }

    @Override
    public Try<byte[]> read(KRange range) {
      int readLength = (int) (range.upperAbsEndpoint() - range.lowerAbsEndpoint() + 1);
      byte[] readVal = new byte[readLength];
      int readPos = (int) range.lowerAbsEndpoint();
      String f = resource.dirPath + File.separator + resource.fileName;
      try (RandomAccessFile raf = new RandomAccessFile(f, "rw")) {
        raf.seek(readPos);
        raf.readFully(readVal);
        return new Try.Success(readVal);
      } catch (IOException ex) {
        String msg = "could not read file:" + f;
        return new Try.Failure(new DelaStorageException(msg, ex));
      }
    }

    @Override
    public Try<byte[]> readAllFile() {
      Try<byte[]> result = new Try.Success(true)
        .flatMap(TryHelper.tryFSucc0(() -> fileSize()))
        .flatMap(TryHelper.tryFSucc1((Long fileSize) -> {
          String f = resource.dirPath + File.separator + resource.fileName;
          if (fileSize > Integer.MAX_VALUE) {
            String msg = "file:" + f + " is too big to read fully";
            return new Try.Failure(new DelaStorageException(msg));
          }
          int readLength = (int) (long) fileSize;
          byte[] readVal = new byte[readLength];

          try (RandomAccessFile raf = new RandomAccessFile(f, "rw")) {
            raf.readFully(readVal);
            return new Try.Success(readVal);
          } catch (IOException ex) {
            String msg = "could not read file:" + f;
            return new Try.Failure(new DelaStorageException(msg, ex));
          }
        }));
      return result;
    }

    @Override
    public Try<Boolean> append(long pos, byte[] data) {
      String f = resource.dirPath + File.separator + resource.fileName;
      try (RandomAccessFile raf = new RandomAccessFile(f, "rw")) {
        raf.seek(pos);
        raf.write(data);
        return new Try.Success(true);
      } catch (IOException ex) {
        String msg = "could not write file:" + f;
        return new Try.Failure(new DelaStorageException(msg, ex));
      }
    }

    @Override
    public Try<DelaReadStream> readSession(TimerProxy timer) {
      String filePath = resource.dirPath + File.separator + resource.fileName;
      try {
        RandomAccessFile raf = new RandomAccessFile(filePath, "rw");
        return new Try.Success(new ReadStream(filePath, raf));
      } catch (FileNotFoundException ex) {
        String msg = "could not find file:" + filePath;
        return new Try.Failure(new DelaStorageException(msg, ex));
      }
    }

    @Override
    public Try<DelaAppendStream> appendSession(TimerProxy timer) {
      String filePath = resource.dirPath + File.separator + resource.fileName;
      try {
        RandomAccessFile raf = new RandomAccessFile(filePath, "rw");
        return new Try.Success(new AppendStream(filePath, raf));
      } catch (FileNotFoundException ex) {
        String msg = "could not find file:" + filePath;
        return new Try.Failure(new DelaStorageException(msg, ex));
      }
    }
  }

  public static class ReadStream implements DelaReadStream {

    private final String filePath;
    private final RandomAccessFile raf;

    public ReadStream(String filePath, RandomAccessFile raf) {
      this.filePath = filePath;
      this.raf = raf;
    }

    @Override
    public void read(KRange range, Consumer<Try<byte[]>> callback) {
      int readLength = (int) (range.upperAbsEndpoint() - range.lowerAbsEndpoint() + 1);
      byte[] readVal = new byte[readLength];
      int readPos = (int) range.lowerAbsEndpoint();
      try {
        raf.seek(readPos);
        raf.readFully(readVal);
        callback.accept(new Try.Success(readVal));
      } catch (IOException ex) {
        String msg = "could not read file:" + filePath;
        callback.accept(new Try.Failure(new DelaStorageException(msg, ex)));
      }
    }

    @Override
    public Try<Boolean> close() {
      try {
        raf.close();
        return new Try.Success(true);
      } catch (IOException ex) {
        String msg = "closing file:" + filePath;
        return new Try.Failure(new DelaStorageException(msg, ex));
      }
    }
  }

  public static class AppendStream implements DelaAppendStream {

    private final String filePath;
    private final RandomAccessFile raf;

    public AppendStream(String filePath, RandomAccessFile raf) {
      this.filePath = filePath;
      this.raf = raf;
    }

    @Override
    public void write(long pos, byte[] data, Consumer<Try<Boolean>> callback) {
      try {
        raf.seek(pos);
        raf.write(data);
        callback.accept(new Try.Success(true));
      } catch (IOException ex) {
        String msg = "could not write file:" + filePath;
        callback.accept(new Try.Failure(new DelaStorageException(msg, ex)));
      }
    }

    @Override
    public Try<Boolean> close() {
      try {
        raf.close();
        return new Try.Success(true);
      } catch (IOException ex) {
        String msg = "closing file:" + filePath;
        return new Try.Failure(new DelaStorageException(msg, ex));
      }
    }
  }

  public static Try<Long> fileSize(DiskEndpoint endpoint, DiskResource resource) {
    File f = new File(resource.dirPath, resource.fileName);
    return new Try.Success(f.length());
  }
}
