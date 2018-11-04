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
package se.sics.dela.storage.common;

import com.google.cloud.storage.Storage;
import com.google.common.util.concurrent.SettableFuture;
import java.io.IOException;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.junit.Assert;
import org.junit.Before;
import se.sics.dela.storage.StorageEndpoint;
import se.sics.dela.storage.StorageResource;
import se.sics.dela.storage.aws.AWSConfig;
import se.sics.dela.storage.aws.AWSEndpoint;
import se.sics.dela.storage.aws.AWSResource;
import se.sics.dela.storage.aws.DelaAWS;
import se.sics.dela.storage.aws.AWSRegionsConverter;
import se.sics.dela.storage.disk.DelaDisk;
import se.sics.dela.storage.disk.DiskEndpoint;
import se.sics.dela.storage.disk.DiskResource;
import se.sics.dela.storage.gcp.DelaGCP;
import se.sics.dela.storage.gcp.GCPConfig;
import se.sics.dela.storage.gcp.GCPEndpoint;
import se.sics.dela.storage.gcp.GCPResource;
import se.sics.dela.storage.hdfs.HDFSEndpoint;
import se.sics.dela.storage.hdfs.DelaHDFS;
import se.sics.dela.storage.hdfs.HDFSResource;
import se.sics.dela.util.TimerProxy;
import se.sics.kompics.ComponentProxy;
import se.sics.kompics.config.Conversions;
import se.sics.kompics.config.TypesafeConfig;
import se.sics.ktoolbox.util.trysf.Try;
import se.sics.ktoolbox.util.trysf.TryHelper;
import static se.sics.ktoolbox.util.trysf.TryHelper.tryAssert;
import se.sics.nstream.util.range.KBlockImpl;
import se.sics.nstream.util.range.KRange;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class DelaStorageTest {

  private static final int BLOCKS = 2;

  @Before
  public void setup() {
    Conversions.register(new AWSRegionsConverter());
  }

  //************************************************AWS***************************************************************
  //aws multiple single append do not make sense since once write channel is close, the blobs become immutable
//  @Test
  public void testAWSMultiAppend() throws IOException, Throwable {
    System.err.println("aws");
    ExecutorService executors = Executors.newFixedThreadPool(5);
    AWSConfig config = AWSConfig.read(TypesafeConfig.load()).checkedGet();
    AWSEndpoint endpoint = new AWSEndpoint(config.credentials, config.bucket, config.region);
    AWSResource resource = new AWSResource(config.bucket, "/", "test", "file");
    DelaAWS.StorageHandler storage = DelaAWS.StorageHandler.instance(executors, config.credentials, endpoint);
    testMultiAppend(storage, resource, BLOCKS, 10 * 1024 * 1024);
  }

//  @Test
  public void testAWSMultiRead() throws IOException, Throwable {
    System.err.println("aws");
    ExecutorService executors = Executors.newFixedThreadPool(5);
    AWSConfig config = AWSConfig.read(TypesafeConfig.load()).checkedGet();
    AWSEndpoint endpoint = new AWSEndpoint(config.credentials, config.bucket, config.region);
    AWSResource resource = new AWSResource(config.bucket, "/", "test", "file");
    DelaAWS.StorageHandler storage = DelaAWS.StorageHandler.instance(executors, config.credentials, endpoint);
    testMultiRead(storage, resource, BLOCKS, 10 * 1024 * 1024);
  }

  //************************************************GCP***************************************************************
  //gcp multiple single append do not make sense since once write channel is close, the blobs become immutable
//  @Test
  public void testGCPMultiAppend() throws IOException, Throwable {
    System.err.println("gcp");
    GCPConfig config = GCPConfig.read(TypesafeConfig.load()).checkedGet();
    GCPEndpoint endpoint = new GCPEndpoint(config.credentials, config.project, config.bucket);
    GCPResource resource = new GCPResource(config.bucket, "/", "test", "file");
    Storage gcp = DelaGCP.getStorage(config.credentials, config.project);
    DelaGCP.StorageHandler storage = new DelaGCP.StorageHandler(gcp, endpoint);
    testMultiAppend(storage, resource, BLOCKS, 10 * 1024 * 1024);
  }

//  @Test
  public void testGCPMultiRead() throws IOException, Throwable {
    System.err.println("gcp");
    GCPConfig config = GCPConfig.read(TypesafeConfig.load()).checkedGet();
    GCPEndpoint endpoint = new GCPEndpoint(config.credentials, config.project, config.bucket);
    GCPResource resource = new GCPResource(config.bucket, "/", "test", "file");
    Storage gcp = DelaGCP.getStorage(config.credentials, config.project);
    DelaGCP.StorageHandler storage = new DelaGCP.StorageHandler(gcp, endpoint);
    testMultiRead(storage, resource, BLOCKS, 10 * 1024 * 1024);
  }

  //************************************************HDFS****************************************************************
//    @Test
  public void testHDFSMultiAppend() throws IOException, Throwable {
    System.err.println("hdfs");
    HDFSEndpoint endpoint = HDFSEndpoint.getBasic("vagrant", "10.0.2.15", 8020).get();
    HDFSResource resource = new HDFSResource("/test", "file");
    DelaHDFS.StorageHandler storage = DelaHDFS.StorageHandler.handler(endpoint).checkedGet();
    testMultiAppend(storage, resource, BLOCKS, 10 * 1024 * 1024);
  }

//    @Test
  public void testHDFSMultiRead() throws IOException, Throwable {
    System.err.println("hdfs");
    HDFSEndpoint endpoint = HDFSEndpoint.getBasic("vagrant", "10.0.2.15", 8020).get();
    HDFSResource resource = new HDFSResource("/test", "file");
    DelaHDFS.StorageHandler storage = DelaHDFS.StorageHandler.handler(endpoint).checkedGet();
    testMultiRead(storage, resource, BLOCKS, 10 * 1024 * 1024);
  }

  //***************************************************DISK*************************************************************
//  @Test
  public void testDiskMultiAppend() throws Throwable {
    System.err.println("disk");
    DiskEndpoint endpoint = new DiskEndpoint();
    DiskResource resource = new DiskResource("src/test/resources", "disk_test_file");
    DelaDisk.StorageHandler storage = new DelaDisk.StorageHandler(endpoint);
    testMultiAppend(storage, resource, BLOCKS, 10 * 1024 * 1024);
  }

//  @Test
  public void testDiskMultiRead() throws Throwable {
    System.err.println("disk");
    DiskEndpoint endpoint = new DiskEndpoint();
    DiskResource resource = new DiskResource("src/test/resources", "disk_test_file");
    DelaDisk.StorageHandler storage = new DelaDisk.StorageHandler(endpoint);
    testMultiRead(storage, resource, BLOCKS, 10 * 1024 * 1024);
  }

  private <E extends StorageEndpoint, R extends StorageResource> DelaFileHandler<E, R> prepareFile(
    DelaStorageHandler<E, R> storage, R resource) throws Throwable {
    Try<DelaFileHandler> tryFile = new Try.Success(true)
      .flatMap(TryHelper.tryTSucc0(() -> storage.delete(resource)))
      .flatMap(TryHelper.tryTSucc0(() -> storage.create(resource)));
    DelaFileHandler file = tryFile.checkedGet();
    file.setTimerProxy(new TestTimer());
    return file;
  }

  private <E extends StorageEndpoint, R extends StorageResource> void testMultiAppend(
    DelaStorageHandler<E, R> storage, R resource, int blocks, int blockSize) throws Throwable {
    System.err.println("test multi write");
    DelaFileHandler<E, R> file = prepareFile(storage, resource);
    Try<Boolean> result = new Try.Success(true)
      .flatMap(TryHelper.tryTSucc0(multiAppendFile(file, blocks, blockSize)))
      .flatMap(TryHelper.tryTSucc0(() -> file.size()))
      .map(tryAssert((Long size) -> Assert.assertEquals(1l * blocks * blockSize, (long) size)))
      .flatMap(TryHelper.tryTSucc0(() -> storage.delete(resource)));
    result.checkedGet();
  }

  private <E extends StorageEndpoint, R extends StorageResource> Supplier<Try<Boolean>> multiAppendFile(
    DelaFileHandler<E, R> file, int blocks, int blockSize) {
    return () -> {
      byte[] block = new byte[blockSize];
      Random rand = new Random(123);
      rand.nextBytes(block);

      SettableFuture<Try<Boolean>> completedFuture = SettableFuture.create();
      Consumer<Try<Boolean>> completed = (Try<Boolean> result) -> completedFuture.set(result);
      
      Consumer<Try<Boolean>> appendCallbacks = new Consumer<Try<Boolean>>() {
        int currentAppend = 0;

        @Override
        public void accept(Try<Boolean> result) {
          if (result.isFailure()) {
            throw new RuntimeException(TryHelper.tryError(result));
          } else {
            System.err.println("append:" + currentAppend);
            currentAppend++;
          }
        }
      };

      long start = System.nanoTime();
      try (DelaAppendStream stream = file.appendStream(1l * blocks * blockSize, completed).checkedGet()) {
        for (int i = 0; i < blocks; i++) {
          stream.write(i * blockSize, block, appendCallbacks);
        }
        System.err.println("waiting");
        Try<Boolean> result = completedFuture.get();
      } catch (Throwable ex) {
      }

      long end = System.nanoTime();
      long sizeMB = 1l * blocks * blockSize / (1024 * 1024);
      double time = (double) (end - start) / (1000 * 1000 * 1000);
      System.err.println("multiwrite time(s):" + time);
      System.err.println("multiwrite avg speed(MB/s):" + sizeMB / time);
      return new Try.Success(true);
    };
  }

  private <E extends StorageEndpoint, R extends StorageResource> void testMultiRead(
    DelaStorageHandler<E, R> storage, R resource, int blocks, int blockSize)
    throws Throwable {
    System.err.println("test multi read");
    DelaFileHandler<E, R> file = prepareFile(storage, resource);
    Try<Boolean> result = new Try.Success(true)
      .flatMap(TryHelper.tryTSucc0(multiAppendFile(file, blocks, blockSize)))
      .flatMap(TryHelper.tryTSucc0(multiReadFile(file, blocks, blockSize)))
      .flatMap(TryHelper.tryTSucc0(() -> file.size()))
      .map(tryAssert((Long size) -> Assert.assertEquals(1l * blocks * blockSize, (long) size)))
      .flatMap(TryHelper.tryTSucc0(() -> storage.delete(resource)));
    result.checkedGet();
  }

  private <E extends StorageEndpoint, R extends StorageResource> Supplier<Try<Boolean>> multiReadFile(
    DelaFileHandler<E, R> storage, int blocks, int blockSize) {
    return () -> {
      Consumer<Try<byte[]>> callback = new Consumer<Try<byte[]>>() {
        int currentRead = 0;

        @Override
        public void accept(Try<byte[]> result) {
          if (result.isFailure()) {
            throw new RuntimeException(TryHelper.tryError(result));
          } else {
            System.err.println("read:" + currentRead);
            currentRead += 1;
          }
        }
      };
      long start = System.nanoTime();
      try (DelaReadStream stream = storage.readStream().checkedGet()) {
        for (int i = 0; i < blocks; i++) {
          KRange range = new KBlockImpl(i, i * blockSize, (i + 1) * blockSize - 1);
          stream.read(range, callback);
        }
      } catch (Throwable ex) {
        return new Try.Failure(ex);
      }
      long end = System.nanoTime();
      long sizeMB = 1l * blocks * blockSize / (1024 * 1024);
      double time = (double) (end - start) / (1000 * 1000 * 1000);
      System.err.println("multiread time(s):" + time);
      System.err.println("multiread avg speed(MB/s):" + sizeMB / time);
      return new Try.Success(true);
    };
  }

  public static class TestTimer implements TimerProxy {

    @Override
    public TimerProxy setup(ComponentProxy proxy) {
      return this;
    }

    @Override
    public UUID schedulePeriodicTimer(long delay, long period, Consumer<Boolean> callback) {
      return UUID.randomUUID();
    }

    @Override
    public void cancelPeriodicTimer(UUID timeoutId) {
    }
  }
}
