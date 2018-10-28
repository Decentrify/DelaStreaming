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
package se.sics.dela.storage.common;

import java.util.Random;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.Assert;
import org.junit.Test;
import se.sics.dela.storage.StorageEndpoint;
import se.sics.dela.storage.StorageResource;
import se.sics.dela.storage.hdfs.HDFSEndpoint;
import se.sics.dela.storage.hdfs.HDFSHelper;
import static se.sics.dela.storage.hdfs.HDFSHelper.createPathOp;
import se.sics.dela.storage.hdfs.HDFSResource;
import se.sics.dela.util.TimerProxy;
import se.sics.kompics.ComponentProxy;
import se.sics.ktoolbox.util.trysf.Try;
import static se.sics.ktoolbox.util.trysf.TryHelper.tryAssert;
import se.sics.nstream.util.range.KBlockImpl;
import se.sics.nstream.util.range.KRange;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class DelaStorageTest {

//  @Test
  public void testHDFSAppend() {
    HDFSEndpoint endpoint = HDFSEndpoint.getBasic("vagrant", "10.0.2.15", 8020).get();
    HDFSResource resource = new HDFSResource("/test", "file");
    UserGroupInformation ugi = UserGroupInformation.createRemoteUser(endpoint.user);

    DelaStorageProvider storage = new HDFSHelper.StorageProvider();
    FileDesc file = new FileDesc(new HDFSHelper.StorageProvider(), endpoint, resource);

    HDFSHelper.DoAs doAs = new HDFSHelper.DoAs(ugi);
    Try<Boolean> result = new Try.Success(true)
      .flatMap(doAs.wrapOp(file.delete()))
      .flatMap(doAs.wrapOp(createPathOp(endpoint, resource)))
      .flatMap(doAs.wrapOp(file.create()))
      .flatMap(doAs.wrapOp(appendFile(storage, endpoint, resource)))
      .flatMap(doAs.wrapOp(file.size()))
      .map(tryAssert((Long size) -> Assert.assertEquals(10 * 10 * 1024 * 1024l, (long) size)))
      .flatMap(doAs.wrapOp(file.delete()));
    result.get();
  }

  private Supplier<Try<Boolean>> appendFile(
    DelaStorageProvider storage, StorageEndpoint endpoint, StorageResource resource) {
    return () -> {
      byte[] block = new byte[10 * 1024 * 1024];
      Random rand = new Random(123);
      rand.nextBytes(block);

      long start = System.nanoTime();
      for (int i = 0; i < 10; i++) {
        storage.append(endpoint, resource, block);
      }
      long end = System.nanoTime();
      long sizeMB = 10 * 10;
      double time = (double) (end - start) / (1000 * 1000 * 1000);
      System.err.println("write time(s):" + time);
      System.err.println("write avg speed(MB/s):" + sizeMB / time);
      return new Try.Success(true);
    };
  }

  @Test
  public void testHDFSMultiAppend() {
    HDFSEndpoint endpoint = HDFSEndpoint.getBasic("vagrant", "10.0.2.15", 8020).get();
    HDFSResource resource = new HDFSResource("/test", "file");
    UserGroupInformation ugi = UserGroupInformation.createRemoteUser(endpoint.user);

    DelaStorageProvider storage = new HDFSHelper.StorageProvider();
    FileDesc file = new FileDesc(new HDFSHelper.StorageProvider(), endpoint, resource);

    HDFSHelper.DoAs doAs = new HDFSHelper.DoAs(ugi);
    Try<Boolean> result = new Try.Success(true)
      .flatMap(doAs.wrapOp(file.delete()))
      .flatMap(doAs.wrapOp(createPathOp(endpoint, resource)))
      .flatMap(doAs.wrapOp(file.create()))
      .flatMap(doAs.wrapOp(multiAppendFile(storage, endpoint, resource)))
      .flatMap(doAs.wrapOp(file.size()))
      .map(tryAssert((Long size) -> Assert.assertEquals(10 * 100 * 1024 * 1024l, (long) size)))
      .flatMap(doAs.wrapOp(file.delete()));
    result.get();
  }

  private Supplier<Try<Boolean>> multiAppendFile(
    DelaStorageProvider storage, StorageEndpoint endpoint, StorageResource resource) {
    return () -> {
      byte[] block = new byte[10 * 1024 * 1024];
      Random rand = new Random(123);
      rand.nextBytes(block);

      long start = System.nanoTime();
      Try<HDFSHelper.AppendSession> appendOp = storage.appendSession((HDFSEndpoint) endpoint,
        (HDFSResource) resource, new TestTimer());
      if (appendOp.isFailure()) {
        return (Try.Failure) appendOp;
      }
      for (int i = 0; i < 100; i++) {
        System.err.println(i);
        appendOp.get().append(block, new AppendCallback());
      }
      Try<Boolean> close = appendOp.get().close();
      if (close.isFailure()) {
        return (Try.Failure) close;
      }
      long end = System.nanoTime();
      long sizeMB = 100 * 10;
      double time = (double) (end - start) / (1000 * 1000 * 1000);
      System.err.println("multiwrite time(s):" + time);
      System.err.println("multiwrite avg speed(MB/s):" + sizeMB / time);
      return new Try.Success(true);
    };
  }

//  @Test
  public void testHDFSMultiRead() {
    HDFSEndpoint endpoint = HDFSEndpoint.getBasic("vagrant", "10.0.2.15", 8020).get();
    HDFSResource resource = new HDFSResource("/test", "file");
    UserGroupInformation ugi = UserGroupInformation.createRemoteUser(endpoint.user);

    DelaStorageProvider storage = new HDFSHelper.StorageProvider();
    FileDesc file = new FileDesc(new HDFSHelper.StorageProvider(), endpoint, resource);

    HDFSHelper.DoAs doAs = new HDFSHelper.DoAs(ugi);
    Try<Boolean> result = new Try.Success(true)
      .flatMap(doAs.wrapOp(file.delete()))
      .flatMap(doAs.wrapOp(createPathOp(endpoint, resource)))
      .flatMap(doAs.wrapOp(file.create()))
      .flatMap(doAs.wrapOp(multiAppendFile(storage, endpoint, resource)))
      .flatMap(doAs.wrapOp(multiReadFile(storage, endpoint, resource)))
      .flatMap(doAs.wrapOp(file.size()))
      .map(tryAssert((Long size) -> Assert.assertEquals(10 * 100 * 1024 * 1024l, (long) size)))
      .flatMap(doAs.wrapOp(file.delete()));
    result.get();
  }

  private Supplier<Try<Boolean>> multiReadFile(
    DelaStorageProvider storage, StorageEndpoint endpoint, StorageResource resource) {
    return () -> {
      long start = System.nanoTime();
      Try<HDFSHelper.ReadSession> appendSession = storage.readSession((HDFSEndpoint) endpoint,
        (HDFSResource) resource, new TestTimer());
      if (appendSession.isFailure()) {
        return (Try.Failure) appendSession;
      }
      for (int i = 0; i < 1000; i++) {
        KRange range = new KBlockImpl(i, i * 1024 * 1024, (i + 1) * 1024 * 1024 - 1);
        appendSession.get().read(range, new ReadCallback());
      }
      Try<Boolean> close = appendSession.get().close();
      if (close.isFailure()) {
        return (Try.Failure) close;
      }
      long end = System.nanoTime();
      long sizeMB = 1000;
      double time = (double) (end - start) / (1000 * 1000 * 1000);
      System.err.println("multiread time(s):" + time);
      System.err.println("multiread avg speed(MB/s):" + sizeMB / time);
      return new Try.Success(true);
    };
  }

//  @Test
  public void testHDFSRead() {
    HDFSEndpoint endpoint = HDFSEndpoint.getBasic("vagrant", "10.0.2.15", 8020).get();
    HDFSResource resource = new HDFSResource("/test", "file");
    UserGroupInformation ugi = UserGroupInformation.createRemoteUser(endpoint.user);

    DelaStorageProvider storage = new HDFSHelper.StorageProvider();
    FileDesc file = new FileDesc(new HDFSHelper.StorageProvider(), endpoint, resource);

    HDFSHelper.DoAs doAs = new HDFSHelper.DoAs(ugi);
    Try<Boolean> result = new Try.Success(true)
      .flatMap(doAs.wrapOp(file.delete()))
      .flatMap(doAs.wrapOp(createPathOp(endpoint, resource)))
      .flatMap(doAs.wrapOp(file.create()))
      .flatMap(doAs.wrapOp(multiAppendFile(storage, endpoint, resource)))
      .flatMap(doAs.wrapOp(readFile(storage, endpoint, resource)))
      .flatMap(doAs.wrapOp(file.size()))
      .map(tryAssert((Long size) -> Assert.assertEquals(10 * 100 * 1024 * 1024l, (long) size)))
      .flatMap(doAs.wrapOp(file.delete()));
    result.get();
  }

  private Supplier<Try<Boolean>> readFile(
    DelaStorageProvider storage, StorageEndpoint endpoint, StorageResource resource) {
    return () -> {
      long start = System.nanoTime();
      for (int i = 0; i < 100; i++) {
        KRange range = new KBlockImpl(i, i * 1024 * 1024, (i + 1) * 1024 * 1024 - 1);
        storage.read(endpoint, resource, range);
      }
      long end = System.nanoTime();
      long sizeMB = 100;
      double time = (double) (end - start) / (1000 * 1000 * 1000);
      System.err.println("read time(s):" + time);
      System.err.println("read avg speed(MB/s):" + sizeMB / time);
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

  public static class AppendCallback implements Consumer<Try<Boolean>> {

    @Override
    public void accept(Try<Boolean> t) {
      if (t.isFailure()) {
        try {
          t.checkedGet();
        } catch (Throwable ex) {
          throw new RuntimeException(ex);
        }
      }
    }
  }

  public static class ReadCallback implements Consumer<Try<byte[]>> {

    @Override
    public void accept(Try<byte[]> t) {
    }
  }
}
