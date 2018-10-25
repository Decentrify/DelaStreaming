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
import java.util.function.BiFunction;
import java.util.function.Supplier;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.Assert;
import org.junit.Test;
import se.sics.dela.storage.StorageEndpoint;
import se.sics.dela.storage.StorageResource;
import se.sics.dela.storage.hdfs.HDFSEndpoint;
import se.sics.dela.storage.hdfs.HDFSHelper;
import static se.sics.dela.storage.hdfs.HDFSHelper.createPathOp;
import se.sics.dela.storage.hdfs.HDFSResource;
import se.sics.ktoolbox.util.trysf.Try;
import static se.sics.ktoolbox.util.trysf.TryHelper.tryAssert;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class DelaStorageTest {

  @Test
  public void testHDFS() {
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
      .flatMap(doAs.wrapOp(writeFile(storage, endpoint, resource)))
      .flatMap(doAs.wrapOp(file.size()))
      .map(tryAssert((Long size) -> Assert.assertEquals(1024*1024*1024l, (long) size)))
      .flatMap(doAs.wrapOp(file.delete()));
    result.get();
  }

  private Supplier<Try<Boolean>> writeFile(
    DelaStorageProvider storage, StorageEndpoint endpoint, StorageResource resource) {
    return () -> {
      byte[] block = new byte[1024 * 1024];
      Random rand = new Random(123);
      rand.nextBytes(block);

      long start = System.nanoTime();
      for (int i = 0; i < 1024; i++) {
        storage.append(endpoint, resource, block);
      }
      long end = System.nanoTime();
      System.err.println("time:" + (end - start) / (1000 * 1000));
      return new Try.Success(true);
    };
  }
}
