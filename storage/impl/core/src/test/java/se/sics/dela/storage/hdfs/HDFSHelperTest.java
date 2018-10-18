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

import java.io.IOException;
import java.util.Random;
import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.Assert;
import org.junit.Test;
import se.sics.dela.storage.hdfs.HDFSHelper.DoAs;
import se.sics.dela.storage.hdfs.HDFSHelper.FileDesc;
import se.sics.ktoolbox.util.trysf.Try;
import static se.sics.dela.storage.hdfs.HDFSHelper.createDirs;
import se.sics.ktoolbox.util.trysf.TryHelper;
import static se.sics.ktoolbox.util.trysf.TryHelper.tryAssert;
import se.sics.nstream.util.range.KBlockImpl;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class HDFSHelperTest {

//  @Test
  public void test() throws IOException {
    HDFSEndpoint endpoint = HDFSEndpoint.getBasic("vagrant", "10.0.2.15", 8020);
    HDFSResource resource = new HDFSResource("/test", "file3");
    UserGroupInformation ugi = UserGroupInformation.createRemoteUser(endpoint.user);

    FileDesc file = new FileDesc(endpoint, resource);

    DoAs doAs = new DoAs(ugi);
    Try<Boolean> result = new Try.Success(true)
      .flatMap(doAs.wrap(file.deleteOp()))
      .flatMap(doAs.wrap(file.sizeOp()))
      .map(tryAssert((Long size) -> Assert.assertEquals(-1l, (long) size)))
      .flatMap(doAs.wrap(createDirs(endpoint, resource)))
      .flatMap(doAs.wrap(file.createOp()))
      .flatMap(doAs.wrap(file.sizeOp()))
      .map(tryAssert((Long size) -> Assert.assertEquals(0l, (long) size)))
      .flatMap(doAs.wrap(writeFile(file, 2, 1024)))
      .flatMap(doAs.wrap(file.sizeOp()))
      .map(tryAssert((Long size) -> Assert.assertEquals(2 * 1024l, (long) size)))
      .flatMap(doAs.wrap(file.readOp(new KBlockImpl(0, 0l, 1 * 1024l))))
      .map(tryAssert((byte[] block) -> Assert.assertEquals(1 * 1024l, block.length)))
      .flatMap(doAs.wrap(file.deleteOp()));
    result.get();
  }

  private Supplier<Try<Boolean>> writeFile(FileDesc file, int nrBlocks, int blockSize) {
    return () -> {
      Random rand = new Random(123);
      for (int i = 0; i < nrBlocks; i++) {
        byte[] block = new byte[blockSize];
        rand.nextBytes(block);
        Try<Boolean> result = file.append(block);
        try {
          Thread.sleep(1000);
        } catch (InterruptedException ex) {
          Logger.getLogger(HDFSHelperTest.class.getName()).log(Level.SEVERE, null, ex);
        }
        if (result.isFailure()) {
          return result;
        }
      }
      return new Try.Success(true);
    };
  }

  @Test
  public void tes2() throws IOException {
    Configuration conf = new Configuration();
    conf.set("fs.defaultFS", "hdfs://10.0.2.15:8020");
    UserGroupInformation ugi = UserGroupInformation.createRemoteUser("vagrant");
    DistributedFileSystem fs = (DistributedFileSystem) FileSystem.get(conf);
    
    String filePath = "/test/file";
    fs.delete(new Path(filePath), true);
    fs.create(new Path(filePath)).close();
    
    FSDataOutputStream out = fs.append(new Path(filePath));
    out.write(1);
    out.write(2);
    out.write(3);
    out.close();
    
    out = fs.append(new Path(filePath));
    out.write(1);
    //test fails here due to "dfs.client.block.write.replace-datanode-on-failure.policy"
    //setting this policy to NEVER makes the test pass
    out.close();
    
    fs.close();
  }
}
