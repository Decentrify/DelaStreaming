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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.security.UserGroupInformation;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class HDFSHelperTest {

//  @Test
  public void baseTest2() throws IOException {
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
