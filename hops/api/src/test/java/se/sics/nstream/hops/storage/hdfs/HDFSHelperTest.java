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
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
 */
package se.sics.nstream.hops.storage.hdfs;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.junit.Test;
import static se.sics.nstream.hops.storage.hdfs.HDFSHelper.DATANODE_FAILURE_POLICY;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class HDFSHelperTest {
//  @Test
  public void test() {
    String xmlConfigPath = "/srv/hops/hadoop/etc/hadoop/core-site.xml";
    String user = "vagrant";
    HDFSEndpoint endpoint = HDFSEndpoint.getXML(xmlConfigPath, user);
  }
  
//  @Test
  public void baseTest() {
    String xmlConfigPath = "/srv/hops/hadoop/etc/hadoop/core-site.xml";
    String user = "vagrant";
    Configuration conf = new Configuration();
    conf.addResource(new Path(xmlConfigPath));
    
    try (DistributedFileSystem fs = (DistributedFileSystem) FileSystem.get(conf)) {
      if (fs.getDataNodeStats().length == 1) {
        fs.close();
        conf.set(DATANODE_FAILURE_POLICY, "NEVER");
      }
    } catch (IOException ex) {
      throw new RuntimeException(ex);
    }
  }
}
