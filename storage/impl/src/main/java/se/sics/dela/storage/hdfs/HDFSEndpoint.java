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

import java.io.File;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import se.sics.dela.storage.StorageEndpoint;
import static se.sics.dela.storage.hdfs.HDFSHelper.HOPS_URL;
import se.sics.ktoolbox.util.trysf.Try;
import se.sics.ktoolbox.util.trysf.TryHelper;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class HDFSEndpoint implements StorageEndpoint {

  public final Configuration hdfsConfig;
  public final String hopsURL;
  public final String user;

  public HDFSEndpoint(Configuration hdfsConfig, String user) {
    this.hdfsConfig = hdfsConfig;
    this.user = user;
    this.hopsURL = hdfsConfig.get(HDFSHelper.HOPS_URL);
  }

  @Override
  public String getEndpointName() {
    return hopsURL + "_" + user;
  }

  @Override
  public String toString() {
    return "HDFSEndpoint{" + "hopsURL=" + hopsURL + ", user=" + user + '}';
  }

  public static Try<HDFSEndpoint> getBasic(String user, String hopsIp, int hopsPort) {
    String hopsURL = "hdfs://" + hopsIp + ":" + hopsPort;
    return getBasic(hopsURL, user);
  }

  public static Try<HDFSEndpoint> getBasic(String hopsURL, String user) {
    Configuration conf = new Configuration();
    conf.set(HOPS_URL, hopsURL);
    return HDFSHelper.fixConfig(conf)
      .map(TryHelper.tryFSucc1((Configuration config) -> new HDFSEndpoint(config, user)));
  }

  public static Try<HDFSEndpoint> getXML(String hdfsXMLPath, String user) {
    if (!new File(hdfsXMLPath).exists()) {
      throw new RuntimeException("conf file does not exist:" + hdfsXMLPath);
    }
    Configuration conf = new Configuration();
    conf.addResource(new Path(hdfsXMLPath));
    return HDFSHelper.fixConfig(conf)
      .map(TryHelper.tryFSucc1((Configuration config) -> new HDFSEndpoint(config, user)));
  }
}
