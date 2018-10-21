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
package se.sics.nstream.hops.storage.hdfs;

import java.io.File;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import se.sics.ktoolbox.util.trysf.Try;
import se.sics.ktoolbox.util.trysf.TryHelper;
import static se.sics.nstream.hops.storage.hdfs.HDFSHelper.HOPS_URL;
import se.sics.nstream.storage.durable.util.StreamEndpoint;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class HDFSEndpoint implements StreamEndpoint {

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

  public static HDFSEndpoint getBasic(String user, String hopsIp, int hopsPort) {
    String hopsURL = "hdfs://" + hopsIp + ":" + hopsPort;
    return getBasic(hopsURL, user);
  }

  public static HDFSEndpoint getBasic(String hopsURL, String user) {
    Configuration conf = new Configuration();
    conf.set(HOPS_URL, hopsURL);
    Try<HDFSEndpoint> endpoint = HDFSHelper.fixConfig(conf)
      .map(TryHelper.tryFSucc1((Configuration config) -> new HDFSEndpoint(config, user)));
    try {
      return endpoint.checkedGet();
    } catch (Throwable ex) {
      throw new RuntimeException(ex);
    }
  }

  public static Try<HDFSEndpoint> getXML(String hdfsXMLPath, String user) {
    if (!new File(hdfsXMLPath).exists()) {
      return new Try.Failure(new HDFSException("conf file does not exist:" + hdfsXMLPath));
    }
    Configuration conf = new Configuration();
    conf.addResource(new Path(hdfsXMLPath));
    return HDFSHelper.fixConfig(conf)
      .map(TryHelper.tryFSucc1((Configuration config) -> new HDFSEndpoint(config, user)));

  }
}
