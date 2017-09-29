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
package se.sics.nstream.hops.hdfs;

import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.ktoolbox.util.result.Result;
import se.sics.nstream.hops.manifest.ManifestJSON;
import se.sics.nstream.hops.storage.hdfs.HDFSEndpoint;
import se.sics.nstream.hops.storage.hdfs.HDFSResource;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class HDFSHelper {

  private final static Logger LOG = LoggerFactory.getLogger(HDFSHelper.class);
  private static String logPrefix = "";
  
  public static HDFSHelperMock mock = null;
  
  public static Result<ManifestJSON> readManifest(UserGroupInformation ugi, final HDFSEndpoint hdfsEndpoint, HDFSResource hdfsResource) {
    LOG.info("{}loading manifest from endpoint:{} resource:{}", new Object[]{logPrefix, hdfsEndpoint, hdfsResource});
    return mock.readManifest(ugi, hdfsEndpoint, hdfsResource);
  }
  
  public static Result<Boolean> writeManifest(UserGroupInformation ugi, final HDFSEndpoint hdfsEndpoint, final HDFSResource hdfsResource, final ManifestJSON manifest) {
    LOG.info("{}writting manifest to endpoint:{} resource:{}", new Object[]{logPrefix, hdfsEndpoint, hdfsResource});
    return mock.writeManifest(ugi, hdfsEndpoint, hdfsResource, manifest);
  }
}
