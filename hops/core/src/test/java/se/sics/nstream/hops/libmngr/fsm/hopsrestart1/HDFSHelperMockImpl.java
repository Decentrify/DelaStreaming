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
package se.sics.nstream.hops.libmngr.fsm.hopsrestart1;

import com.google.gson.Gson;
import com.google.gson.stream.JsonReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import org.apache.hadoop.security.UserGroupInformation;
import se.sics.ktoolbox.util.result.Result;
import se.sics.nstream.hops.hdfs.HDFSHelperMock;
import se.sics.nstream.hops.manifest.ManifestJSON;
import se.sics.nstream.hops.storage.hdfs.HDFSEndpoint;
import se.sics.nstream.hops.storage.hdfs.HDFSResource;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class HDFSHelperMockImpl implements HDFSHelperMock {
  private final String manifestFile;
  
  public HDFSHelperMockImpl(String manifestFile) {
    this.manifestFile = manifestFile;
  }

  @Override
  public Result<ManifestJSON> readManifest(UserGroupInformation ugi, HDFSEndpoint hdfsEndpoint,
    HDFSResource hdfsResource) {
    try {
      JsonReader reader = new JsonReader(new FileReader(manifestFile));
      Gson gson = new Gson();
      ManifestJSON manifest = gson.fromJson(reader, ManifestJSON.class);
      return Result.success(manifest);
    } catch (FileNotFoundException ex) {
      throw new RuntimeException(ex);
    }
  }
}
