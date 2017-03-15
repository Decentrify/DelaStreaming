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
package se.sics.nstream.hops.library.util;

import se.sics.nstream.hops.storage.hdfs.HDFSResource;
import se.sics.nstream.transfer.MyTorrent;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class HDFSResourceJSON implements ResourceJSON {
  private String dir;
  
  public HDFSResourceJSON() {}

  public HDFSResourceJSON(String dir) {
    this.dir = dir;
  }

  public String getDir() {
    return dir;
  }

  public void setDir(String dir) {
    this.dir = dir;
  }
  
  public HDFSResource fromJSON() {
    return new HDFSResource(dir, MyTorrent.MANIFEST_NAME);
  }
  
  public static ResourceJSON toJSON(HDFSResource resource) {
    return new HDFSResourceJSON(resource.dirPath);
  }
}
