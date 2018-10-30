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
package se.sics.dela.storage.hdfs;

import se.sics.dela.storage.StorageResource;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class HDFSResource implements StorageResource {

  public final String dirPath;
  public final String fileName;

  public HDFSResource(String dirPath, String fileName) {
    this.dirPath = dirPath;
    this.fileName = fileName;
  }

  public HDFSResource withFile(String fileName) {
    return new HDFSResource(dirPath, fileName);
  }

  @Override
  public String getSinkName() {
    return "hdfs://" + dirPath + "/" + fileName;
  }

  @Override
  public String toString() {
    return "HDFSResource{" + "dirPath=" + dirPath + ", fileName=" + fileName + '}';
  }
}
