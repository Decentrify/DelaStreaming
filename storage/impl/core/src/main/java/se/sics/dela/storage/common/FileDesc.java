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

import java.util.function.Supplier;
import se.sics.dela.storage.StorageEndpoint;
import se.sics.dela.storage.StorageResource;
import se.sics.ktoolbox.util.trysf.Try;
import se.sics.nstream.util.range.KRange;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class FileDesc<E extends StorageEndpoint, R extends StorageResource> {
  public final StorageOp<E, R> storageOp;
  public final E endpoint;
  public final R resource;
  

  public FileDesc(DelaStorageProvider<E, R> storage, E endpoint, R resource) {
    this.storageOp = new StorageOp(storage);
    this.endpoint = endpoint;
    this.resource = resource;
  }

  public Supplier<Try<Boolean>> createPath() {
    return storageOp.createPath(endpoint, resource);
  }

  public Supplier<Try<Boolean>> exists() {
    return storageOp.fileExists(endpoint, resource);
  }

  public Supplier<Try<Boolean>> create() {
    return storageOp.createFile(endpoint, resource);
  }
  
  public Supplier<Try<Boolean>> delete() {
    return storageOp.deleteFile(endpoint, resource);
  }

  public Supplier<Try<Long>> size() {
    return storageOp.fileSize(endpoint, resource);
  }

  public Supplier<Try<byte[]>> read(KRange range) {
    return storageOp.read(endpoint, resource, range);
  }
  
  public Supplier<Try<byte[]>> readAll() {
    return storageOp.readAllFile(endpoint, resource);
  }

  public Supplier<Try<Boolean>> append(byte[] data) {
    return storageOp.append(endpoint, resource, data);
  }
}
