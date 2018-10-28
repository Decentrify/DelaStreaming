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

import se.sics.dela.storage.StorageEndpoint;
import se.sics.dela.storage.StorageResource;
import se.sics.ktoolbox.util.trysf.Try;
import se.sics.ktoolbox.util.trysf.TryHelper;
import se.sics.nstream.hops.manifest.ManifestHelper;
import se.sics.nstream.hops.manifest.ManifestJSON;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class DelaStorage<E extends StorageEndpoint, R extends StorageResource> {

  private final StorageOp ops;

  public DelaStorage(DelaStorageProvider storage) {
    this.ops = new StorageOp(storage);
  }

  public Try<ManifestJSON> readManifest() {
    return new Try.Success(true)
      .flatMap(TryHelper.tryFSucc0(ops.fileExists()))
      .flatMap(TryHelper.tryFSucc1((Boolean fileExists) -> {
        if (fileExists) {
          return new Try.Success(true);
        } else {
          String msg = "manifest file does not exist:" + ops.storage.getResource().getSinkName();
          return new Try.Failure(new DelaStorageException(msg));
        }
      }))
      .flatMap(TryHelper.tryFSucc0(ops.readAllFile()))
      .map(TryHelper.tryFSucc1((byte[] manifestBytes) -> ManifestHelper.getManifestJSON(manifestBytes)));
  }
  
  public Try<Boolean> writeManifest(ManifestJSON manifest) {
    return new Try.Success(true)
      .flatMap(TryHelper.tryFSucc0(ops.fileExists()))
      .flatMap(TryHelper.tryFSucc1((Boolean fileExists) -> {
        if (fileExists) {
          String msg = "manifest already exists:" + ops.storage.getResource().getSinkName();
          return new Try.Failure(new DelaStorageException(msg));
        } else {
          return new Try.Success(true)
            .flatMap(TryHelper.tryFSucc0(ops.createPath()))
            .flatMap(TryHelper.tryFSucc0(ops.createFile()));
        }
      }))
      .flatMap(TryHelper.tryFSucc0(ops.append(ManifestHelper.getManifestByte(manifest))));
  }
  
  public static class DelaStorageException extends Exception {

    public DelaStorageException(String msg, Throwable cause) {
      super(msg, cause);
    }

    public DelaStorageException(String msg) {
      super(msg);
    }
  }
}
