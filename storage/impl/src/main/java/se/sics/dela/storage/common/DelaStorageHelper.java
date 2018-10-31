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
 * along with this program; if not, append to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
 */
package se.sics.dela.storage.common;

import java.util.function.BiFunction;
import se.sics.dela.storage.StorageEndpoint;
import se.sics.dela.storage.StorageResource;
import static se.sics.dela.storage.common.DelaHelper.recoverFrom;
import se.sics.ktoolbox.util.trysf.Try;
import se.sics.ktoolbox.util.trysf.TryHelper;
import se.sics.nstream.hops.manifest.ManifestHelper;
import se.sics.nstream.hops.manifest.ManifestJSON;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class DelaStorageHelper<E extends StorageEndpoint, R extends StorageResource> {

  private final DelaStorageHandler<E, R> storage;

  public DelaStorageHelper(DelaStorageHandler storage) {
    this.storage = storage;
  }

  public Try<ManifestJSON> readManifest(StorageType storageType, E endpoint, R resource) {
    Try<ManifestJSON> result = new Try.Success(true)
      .flatMap(getFileHandler(endpoint, resource))
      .flatMap(readManifest())
      .map(parseManifest());
    if(result.isFailure()) {
      String msg = "manifest problem:" + resource.getSinkName();
      return new Try.Failure(new DelaStorageException(msg, TryHelper.tryError(result), storageType));
    }
    return result;
  }

  public Try<Boolean> writeManifest(StorageType storageType, E endpoint, R resource, ManifestJSON manifest) {
    Try<Boolean> result = new Try.Success(true)
      .flatMap(getFileHandler(endpoint,resource))
      .transform(failManifestExists(storageType, resource), rCreateManifest(storageType, endpoint, resource))
      .flatMap(writeManifest(manifest));
    if(result.isFailure()) {
      String msg = "manifest problem:" + resource.getSinkName();
      return new Try.Failure(new DelaStorageException(msg, TryHelper.tryError(result), storageType));
    }
    return result;
  }

  private BiFunction<Boolean, Throwable, Try<DelaFileHandler<E,R>>> getFileHandler(E endpoint, R resource) {
    return TryHelper.tryFSucc0(() -> storage.get(resource));
  }
  
  private <O> BiFunction<DelaFileHandler, Throwable, Try<O>> failManifestExists(StorageType storageType, R resource) {
    return TryHelper.tryFSucc1((DelaFileHandler file) -> {
      String msg = "manifest already exists:" + file.getResource().getSinkName();
      return new Try.Failure(new DelaStorageException(msg, storageType));
    });
  }
  
  private BiFunction<DelaFileHandler<E,R>, Throwable, Try<DelaFileHandler<E, R>>> rCreateManifest(
    StorageType storageType, E endpoint, R resource) {
    return recoverFrom(storageType, DelaStorageException.RESOURCE_DOES_NOT_EXIST,
      () -> storage.create(resource));
  }
  
  private BiFunction<DelaFileHandler<E,R>, Throwable, Try<Boolean>> writeManifest(ManifestJSON manifest) {
    long pos = 0; //start
    byte[] manifestBytes = ManifestHelper.getManifestByte(manifest);
    return TryHelper.tryFSucc1((DelaFileHandler<E,R> file) -> file.append(pos, manifestBytes));
  }
  
  private BiFunction<DelaFileHandler<E,R>, Throwable, Try<byte[]>> readManifest() {
    return TryHelper.tryFSucc1((DelaFileHandler<E,R> file) -> file.readAll());
  }
  
  private BiFunction<byte[], Throwable, ManifestJSON> parseManifest() {
    return TryHelper.tryFSucc1((byte[] manifestBytes) -> ManifestHelper.getManifestJSON(manifestBytes));
  }
}
