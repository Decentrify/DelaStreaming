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
package se.sics.nstream.gcp;

import com.google.cloud.WriteChannel;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Storage;
import java.util.function.BiFunction;
import java.util.logging.Level;
import java.util.logging.Logger;
import se.sics.ktoolbox.util.result.Result;
import se.sics.ktoolbox.util.trysf.Try;
import se.sics.ktoolbox.util.trysf.TryHelper;
import se.sics.nstream.hops.manifest.ManifestHelper;
import se.sics.nstream.hops.manifest.ManifestJSON;
import se.sics.nstream.hops.storage.gcp.GCPEndpoint;
import se.sics.nstream.hops.storage.gcp.GCPHelper;
import se.sics.nstream.hops.storage.gcp.GCPHelper.BlobException;
import se.sics.nstream.hops.storage.gcp.GCPResource;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class DelaGCPHelper {

  public static Try<ManifestJSON> tryReadManifest(GCPEndpoint endpoint, GCPResource resource) {
    Try<Storage> storage = new Try.Success(GCPHelper.getStorage(endpoint.credentials, endpoint.projectName));
    BlobId blobId = resource.getBlobId();
    Try<ManifestJSON> manifest = storage
      .flatMap(GCPHelper.readAllBlob(blobId))
      .flatMap(ManifestHelper.tryGetManifestJSON());
    return manifest;
  }

  public static Result<ManifestJSON> readManifest(GCPEndpoint endpoint, GCPResource resource) {
    try {
      ManifestJSON manifest = tryReadManifest(endpoint, resource).checkedGet();
      return Result.success(manifest);
    } catch (Throwable ex) {
      return Result.externalSafeFailure((Exception) ex);
    }
  }

  public static Try<Integer> tryWriteManifest(GCPEndpoint endpoint, GCPResource resource, ManifestJSON manifest) {
    BlobId blobId = resource.getBlobId();
    Storage storage = GCPHelper.getStorage(endpoint.credentials, endpoint.projectName);
    Try<byte[]> toWriteManifest = ManifestHelper.tryGetManifestBytes(manifest);
    Try<byte[]> writtenManifest = GCPHelper.getBlob(storage, blobId)
      .flatMap(GCPHelper.readAllBlob());
    Try<Boolean> sameExistingManifest = TryHelper.Joiner.combine(toWriteManifest, writtenManifest)
      .map(TryHelper.compareArray())
      .recoverWith(noManifestException());
    try {
      if (sameExistingManifest.checkedGet()) {
        return new Try.Success(0);
      }
    } catch (Throwable ex) {
      return new Try.Failure(ex);
    }
    Try<Integer> writeResult = TryHelper.Joiner.combine(
      GCPHelper.createBlob(storage, blobId),
      toWriteManifest)
      .flatMap(GCPHelper.writeAllBlob());
    return writeResult;
  }
  
  public static Result<Boolean> writeManifest(GCPEndpoint endpoint, GCPResource resource, ManifestJSON manifest) {
    try {
      if(tryWriteManifest(endpoint, resource, manifest).checkedGet() == 0) {
        return Result.success(false);
      } else {
        return Result.success(true);
      }
    } catch (Throwable ex) {
      return Result.externalSafeFailure((Exception) ex);
    }
  }

  private static BiFunction<Boolean, Throwable, Try<Boolean>> noManifestException() {
    return TryHelper.tryFFail((Throwable t) -> {
      if (t instanceof BlobException && ((BlobException) t).isNoSuchBlob()) {
        return new Try.Success(false);
      }
      return new Try.Failure(t);
    });
  }
}
