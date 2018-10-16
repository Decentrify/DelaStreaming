/*
 * Copyright (C) 2013 - 2018, Logical Clocks AB and RISE SICS AB. All rights reserved
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of this
 * software and associated documentation files (the "Software"), to deal in the Software
 * without restriction, including without limitation the rights to use, copy, modify, merge,
 * publish, distribute, sublicense, and/or sell copies of the Software, and to permit
 * persons to whom the Software is furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all copies or
 * substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS  OR IMPLIED, INCLUDING
 * BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL  THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM,
 * DAMAGES OR  OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 *
 */
package se.sics.dela.storage.aws;

import com.amazonaws.services.s3.AmazonS3;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Storage;
import java.util.function.BiFunction;
import se.sics.ktoolbox.util.result.Result;
import se.sics.ktoolbox.util.trysf.Try;
import se.sics.ktoolbox.util.trysf.TryHelper;
import se.sics.nstream.hops.manifest.ManifestHelper;
import se.sics.nstream.hops.manifest.ManifestJSON;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class DelaAWSHelper {
  public static Try<ManifestJSON> tryReadManifest(AWSEndpoint endpoint, AWSResource resource) {
    Try<AmazonS3> storage = new Try.Success(AWSHelper.client(endpoint.credentials, endpoint.region));
    Try<ManifestJSON> manifest = storage
      .flatMap(GCPHelper.readAllBlob(blobId))
      .flatMap(ManifestHelper.tryGetManifestJSON());
    return manifest;
  }

  public static Result<ManifestJSON> readManifest(AWSEndpoint endpoint, GCPResource resource) {
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
      if (t instanceof GCPHelper.BlobException && ((GCPHelper.BlobException) t).isNoSuchBlob()) {
        return new Try.Success(false);
      }
      return new Try.Failure(t);
    });
  }
}
