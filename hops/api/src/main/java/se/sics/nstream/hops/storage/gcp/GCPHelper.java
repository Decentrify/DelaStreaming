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
package se.sics.nstream.hops.storage.gcp;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.ReadChannel;
import com.google.cloud.WriteChannel;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.BucketInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageClass;
import com.google.cloud.storage.StorageException;
import com.google.cloud.storage.StorageOptions;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Random;
import java.util.function.BiFunction;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.javatuples.Pair;
import org.javatuples.Triplet;
import se.sics.ktoolbox.util.Either;
import se.sics.ktoolbox.util.trysf.Try;
import se.sics.ktoolbox.util.trysf.TryHelper;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class GCPHelper {

  public static BiFunction<GoogleCredentials, Throwable, Try<Boolean>> testCredentials(String projectName) {
    return TryHelper.tryFSucc1((GoogleCredentials credentials) -> {
      Random rand = new Random();
      StorageClass storageClass = StorageClass.REGIONAL;
      String storageLocation = "europe-west2";
      String bucketName = "dela_testing_bucket_" + rand.nextInt();
      String blobName = "dela_testing_blob_" + rand.nextInt();
      byte[] writeVal = new byte[]{0x01, 0x02, 0x03, 0x04};
      return testCredentials(credentials, storageClass, storageLocation, projectName, bucketName, blobName, writeVal);
    });
  }

  public static Try<Boolean> testCredentials(GoogleCredentials credentials, String projectName, String bucketName) {
    Random rand = new Random();
    String blobName = "dela_testing_blob_" + rand.nextInt();
    byte[] writeVal = new byte[]{0x01, 0x02, 0x03, 0x04};
    return testCredentials(credentials, projectName, bucketName, blobName, writeVal);
  }

  public static Try<Boolean> testCredentials(GoogleCredentials credentials, String projectName, String bucketName,
    String blobName, byte[] writeVal) {
    BlobId blobId = BlobId.of(bucketName, blobName);
    Try<Storage> storage = new Try.Success(getStorage(credentials, projectName));
    Try<Bucket> bucket = storage
      .flatMap(getBucket(bucketName));
    Try<Blob> blob = TryHelper.Joiner.map(bucket, storage)
      .flatMap(getBlob(blobId))
      .recoverWith(rCreateBlob(blobId));
    Try<Integer> writeOp = TryHelper.Joiner.map(blob, storage)
      .flatMap(writeAllBlob(blobId, writeVal));
    Try<byte[]> readOp = TryHelper.Joiner.map(writeOp, storage)
      .flatMap(readFromBlob(blobId, 0, writeVal.length));
    Try<Boolean> readWriteCompare = TryHelper.Joiner.combine(writeOp, readOp)
      .map(compareReadWrite(writeVal));
    Try<Boolean> deleteBlob = storage
      .map(deleteBlob(blobId));
    Try<Boolean> result = TryHelper.Joiner.combine(readWriteCompare, deleteBlob)
      .flatMap(testCredentialsResult2());
    return result;
  }

  public static Try<Boolean> testCredentials(GoogleCredentials credentials, StorageClass storageClass,
    String storageLocation, String projectName, String bucketName, String blobName, byte[] writeVal) {
    BlobId blobId = BlobId.of(bucketName, blobName);
    Try<Storage> storage = new Try.Success(credentials)
      .map(getStorage(projectName));
    Try<Bucket> bucket = storage
      .flatMap(getBucket(bucketName))
      .recoverWith(rCreateBucket(bucketName, storageClass, storageLocation));
    Try<Blob> blob = TryHelper.Joiner.map(bucket, storage)
      .flatMap(getBlob(blobId))
      .recoverWith(rCreateBlob(blobId));
    Try<Integer> writeOp = TryHelper.Joiner.map(blob, storage)
      .flatMap(writeAllBlob(blobId, writeVal));
    Try<byte[]> readOp = TryHelper.Joiner.map(writeOp, storage)
      .flatMap(readFromBlob(blobId, 0, writeVal.length));
    Try<Boolean> readWriteCompare = TryHelper.Joiner.combine(writeOp, readOp)
      .map(compareReadWrite(writeVal));
    Try<Boolean> deleteBlob = storage
      .map(deleteBlob(blobId));
    Try<Boolean> deleteBucket = storage
      .map(deleteBucket(bucketName));
    Try<Boolean> result = TryHelper.Joiner.combine(readWriteCompare, deleteBlob, deleteBucket)
      .flatMap(testCredentialsResult1());
    return result;
  }

  private static BiFunction<Triplet<Boolean, Boolean, Boolean>, Throwable, Try<Boolean>> testCredentialsResult1() {
    return TryHelper.tryFSucc3((Boolean readWriteCompare) -> (Boolean deleteBlob) -> (Boolean deleteBucket) -> {
      if (!readWriteCompare) {
        return new Try.Failure(new IllegalStateException("write/read values do not match"));
      }
      return new Try.Success(true);
    });
  }

  private static BiFunction<Pair<Boolean, Boolean>, Throwable, Try<Boolean>> testCredentialsResult2() {
    return TryHelper.tryFSucc2((Boolean readWriteCompare) -> (Boolean deleteBlob) -> {
      if (!readWriteCompare) {
        return new Try.Failure(new IllegalStateException("write/read values do not match"));
      }
      return new Try.Success(true);
    });
  }

  public static BiFunction<GoogleCredentials, Throwable, Storage> getStorage(String projectName) {
    return TryHelper.tryFSucc1((GoogleCredentials credentials) -> {
      return getStorage(credentials, projectName);
    });
  }

  public static Storage getStorage(GoogleCredentials credentials, String projectId) {
    Storage storage = StorageOptions.newBuilder()
      .setProjectId(projectId)
      .setCredentials(credentials)
      .build()
      .getService();
    return storage;
  }

  public static BiFunction<Storage, Throwable, Try<Bucket>> getBucket(String bucketName) {
    return TryHelper.tryFSucc1((Storage storage) -> {
      return getBucket(storage, bucketName);
    });
  }

  public static Try<Bucket> getBucket(Storage storage, String bucketName) {
    Bucket bucket = storage.get(bucketName);
    if (bucket == null) {
      return new Try.Failure(BucketException.noSuchBucket(storage));
    }
    return new Try.Success(bucket);
  }

  public static BiFunction<Storage, Throwable, Bucket> createBucket(String bucketName,
    StorageClass storageClass, String storageLocation) {
    return TryHelper.tryFSucc1((Storage storage) -> {
      BucketInfo bucketInfo = BucketInfo.newBuilder(bucketName)
        .setStorageClass(storageClass)
        .setLocation(storageLocation).build();
      Bucket bucket = storage.create(bucketInfo);
      return bucket;
    });
  }

  public static <I> BiFunction<Bucket, Throwable, Try<Bucket>> rCreateBucket(String bucketName,
    StorageClass storageClass, String storageLocation) {
    return TryHelper.tryFFail((Throwable ex) -> {
      if (ex instanceof BucketException) {
        BucketException be = (BucketException) ex;
        if (be.isNoSuchBucket()) {
          BucketInfo bucketInfo = BucketInfo.newBuilder(bucketName)
            .setStorageClass(storageClass)
            .setLocation(storageLocation).build();
          Bucket bucket = be.storage.create(bucketInfo);
          return new Try.Success(bucket);
        }
      }
      return new Try.Failure(ex);
    });
  }

  public static BiFunction<Storage, Throwable, Boolean> deleteBucket(String bucketName) {
    return TryHelper.tryFSucc1((Storage storage) -> {
      return storage.delete(bucketName);
    });
  }

  public static BiFunction<Storage, Throwable, Try<Blob>> getBlob(BlobId blobId) {
    return TryHelper.tryFSucc1((Storage storage) -> {
      return getBlob(storage, blobId);
    });
  }

  public static Try<Blob> getBlob(Storage storage, BlobId blobId) {
    try {
      Blob blob = storage.get(blobId);
      if (blob == null) {
        return new Try.Failure(BlobException.noSuchBlob(storage));
      }
      return new Try.Success(blob);
    } catch (StorageException ex) {
      return new Try.Failure(ex);
    }
  }

  public static BiFunction<Storage, Throwable, Try<Blob>> createBlob(BlobId blobId) {
    return TryHelper.tryFSucc1((Storage storage) -> {
      return createBlob(storage, blobId);
    });
  }

  public static Try<Blob> createBlob(Storage storage, BlobId blobId) {
    BlobInfo blobInfo = BlobInfo.newBuilder(blobId).setContentType("text/plain").build();
    try {
      Blob blob = storage.create(blobInfo);
      return new Try.Success(blob);
    } catch (StorageException ex) {
      return new Try.Failure(ex);
    }
  }

  public static BiFunction<Blob, Throwable, Try<Blob>> rCreateBlob(BlobId blobId) {
    return TryHelper.tryFFail((Throwable ex) -> {
      if (ex instanceof BlobException) {
        BlobException be = (BlobException) ex;
        if (be.isNoSuchBlob()) {
          BlobInfo blobInfo = BlobInfo.newBuilder(blobId).setContentType("text/plain").build();
          Blob blob = be.storage.create(blobInfo);
          return new Try.Success(blob);
        }
      }
      return new Try.Failure(ex);
    });
  }

  public static BiFunction<Storage, Throwable, Boolean> deleteBlob(BlobId blobId) {
    return TryHelper.tryFSucc1((Storage storage) -> {
      return storage.delete(blobId);
    });
  }

  public static ReadChannel readChannel(GoogleCredentials credentials, String projectName, BlobId blobId) {
    Storage storage = getStorage(credentials, projectName);
    ReadChannel reader = storage.reader(blobId);
    return reader;
  }

  public static ReadChannel readChannel(Storage storage, BlobId blobId) {
    ReadChannel reader = storage.reader(blobId);
    return reader;
  }

  public static BiFunction<Blob, Throwable, Try<byte[]>> readAllBlob() {
    return TryHelper.tryFSucc1((Blob blob) -> {
      try {
        byte[] content = blob.getContent();
        return new Try.Success(content);
      } catch (StorageException ex) {
        return new Try.Failure(ex);
      }
    });
  }

  public static BiFunction<Storage, Throwable, Try<byte[]>> readAllBlob(BlobId blobId) {
    return TryHelper.tryFSucc1((Storage storage) -> {
      return GCPHelper.readAllBlob(storage, blobId);
    });
  }

  public static Try<byte[]> readAllBlob(Storage storage, BlobId blobId) {
    try {
      byte[] content = storage.readAllBytes(blobId);
      return new Try.Success(content);
    } catch (StorageException ex) {
      return new Try.Failure(ex);
    }
  }

  public static BiFunction<Storage, Throwable, Try<byte[]>> readFromBlob(BlobId blobId, long readPos, int readLength) {
    return TryHelper.tryFSucc1((Storage storage) -> {
      try (ReadChannel reader = storage.reader(blobId)) {
        ByteBuffer buf = ByteBuffer.allocate(readLength);
        int chunkSize = 64 * 1024;
        int readFrom = 0;
        ByteBuffer bytes = ByteBuffer.allocate(chunkSize);
        reader.seek(readPos);
        while (readFrom < readLength) {
          readFrom += reader.read(bytes);
          bytes.flip();
          buf.put(bytes);
          bytes.clear();
        }
        return new Try.Success(buf.array());
      } catch (IOException ex) {
        return new Try.Failure(ex);
      }
    });
  }

  public static BiFunction<Boolean, Throwable, Try<byte[]>> readFromBlob(ReadChannel reader,
    long readPos, int readLength) {
    return TryHelper.tryFSucc0(() -> {
      try {
        ByteBuffer buf = ByteBuffer.allocate(readLength);
        int chunkSize = 64 * 1024;
        int readFrom = 0;
        ByteBuffer bytes = ByteBuffer.allocate(chunkSize);
        reader.seek(readPos);
        while (readFrom < readLength) {
          readFrom += reader.read(bytes);
          bytes.flip();
          buf.put(bytes);
          bytes.clear();
        }
        return new Try.Success(buf.array());
      } catch (IOException ex) {
        return new Try.Failure(ex);
      }
    });
  }

  public static WriteChannel writeChannel(GoogleCredentials credentials, String projectId, BlobId blobId) {
    Storage storage = getStorage(credentials, projectId);
    BlobInfo blobInfo = BlobInfo.newBuilder(blobId).setContentType("text/plain").build();
    WriteChannel writer = storage.writer(blobInfo);
    return writer;
  }

  public static WriteChannel writeChannel(Storage storage, BlobId blobId) {
    BlobInfo blobInfo = BlobInfo.newBuilder(blobId).setContentType("text/plain").build();
    WriteChannel writer = storage.writer(blobInfo);
    return writer;
  }

  public static <I> Try<I> closeWriter(WriteChannel writer, Try<I> writeResult) {
    try {
      writer.close();
      return writeResult;
    } catch (IOException ex) {
      if (writeResult.isFailure()) {
        return writeResult;
      } else {
        return new Try.Failure(ex);
      }
    }
  }

  public static BiFunction<Pair<Blob, byte[]>, Throwable, Try<Integer>> writeAllBlob() {
    return TryHelper.tryFSucc2((Blob blob) -> (byte[] val) -> {
      WriteChannel writer = blob.writer();
      Try<Integer> writeResult = writeToBlobAux(writer, val);
      Try<Integer> result = closeWriter(writer, writeResult);
      return result;
    });
  }

  public static Try<Integer> writeAllBlob(Storage storage, BlobId blobId, byte[] val) {
    BlobInfo blobInfo = BlobInfo.newBuilder(blobId).setContentType("text/plain").build();
    WriteChannel writer = storage.writer(blobInfo);
    Try<Integer> writeResult = writeToBlobAux(writer, val);
    Try<Integer> result = closeWriter(writer, writeResult);
    return result;
  }

  public static BiFunction<Storage, Throwable, Try<Integer>> writeAllBlob(BlobId blobId, byte[] val) {
    return TryHelper.tryFSucc1((Storage storage) -> {
      return GCPHelper.writeAllBlob(storage, blobId, val);
    });
  }

  public static BiFunction<Boolean, Throwable, Try<Integer>> writeToBlob(WriteChannel writer, byte[] val) {
    return TryHelper.tryFSucc0(() -> {
      return writeToBlobAux(writer, val);
    });
  }

  public static BiFunction<byte[], Throwable, Try<Integer>> writeToBlob(WriteChannel writer) {
    return TryHelper.tryFSucc1((byte[] val) -> {
      return writeToBlobAux(writer, val);
    });
  }

  private static Try<Integer> writeToBlobAux(WriteChannel writer, byte[] val) {
    try {
      int chunkSize = 64 * 1024;
      int writeChunk = Math.min(chunkSize, val.length);
      int writeFrom = 0;
      while (writeFrom < val.length) {
        int written = writer.write(ByteBuffer.wrap(val, writeFrom, writeChunk));
        writeFrom += written;
        writeChunk = Math.min(chunkSize, val.length - writeFrom);
      }
      return new Try.Success(writeFrom);
    } catch (IOException ex) {
      return new Try.Failure(ex);
    }
  }

  public static BiFunction<Pair<Integer, byte[]>, Throwable, Boolean> compareReadWrite(byte[] writeVal) {
    return TryHelper.tryFSucc2((Integer writtenBytes) -> (byte[] readVal) -> {
      if (writtenBytes != writeVal.length || readVal == null || readVal.length != writtenBytes) {
        return false;
      }
      return Arrays.equals(writeVal, readVal);
    });

  }

  public static class BucketException extends Exception {

    public static String NO_SUCH_BUCKET = "no such bucket";

    public final Either<String, Throwable> cause;
    public final Storage storage;

    public BucketException(Throwable cause, Storage storage) {
      this.cause = Either.right(cause);
      this.storage = storage;
    }

    public BucketException(String cause, Storage storage) {
      this.cause = Either.left(cause);
      this.storage = storage;
    }

    public boolean isNoSuchBucket() {
      return cause.isLeft() && cause.getLeft().equals(NO_SUCH_BUCKET);
    }

    public static BucketException noSuchBucket(Storage storage) {
      return new BucketException(NO_SUCH_BUCKET, storage);
    }
  }

  public static class BlobException extends Exception {

    public static String NO_SUCH_BLOB = "no such blob";

    public final Either<String, Throwable> cause;
    public final Storage storage;

    public BlobException(Throwable cause, Storage storage) {
      this.cause = Either.right(cause);
      this.storage = storage;
    }

    public BlobException(String cause, Storage storage) {
      this.cause = Either.left(cause);
      this.storage = storage;
    }

    public boolean isNoSuchBlob() {
      return cause.isLeft() && cause.getLeft().equals(NO_SUCH_BLOB);
    }

    public static BlobException noSuchBlob(Storage storage) {
      return new BlobException(NO_SUCH_BLOB, storage);
    }
  }
}
