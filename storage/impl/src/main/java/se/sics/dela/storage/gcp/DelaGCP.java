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
package se.sics.dela.storage.gcp;

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
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.javatuples.Pair;
import org.javatuples.Triplet;
import org.slf4j.Logger;
import se.sics.dela.storage.StorageEndpoint;
import se.sics.dela.storage.StorageResource;
import se.sics.dela.storage.common.DelaAppendStream;
import se.sics.dela.storage.common.DelaReadStream;
import se.sics.dela.storage.common.DelaStorageException;
import se.sics.dela.util.TimerProxy;
import se.sics.ktoolbox.util.Either;
import se.sics.ktoolbox.util.trysf.Try;
import se.sics.ktoolbox.util.trysf.TryHelper;
import se.sics.nstream.util.range.KRange;
import se.sics.dela.storage.common.DelaFileHandler;
import se.sics.dela.storage.common.DelaHelper;
import se.sics.dela.storage.common.DelaStorageHandler;
import se.sics.dela.storage.common.StorageType;
import se.sics.dela.storage.core.DelaStorageComp;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class DelaGCP {

  public static String BUCKET_NOT_FOUND = "dela_gcp_bucket_not_found";

  public static class StorageCompProvider implements se.sics.dela.storage.mngr.StorageProvider<DelaStorageComp> {

    public final StorageHandler storage;

    public StorageCompProvider(StorageHandler storage) {
      this.storage = storage;
    }

    @Override
    public Pair<DelaStorageComp.Init, Long> initiate(StorageResource resource, Logger logger) {
      GCPResource gcpResource = (GCPResource) resource;
      Try<DelaFileHandler<GCPEndpoint, GCPResource>> file
        = storage.get(gcpResource)
          .recoverWith(TryHelper.tryFSucc0(() -> storage.create(gcpResource)));
      if (file.isFailure()) {
        throw new RuntimeException(TryHelper.tryError(file));
      }
      Try<Long> pos = file.get().size();
      if (pos.isFailure()) {
        throw new RuntimeException(TryHelper.tryError(pos));
      }
      DelaStorageComp.Init init = new DelaStorageComp.Init(file.get(), pos.get());
      return Pair.with(init, pos.get());
    }

    @Override
    public String getName() {
      return storage.endpoint.getEndpointName();
    }

    @Override
    public Class<DelaStorageComp> getStorageDefinition() {
      return DelaStorageComp.class;
    }

    @Override
    public StorageEndpoint getEndpoint() {
      return storage.endpoint;
    }
  }

  public static class StorageHandler implements DelaStorageHandler<GCPEndpoint, GCPResource> {

    private final Storage storage;
    public final GCPEndpoint endpoint;

    public StorageHandler(Storage storage, GCPEndpoint endpoint) {
      this.storage = storage;
      this.endpoint = endpoint;
    }

    @Override
    public Try<DelaFileHandler<GCPEndpoint, GCPResource>> get(GCPResource resource) {
      return new Try.Success(true)
        .flatMap(DelaGCP.getBucketB0(storage, endpoint.bucketName))
        .flatMap(DelaGCP.getBlobB0(storage, resource.getBlobId()))
        .map(TryHelper.tryFSucc1((Blob blob) -> {
          blob.update();
          return new FileHandler(blob, endpoint, resource);
        }));
    }

    @Override
    public Try<DelaFileHandler<GCPEndpoint, GCPResource>> create(GCPResource resource) {
      return new Try.Success(true)
        .flatMap(DelaGCP.getBucketB0(storage, endpoint.bucketName))
        .flatMap(DelaGCP.getBlobB0(storage, resource.getBlobId()))
        .recoverWith(DelaHelper.recoverFrom(StorageType.GCP, DelaStorageException.RESOURCE_DOES_NOT_EXIST,
          DelaGCP.createBlobF0(storage, resource.getBlobId())))
        .map(TryHelper.tryFSucc1((Blob blob) -> {
          blob.update();
          return new FileHandler(blob, endpoint, resource);
        }));
    }

    @Override
    public Try<Boolean> delete(GCPResource resource) {
      return new Try.Success(true)
        .flatMap(DelaGCP.getBucketB0(storage, endpoint.bucketName))
        .flatMap(DelaGCP.getBlobB0(storage, resource.getBlobId()))
        .transform(DelaGCP.deleteBlobB1(), deleteNonExistingBlob());
    }

    private BiFunction<Blob, Throwable, Try<Boolean>> deleteNonExistingBlob() {
      return DelaHelper.recoverFrom(StorageType.GCP, DelaStorageException.RESOURCE_DOES_NOT_EXIST,
        () -> new Try.Success(true));
    }
  }

  public static class FileHandler implements DelaFileHandler<GCPEndpoint, GCPResource> {

    public final GCPEndpoint endpoint;
    public final GCPResource resource;
    private Blob blob;
    //
    private final String bucketName;
    private final BlobId blobId;

    public FileHandler(Blob blob, GCPEndpoint endpoint, GCPResource resource) {
      this.blob = blob;
      this.endpoint = endpoint;
      this.resource = resource;
      this.bucketName = resource.libDir;
      this.blobId = resource.getBlobId();
    }

    @Override
    public GCPEndpoint getEndpoint() {
      return endpoint;
    }

    @Override
    public GCPResource getResource() {
      return resource;
    }

    @Override
    public StorageType storageType() {
      return StorageType.GCP;
    }

    @Override
    public Try<Long> size() {
      return DelaGCP.blobSize(blob);
    }

    @Override
    public Try<byte[]> read(KRange range) {
      return DelaGCP.read(blob, range);
    }

    @Override
    public Try<byte[]> readAll() {
      return size()
        .flatMap(DelaHelper.fullRange(StorageType.GCP, resource))
        .flatMap(TryHelper.tryFSucc1((KRange range) -> read(range)));
    }

    @Override
    public Try<Boolean> append(long pos, byte[] data) {
      if (pos != 0) {
        return new Try.Failure(new DelaStorageException("simple append works only as a full write", StorageType.GCP));
      }
      return DelaGCP.write(blob, data);
    }

    @Override
    public Try<DelaReadStream> readStream(TimerProxy timer) {
      ReadChannel in = DelaGCP.readChannel(blob);
      return new Try.Success(new ReadStream(in));
    }

    @Override
    public Try<DelaAppendStream> appendStream(TimerProxy timer) {
      Try<Long> pos = size();
      if (pos.isFailure()) {
        return (Try.Failure) pos;
      } else if (pos.get() != 0) {
        String msg = "gcp can only append to fresh blobs - otherwise immutable";
        return new Try.Failure(new DelaStorageException(msg, StorageType.GCP));
      } else {
        WriteChannel out = DelaGCP.writeChannel(blob);
        return new Try.Success(new AppendStream(this, out));
      }
    }
    
    private void update() {
      Storage storage = blob.getStorage();
      try {
        Thread.sleep(1000);
        blob = DelaGCP.getBlob(storage, blobId).checkedGet();
      } catch (Throwable ex) {
        throw new RuntimeException(ex);
      }
    }
  }

  public static class ReadStream implements DelaReadStream {

    private final ReadChannel in;

    public ReadStream(ReadChannel in) {
      this.in = in;
    }

    @Override
    public void read(KRange range, Consumer<Try<byte[]>> callback) {
      callback.accept(DelaGCP.read(in, range));
    }

    @Override
    public Try<Boolean> close() {
      in.close();
      return new Try.Success(true);
    }
  }

  public static class AppendStream implements DelaAppendStream {

    private final FileHandler parent;
    private WriteChannel out;
    private long pendingPos = 0;

    public AppendStream(FileHandler parent, WriteChannel out) {
      this.parent = parent;
      this.out = out;
    }

    @Override
    public void write(long pos, byte[] data, Consumer<Try<Boolean>> callback) {
      if (pendingPos != pos) {
        String msg = "pending pos:" + pendingPos + " pos:" + pos;
        callback.accept(new Try.Failure(new DelaStorageException(msg, StorageType.GCP)));
      }
      callback.accept(DelaGCP.write(out, data));
      pendingPos += data.length;
    }

    @Override
    public Try<Boolean> close() {
      try {
        out.close();
        parent.update();
        return new Try.Success(true);
      } catch (IOException ex) {
        return new Try.Failure(new DelaStorageException("dela_gcp", ex, StorageType.GCP));
      }
    }
  }

//  public static BiFunction<GoogleCredentials, Throwable, Try<Boolean>> testCredentials(String projectName) {
//    return TryHelper.tryFSucc1((GoogleCredentials credentials) -> {
//      Random rand = new Random();
//      StorageClass storageClass = StorageClass.REGIONAL;
//      String storageLocation = "europe-west2";
//      String bucketName = "dela_testing_bucket_" + rand.nextInt();
//      String blobName = "dela_testing_blob_" + rand.nextInt();
//      byte[] writeVal = new byte[]{0x01, 0x02, 0x03, 0x04};
//      return testCredentials(credentials, storageClass, storageLocation, projectName, bucketName, blobName, writeVal);
//    });
//  }
//
//  public static Try<Boolean> testCredentials(GoogleCredentials credentials, String projectName, String bucketName) {
//    Random rand = new Random();
//    String blobName = "dela_testing_blob_" + rand.nextInt();
//    byte[] writeVal = new byte[]{0x01, 0x02, 0x03, 0x04};
//    return testCredentials(credentials, projectName, bucketName, blobName, writeVal);
//  }
//  public static Try<Boolean> testCredentials(GoogleCredentials credentials, String projectName, String bucketName,
//    String blobName, byte[] writeVal) {
//    BlobId blobId = BlobId.of(bucketName, blobName);
//    Try<Storage> storage = new Try.Success(getStorage(credentials, projectName));
//    Try<Bucket> bucket = storage
//      .flatMap(getBucketB1(bucketName));
//    Try<Blob> blob = TryHelper.Joiner.map(bucket, storage)
//      .flatMap(getBlobB1(blobId))
//      .recoverWith(
//        recoverFrom(BLOB_NOT_FOUND, storage.flatMap(createBlobB1(blobId)));
//    Try<Integer> writeOp = TryHelper.Joiner.map(blob, storage)
//      .flatMap(writeAllBlob(blobId, writeVal));
//    Try<byte[]> readOp = TryHelper.Joiner.map(writeOp, storage)
//      .flatMap(read(blobId, 0, writeVal.length));
//    Try<Boolean> readWriteCompare = TryHelper.Joiner.combine(writeOp, readOp)
//      .map(compareReadWrite(writeVal));
//    Try<Boolean> deleteBlobB1 = storage
//      .map(deleteBlobB1(blobId));
//    Try<Boolean> result = TryHelper.Joiner.combine(readWriteCompare, deleteBlobB1)
//      .flatMap(testCredentialsResult2());
//    return result;
//  }
//  public static Try<Boolean> testCredentials(GoogleCredentials credentials, StorageClass storageClass,
//    String storageLocation, String projectName, String bucketName, String blobName, byte[] writeVal) {
//    BlobId blobId = BlobId.of(bucketName, blobName);
//    Try<Storage> storage = new Try.Success(credentials)
//      .map(getStorage(projectName));
//    Try<Bucket> bucket = storage
//      .flatMap(getBucketB1(bucketName))
//      .recoverWith(rCreateBucket(bucketName, storageClass, storageLocation));
//    Try<Blob> blob = TryHelper.Joiner.map(bucket, storage)
//      .flatMap(getBlobB1(blobId))
//      .recoverWith(rCreateBlob(blobId));
//    Try<Integer> writeOp = TryHelper.Joiner.map(blob, storage)
//      .flatMap(writeAllBlob(blobId, writeVal));
//    Try<byte[]> readOp = TryHelper.Joiner.map(writeOp, storage)
//      .flatMap(read(blobId, 0, writeVal.length));
//    Try<Boolean> readWriteCompare = TryHelper.Joiner.combine(writeOp, readOp)
//      .map(compareReadWrite(writeVal));
//    Try<Boolean> deleteBlobB1 = storage
//      .map(deleteBlobB1(blobId));
//    Try<Boolean> deleteBucket = storage
//      .map(deleteBucket(bucketName));
//    Try<Boolean> result = TryHelper.Joiner.combine(readWriteCompare, deleteBlobB1, deleteBucket)
//      .flatMap(testCredentialsResult1());
//    return result;
//  }
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

  public static Storage getStorage(GoogleCredentials credentials, String project) {
    Storage storage = StorageOptions.newBuilder()
      .setProjectId(project)
      .setCredentials(credentials)
      .build()
      .getService();
    return storage;
  }

  public static BiFunction<Storage, Throwable, Try<Bucket>> getBucketB0(Storage storage, String bucketName) {
    return TryHelper.tryFSucc0(() -> getBucket(storage, bucketName));
  }

  public static BiFunction<Storage, Throwable, Try<Bucket>> getBucketB1(String bucketName) {
    return TryHelper.tryFSucc1((Storage storage) -> {
      return getBucket(storage, bucketName);
    });
  }

  public static Try<Bucket> getBucket(Storage storage, String bucketName) {
    Bucket bucket = storage.get(bucketName);
    if (bucket == null) {
      return new Try.Failure(new DelaStorageException(BUCKET_NOT_FOUND, StorageType.GCP));
    }
    return new Try.Success(bucket);
  }

  public static Try<Bucket> createBucket(Storage storage, StorageClass storageClass, String storageLocation,
    String bucketName) {
    BucketInfo bucketInfo = BucketInfo.newBuilder(bucketName)
      .setStorageClass(storageClass)
      .setLocation(storageLocation).build();
    try {
      Bucket bucket = storage.create(bucketInfo);
      return new Try.Success(bucket);
    } catch (StorageException ex) {
      return new Try.Failure(new DelaStorageException("dela_gcp", StorageType.GCP));
    }
  }

  public static Supplier<Try<Bucket>> createBucketF0(Storage storage, StorageClass storageClass, String storageLocation,
    String bucketName) {
    return () -> createBucket(storage, storageClass, storageLocation, bucketName);
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

  public static Try<Blob> getBlob(Storage storage, BlobId blobId) {
    Blob blob = storage.get(blobId);
    if (blob == null) {
      return new Try.Failure(new DelaStorageException(DelaStorageException.RESOURCE_DOES_NOT_EXIST, StorageType.GCP));
    }
    return new Try.Success(blob);
  }

  public static <I, O> BiFunction<I, Throwable, Try<Blob>> getBlobB0(Storage storage, BlobId blobId) {
    return TryHelper.tryFSucc0(() -> getBlob(storage, blobId));
  }

  public static BiFunction<Storage, Throwable, Try<Blob>> getBlobB1(BlobId blobId) {
    return TryHelper.tryFSucc1((Storage storage) -> {
      return getBlob(storage, blobId);
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

  public static BiFunction<Storage, Throwable, Try<Blob>> createBlobB1(BlobId blobId) {
    return TryHelper.tryFSucc1((Storage storage) -> {
      return createBlob(storage, blobId);
    });
  }

  public static Supplier<Try<Blob>> createBlobF0(Storage storage, BlobId blobId) {
    return () -> createBlob(storage, blobId);
  }

  public static Try<Boolean> deleteBlob(Blob blob) {
    return new Try.Success(blob.delete());
  }

  public static BiFunction<Blob, Throwable, Try<Boolean>> deleteBlobB1() {
    return TryHelper.tryFSucc1((Blob blob) -> {
      return deleteBlob(blob);
    });
  }

  public static Try<Long> blobSize(Blob blob) {
//    Blob bl = DelaGCP.getBlob(storage, blobId)storage.get(blob.getBlobId());
    return new Try.Success(blob.getSize());
  }

  public static BiFunction<Blob, Throwable, Try<Long>> blobSizeB1() {
    return TryHelper.tryFSucc1((Blob blob) -> {
      return blobSize(blob);
    });
  }

  public static ReadChannel readChannel(GoogleCredentials credentials, String projectName, BlobId blobId) {
    Storage storage = getStorage(credentials, projectName);
    ReadChannel reader = storage.reader(blobId);
    return reader;
  }

  public static ReadChannel readChannel(Blob blob) {
    return blob.reader();
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
      return DelaGCP.readAllBlob(storage, blobId);
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

  public static Try<byte[]> read(ReadChannel reader, KRange range) {
    try {
      int readLength = (int) (range.upperAbsEndpoint() - range.lowerAbsEndpoint() + 1);
      long readPos = range.lowerAbsEndpoint();
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
  }

  public static Try<byte[]> read(Blob blob, KRange range) {
    try (ReadChannel in = blob.reader()) {
      return new Try.Success(read(in, range));
    }
  }

  public static BiFunction<Blob, Throwable, Try<byte[]>> readB1(KRange range) {
    return TryHelper.tryFSucc1((Blob blob) -> read(blob, range));
  }

  private static Try<Boolean> write(WriteChannel writer, byte[] val) {
    try {
      int chunkSize = 64 * 1024;
      int writeChunk = Math.min(chunkSize, val.length);
      int writeFrom = 0;
      while (writeFrom < val.length) {
        int written = writer.write(ByteBuffer.wrap(val, writeFrom, writeChunk));
        writeFrom += written;
        writeChunk = Math.min(chunkSize, val.length - writeFrom);
      }
      return new Try.Success(true);
    } catch (IOException ex) {
      return new Try.Failure(ex);
    }
  }

  public static Try<Boolean> write(Blob blob, byte[] val) {
    try (WriteChannel out = blob.writer()) {
      return write(out, val);
    } catch (IOException ex) {
      return new Try.Failure(ex);
    }
  }

  public static WriteChannel writeChannel(GoogleCredentials credentials, String projectId, BlobId blobId) {
    Storage storage = getStorage(credentials, projectId);
    BlobInfo blobInfo = BlobInfo.newBuilder(blobId).setContentType("text/plain").build();
    WriteChannel writer = storage.writer(blobInfo);
    return writer;
  }

  public static WriteChannel writeChannel(Blob blob) {
//    BlobInfo blobInfo = BlobInfo.newBuilder(blobId).setContentType("text/plain").build();
    WriteChannel writer = blob.writer();
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
