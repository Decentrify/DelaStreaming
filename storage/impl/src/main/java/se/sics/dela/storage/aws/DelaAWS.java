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
package se.sics.dela.storage.aws;

import com.amazonaws.SdkClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.event.ProgressEvent;
import static com.amazonaws.event.ProgressEventType.CLIENT_REQUEST_STARTED_EVENT;
import static com.amazonaws.event.ProgressEventType.CLIENT_REQUEST_SUCCESS_EVENT;
import static com.amazonaws.event.ProgressEventType.HTTP_REQUEST_COMPLETED_EVENT;
import static com.amazonaws.event.ProgressEventType.HTTP_REQUEST_STARTED_EVENT;
import static com.amazonaws.event.ProgressEventType.HTTP_RESPONSE_COMPLETED_EVENT;
import static com.amazonaws.event.ProgressEventType.HTTP_RESPONSE_STARTED_EVENT;
import static com.amazonaws.event.ProgressEventType.REQUEST_BYTE_TRANSFER_EVENT;
import static com.amazonaws.event.ProgressEventType.REQUEST_CONTENT_LENGTH_EVENT;
import static com.amazonaws.event.ProgressEventType.RESPONSE_BYTE_TRANSFER_EVENT;
import static com.amazonaws.event.ProgressEventType.TRANSFER_PART_COMPLETED_EVENT;
import static com.amazonaws.event.ProgressEventType.TRANSFER_PART_STARTED_EVENT;
import static com.amazonaws.event.ProgressEventType.TRANSFER_STARTED_EVENT;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.DeleteObjectRequest;
import com.amazonaws.services.s3.model.GetObjectMetadataRequest;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.services.s3.transfer.PersistableTransfer;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerBuilder;
import com.amazonaws.services.s3.transfer.Upload;
import com.amazonaws.services.s3.transfer.internal.S3ProgressListener;
import java.io.IOException;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.function.Consumer;
import org.javatuples.Pair;
import se.sics.dela.storage.common.DelaAppendStream;
import se.sics.dela.storage.common.DelaFileHandler;
import se.sics.dela.storage.common.DelaHelper;
import se.sics.dela.storage.common.DelaReadStream;
import se.sics.dela.storage.common.DelaStorageException;
import se.sics.dela.storage.common.DelaStorageHandler;
import se.sics.dela.storage.common.StorageType;
import se.sics.dela.util.TimerProxy;
import se.sics.ktoolbox.util.TupleHelper;
import se.sics.ktoolbox.util.trysf.Try;
import static se.sics.ktoolbox.util.trysf.TryHelper.tryFSucc0;
import static se.sics.ktoolbox.util.trysf.TryHelper.tryFSucc1;
import static se.sics.ktoolbox.util.trysf.TryHelper.tryTSucc1;
import static se.sics.ktoolbox.util.trysf.TryHelper.tryVal;
import se.sics.nstream.util.range.KRange;

/**
 *
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class DelaAWS {

  public static class StorageHandler implements DelaStorageHandler<AWSEndpoint, AWSResource> {

    private final ExecutorService executors;
    private final AmazonS3 client;
    public final AWSEndpoint endpoint;

    private StorageHandler(ExecutorService executors, AmazonS3 client, AWSEndpoint endpoint) {
      this.executors = executors;
      this.client = client;
      this.endpoint = endpoint;
    }

    public static StorageHandler instance(ExecutorService executors, AWSCredentials credentials, AWSEndpoint endpoint) {
      AmazonS3 client = DelaAWS.client(credentials, endpoint.region);
      return new StorageHandler(executors, client, endpoint);
    }

    @Override
    public Try<DelaFileHandler<AWSEndpoint, AWSResource>> get(AWSResource resource) {
      return DelaAWS.getObjectMetadata(client, resource.bucket, resource.getKey())
        .map(tryFSucc0(() -> FileHandler.instance(executors, client, endpoint, resource)));
    }

    @Override
    public Try<DelaFileHandler<AWSEndpoint, AWSResource>> create(AWSResource resource) {
      return DelaAWS.exists(client, resource.bucket, resource.getKey())
        .flatMap(tryTSucc1((Boolean exists) -> {
          if (exists) {
            String msg = "file:" + resource.getSinkName() + " already exists";
            return new Try.Failure(new DelaStorageException(msg, StorageType.AWS));
          } else {
            return new Try.Success(FileHandler.instance(executors, client, endpoint, resource));
          }
        }));
    }

    @Override
    public Try<Boolean> delete(AWSResource resource) {
      return DelaAWS.delete(client, resource.bucket, resource.getKey());
    }
  }

  public static class FileHandler implements DelaFileHandler<AWSEndpoint, AWSResource> {

    private final ExecutorService executors;
    public final AWSEndpoint endpoint;
    public final AWSResource resource;
    private final AmazonS3 client;
    private final TransferManager tm;

    private FileHandler(ExecutorService executors, AWSEndpoint endpoint, AWSResource resource, AmazonS3 client,
      TransferManager tm) {
      this.executors = executors;
      this.endpoint = endpoint;
      this.resource = resource;
      this.client = client;
      this.tm = tm;
    }

    public static FileHandler instance(ExecutorService executors, AmazonS3 client,
      AWSEndpoint endpoint, AWSResource resource) {
      TransferManager tm = s3Transfer(client);
      return new FileHandler(executors, endpoint, resource, client, tm);
    }

    @Override
    public AWSEndpoint getEndpoint() {
      return endpoint;
    }

    @Override
    public AWSResource getResource() {
      return resource;
    }

    @Override
    public StorageType storageType() {
      return StorageType.AWS;
    }

    @Override
    public Try<Long> size() {
      Try<Long> result = new Try.Success(DelaAWS.size(client, resource.bucket, resource.getKey()));
      return result;
    }

    @Override
    public Try<byte[]> read(KRange range) {
      Try<byte[]> result = DelaAWS.read(client, resource.bucket, resource.getKey(), range);
      return result;
    }

    @Override
    public Try<byte[]> readAll() {
      Try<byte[]> result = size()
        .flatMap(DelaHelper.fullRange(StorageType.AWS, resource))
        .flatMap(tryFSucc1(
          (KRange range) -> DelaAWS.read(client, resource.bucket, resource.getKey(), range)));
      return result;
    }

    @Override
    public Try<Boolean> append(long pos, byte[] data) {
      if (pos != 0) {
        return new Try.Failure(new DelaStorageException("simple append works only as a full write", StorageType.AWS));
      }
      Try<Boolean> result = DelaAWS.write(executors, tm, resource, data);
      return result;
    }

    @Override
    public Try<DelaReadStream> readStream(TimerProxy timer) {
      return new Try.Success(new ReadStream(client, resource));
    }

    @Override
    public Try<DelaAppendStream> appendStream(TimerProxy timer) {
      return size().
        flatMap(tryFSucc1((Long fileSize) -> AppendStream.instance(executors, tm, resource, fileSize)));
    }
  }

  public static class ReadStream implements DelaReadStream {

    private final AmazonS3 client;
    private final AWSResource resource;

    private ReadStream(AmazonS3 client, AWSResource resource) {
      this.client = client;
      this.resource = resource;
    }

    @Override
    public void read(KRange range, Consumer<Try<byte[]>> callback) {
      Try<byte[]> result = DelaAWS.read(client, resource.bucket, resource.getKey(), range);
      callback.accept(result);
    }

    @Override
    public Try<Boolean> close() {
      return new Try.Success(true);
    }
  }

  public static class AppendStream implements DelaAppendStream, WriterProgress {

    private final ExecutorService executors;
    private Future<Boolean> appendStreamTask;
    private Optional<Upload> upload;
    //
    private final PipedOutputStream out;
    private final PipedInputStream in;
    //
    private final TreeMap<Long, Pair<Integer, Consumer<Try<Boolean>>>> pendingAppends = new TreeMap<>();
    private long pendingPos = 0;

    private AppendStream(ExecutorService executors, PipedInputStream in, PipedOutputStream out) {
      this.executors = executors;
      this.in = in;
      this.out = out;
    }

    private synchronized void setUpload(Upload upload) {
      this.upload = Optional.of(upload);
    }

    private synchronized void setTask(Future<Boolean> appendStreamTask) {
      this.appendStreamTask = appendStreamTask;
    }

    public static Try<DelaAppendStream> instance(ExecutorService executors, TransferManager tm, AWSResource resource,
      long size) {
      PipedOutputStream out = new PipedOutputStream();
      PipedInputStream in;
      try {
        in = new PipedInputStream(out);
      } catch (IOException ex) {
        String msg = "append error1";
        return new Try.Failure(new DelaStorageException(msg, ex, StorageType.AWS));
      }
      AppendStream writer = new AppendStream(executors, in, out);
      Future<Boolean> appendStreamTask = executors.submit(() -> {
        ObjectMetadata meta = new ObjectMetadata();
        meta.setContentLength(size);
        TupleHelper.PairConsumer<ProgressEvent, WriterProgress> transferEventConsumer = transferEventConsumer();
        Upload upload = tm.upload(new PutObjectRequest(resource.bucket, resource.getKey(), in, meta),
          new S3ProgressListener() {
          @Override
          public void onPersistableTransfer(PersistableTransfer pt) {
          }

          @Override
          public void progressChanged(ProgressEvent pe) {
            transferEventConsumer.accept(pe, writer);
          }
        });
        writer.setUpload(upload);
        upload.waitForCompletion();
        return true;
      });
      writer.setTask(appendStreamTask);
      return new Try.Success(writer);
    }

    @Override
    public synchronized void transferred() {
      System.err.println(pendingPos + "/" + upload.get().getProgress().getBytesTransferred());
      callbacks(upload.get().getProgress().getBytesTransferred());
    }

    @Override
    public synchronized void transferCompleted() {
      System.err.println(pendingPos + "/" + upload.get().getProgress().getBytesTransferred());
      callbacks(upload.get().getProgress().getBytesTransferred());
    }

    private void callbacks(Long confirmedPos) {
      Iterator<Map.Entry<Long, Pair<Integer, Consumer<Try<Boolean>>>>> it = pendingAppends.entrySet().iterator();
      while (it.hasNext()) {
        Map.Entry<Long, Pair<Integer, Consumer<Try<Boolean>>>> next = it.next();
        if (next.getKey() + next.getValue().getValue0() <= confirmedPos) {
          next.getValue().getValue1().accept(new Try.Success(true));
          it.remove();
        }
        if (next.getKey() > confirmedPos) {
          return;
        }
      }
    }

    @Override
    public synchronized void error(Throwable t) {
      Iterator<Map.Entry<Long, Pair<Integer, Consumer<Try<Boolean>>>>> it = pendingAppends.entrySet().iterator();
      while (it.hasNext()) {
        Map.Entry<Long, Pair<Integer, Consumer<Try<Boolean>>>> next = it.next();
        next.getValue().getValue1().accept(new Try.Success(true));
        it.remove();
      }
    }

    @Override
    public synchronized void write(long pos, byte[] data, Consumer<Try<Boolean>> callback) {
      if (pendingPos != pos) {
        String msg = "pending pos:" + pendingPos + " pos:" + pos;
        callback.accept(new Try.Failure(new DelaStorageException(msg, StorageType.AWS)));
      }
      try {
        out.write(data);
        pendingPos += data.length;
        pendingAppends.put(pos, Pair.with(data.length, callback));
      } catch (IOException ex) {
        String msg = "append error1";
        callback.accept(new Try.Failure(new DelaStorageException(msg, ex, StorageType.AWS)));
      }
    }

    @Override
    public synchronized Try<Boolean> close() {
      upload.get().abort();
      try {
        appendStreamTask.get();
      } catch (InterruptedException | ExecutionException ex) {
        String msg = "close error1";
        return new Try.Failure(new DelaStorageException(msg, ex, StorageType.AWS));
      } finally {
        try {
          in.close();
        } catch (IOException ex) {
          String msg = "close error2";
          return new Try.Failure(new DelaStorageException(msg, ex, StorageType.AWS));
        } finally {
          try {
            out.close();
          } catch (IOException ex) {
            String msg = "close error3";
            return new Try.Failure(new DelaStorageException(msg, ex, StorageType.AWS));
          }
        }
      }
      return new Try.Success(true);
    }
  }

  public static TupleHelper.PairConsumer<ProgressEvent, WriterProgress> transferEventConsumer() {
    return new TupleHelper.PairConsumer<ProgressEvent, WriterProgress>() {
      @Override
      public void accept(ProgressEvent pe, WriterProgress ctrl) {
        switch (pe.getEventType()) {
          case TRANSFER_STARTED_EVENT:
            break; //skip for now 
          case TRANSFER_PART_STARTED_EVENT:
            break; //skip for now 
          case CLIENT_REQUEST_STARTED_EVENT:
            break; //skip for now 
          case HTTP_REQUEST_STARTED_EVENT:
            break; //skip for now 
          case REQUEST_BYTE_TRANSFER_EVENT:
            ctrl.transferred();
            break; //skip for now 
          case REQUEST_CONTENT_LENGTH_EVENT:
            break; //skip for now 
          case RESPONSE_BYTE_TRANSFER_EVENT:
            break; //skip for now 
          case HTTP_REQUEST_COMPLETED_EVENT:
            break; //skip for now 
          case HTTP_RESPONSE_STARTED_EVENT:
            break; //skip for now 
          case HTTP_RESPONSE_COMPLETED_EVENT:
            break; //skip for now 
          case CLIENT_REQUEST_SUCCESS_EVENT:
            break; //skip for now 
          case TRANSFER_PART_COMPLETED_EVENT:
            break; //skip for now 
          case TRANSFER_COMPLETED_EVENT:
            ctrl.transferCompleted();
            break;
          default:
            System.err.println(pe.getEventType());
        }
      }
    };
  }

  public static interface WriterProgress {

    public void transferred();

    public void transferCompleted();

    public void error(Throwable t);
  }

  //************************************************BASE****************************************************************
  public static Try<ObjectMetadata> getObjectMetadata(AmazonS3 client, String bucket, String key) {
    try {
      ObjectMetadata meta = client.getObjectMetadata(new GetObjectMetadataRequest(bucket, key));
      if (meta == null) {
        return new Try.Failure(new DelaStorageException(DelaStorageException.RESOURCE_DOES_NOT_EXIST, StorageType.AWS));
      } else {
        return new Try.Success(meta);
      }
    } catch (SdkClientException ex) {
      String msg = "error getting object metadata";
      return new Try.Failure(new DelaStorageException(msg, ex, StorageType.AWS));
    }
  }

  public static Try<S3Object> getObject(AmazonS3 client, String bucket, String key, KRange range) {
    try {
      GetObjectRequest req = new GetObjectRequest(bucket, key)
        .withRange(range.lowerAbsEndpoint(), range.upperAbsEndpoint());
      S3Object obj = client.getObject(req);
      if (obj == null) {
        return new Try.Failure(new DelaStorageException(DelaStorageException.RESOURCE_DOES_NOT_EXIST, StorageType.AWS));
      } else {
        return new Try.Success(obj);
      }
    } catch (SdkClientException ex) {
      String msg = "error getting object";
      return new Try.Failure(new DelaStorageException(msg, ex, StorageType.AWS));
    }
  }

  public static Try<Boolean> delete(AmazonS3 client, String bucket, String key) {
    try {
      client.deleteObject(new DeleteObjectRequest(bucket, key));
      return new Try.Success(true);
    } catch (SdkClientException ex) {
      String msg = "error deleting object";
      return new Try.Failure(new DelaStorageException(msg, ex, StorageType.AWS));
    }
  }

  public static Try<byte[]> read(AmazonS3 client, String bucketName, String key, KRange range) {
    return getObject(client, bucketName, key, range)
      .flatMap(tryTSucc1((S3Object s3Object) -> {
        try (S3ObjectInputStream in = s3Object.getObjectContent()) {
          int readLength = (int) (range.upperAbsEndpoint() - range.lowerAbsEndpoint() + 1);
          byte[] val = new byte[readLength];
          int read = in.read(val);
          if (read != readLength) {
            String msg = "read error1";
            return new Try.Failure(new DelaStorageException(msg, StorageType.AWS));
          }
          return tryVal(val);
        } catch (IOException ex) {
          String msg = "read error2";
          return new Try.Failure(new DelaStorageException(msg, ex, StorageType.AWS));
        }
      }));
  }

  public static Try<Boolean> write(ExecutorService executor, TransferManager tm, AWSResource resource, byte[] data) {
    try (PipedOutputStream out = new PipedOutputStream();
      PipedInputStream in = new PipedInputStream(out)) {
      Future<Boolean> upload = executor.submit(() -> {
        ObjectMetadata meta = new ObjectMetadata();
        meta.setContentLength(data.length);
        TupleHelper.PairConsumer<ProgressEvent, WriterProgress> transferEventConsumer = transferEventConsumer();
        Upload upload1 = tm.upload(new PutObjectRequest(resource.bucket, resource.getKey(), in, meta));
        upload1.waitForCompletion();
        return true;
      });
      out.write(data);
      upload.get();
      return new Try.Success(true);
    } catch (InterruptedException | ExecutionException | IOException ex) {
      String msg = "write error1";
      return new Try.Failure(new DelaStorageException(msg, ex, StorageType.AWS));
    }
  }

  //******************************************************DELA**********************************************************
  public static AmazonS3 client(AWSCredentials credentials, Regions region) {
    AmazonS3 client = AmazonS3ClientBuilder.standard()
      .withCredentials(new AWSStaticCredentialsProvider(credentials))
      .withRegion(region)
      .build();
    return client;
  }

  public static TransferManager s3Transfer(AmazonS3 s3) {
    TransferManager tm = TransferManagerBuilder.standard()
      .withS3Client(s3)
      .withMultipartCopyPartSize(10 * 1024 * 1024l)
      .build();
    return tm;
  }

  public static Try<Boolean> exists(AmazonS3 client, String bucketName, String key) {
    return getObjectMetadata(client, bucketName, key)
      .transform(
        tryFSucc0(() -> tryVal(true)),
        DelaHelper.recoverFrom(StorageType.AWS, DelaStorageException.RESOURCE_DOES_NOT_EXIST,
          () -> tryVal(false)));
  }

  public static Try<Long> size(AmazonS3 client, String bucketName, String key) {
    return getObjectMetadata(client, bucketName, key)
      .flatMap(tryFSucc1((ObjectMetadata meta) -> tryVal(meta.getContentLength())));
  }

}
