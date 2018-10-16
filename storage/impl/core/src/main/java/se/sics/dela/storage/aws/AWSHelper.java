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
package se.sics.dela.storage.aws;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.event.ProgressEvent;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.transfer.PersistableTransfer;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerBuilder;
import com.amazonaws.services.s3.transfer.TransferProgress;
import com.amazonaws.services.s3.transfer.Upload;
import com.amazonaws.services.s3.transfer.internal.S3ProgressListener;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.util.Optional;
import java.util.Random;
import java.util.UUID;
import java.util.function.Supplier;
import org.slf4j.Logger;
import se.sics.ktoolbox.util.TupleHelper;
import se.sics.ktoolbox.util.trysf.Try;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class AWSHelper {

  public static Try<AWSCredentials> credentialsFromCSV(String csvKeyFile) {
    BufferedReader br;
    try {
      br = new BufferedReader(new FileReader(csvKeyFile));
      String header = br.readLine();
      if (!header.equals("Access key ID,Secret access key")) {
        return new Try.Failure(new IllegalArgumentException("csv key file has weird format"));
      }
      String[] content = br.readLine().split(",");
      if (content.length != 2) {
        return new Try.Failure(new IllegalArgumentException("csv key file has weird format"));
      }
      if (br.readLine() != null) {
        return new Try.Failure(new IllegalArgumentException("csv key file has weird format"));
      }
      return new Try.Success(new BasicAWSCredentials(content[0], content[1]));
    } catch (IOException ex) {
      return new Try.Failure(ex);
    }
  }

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

  public static class TransferState {

    public final String bucketName;
    public final String key;
    public final long size;
    private Optional<TransferProgress> progress = Optional.empty();
    public Optional<Throwable> cause = Optional.empty();
    public final Logger log;

    public TransferState(String bucketName, String key, long size, Logger log) {
      this.bucketName = bucketName;
      this.key = key;
      this.size = size;
      this.log = log;
    }

    public synchronized void progress(TransferProgress progress) {
      this.progress = Optional.of(progress);
    }

    public synchronized void error(Throwable cause) {
      this.cause = Optional.of(cause);
    }

    public synchronized void transferred() {
      if (progress.isPresent()) {
        log.debug("transfer:<{},{}> progress:{}", 
          new Object[]{bucketName, key, progress.get().getPercentTransferred()});
      }
    }

    public void transferCompleted() {
      log.info("transfer:<{},{}> completed", new Object[]{bucketName, key});
    }
  }

  public static TupleHelper.PairConsumer<ProgressEvent, TransferState> transferEventConsumer() {
    return new TupleHelper.PairConsumer<ProgressEvent, TransferState>() {
      @Override
      public void accept(ProgressEvent pe, TransferState transferState) {
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
            transferState.transferred();
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
            transferState.transferCompleted();
            break;
          default:
            System.out.println(pe.getEventType());
        }
      }
    };
  }
  
  public static class AWSClient {
    
  }

  public static Thread s3WritterThread(AmazonS3 s3, TransferState ts, PipedInputStream in) {
    return new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          InputStream read;
          TransferManager tm = s3Transfer(s3);
          ObjectMetadata meta = new ObjectMetadata();
          meta.setContentLength(ts.size);
          TupleHelper.PairConsumer<ProgressEvent, TransferState> transferEventConsumer = transferEventConsumer();
          Upload upload = tm.upload(new PutObjectRequest(ts.bucketName, ts.key, in, meta), new S3ProgressListener() {

            @Override
            public void onPersistableTransfer(PersistableTransfer pt) {
            }

            @Override
            public void progressChanged(ProgressEvent pe) {
              transferEventConsumer.accept(pe, ts);
            }
          });
          ts.progress(upload.getProgress());
          upload.waitForCompletion();
          tm.shutdownNow();
        } catch (AmazonClientException ex) {
          ts.error(ex);
        } catch (InterruptedException ex) {
          ts.error(ex);
        }
      }
    });
  }

  private static Thread pipedOutputThread(TransferState ts, int n, Supplier<byte[]> s, PipedOutputStream out) {
    return new Thread(new Runnable() {
      @Override
      public void run() {
        for (int j = 0; j < n; j++) {
          try {
            byte[] b = s.get();
            out.write(b);
          } catch (IOException ex) {
            ts.error(ex);
            break;
          }
        }
      }
    });
  }

  private static Supplier<byte[]> randomData(Random rand) {
    return () -> {
      byte[] b = new byte[1024 * 1024];
      rand.nextBytes(b);
      return b;
    };
  }

  public static void testCredentials(AmazonS3 s3, Logger log) throws IOException, InterruptedException {
    String bucketName = "dela-test-bucket-" + UUID.randomUUID();
    String key = "test-object-" + UUID.randomUUID();
    int n = 10;
    long transferSize = n * 1024 * 1024;
    TransferState transferState = new TransferState(bucketName, key, transferSize, log);
    PipedOutputStream out = new PipedOutputStream();
    PipedInputStream in = new PipedInputStream(out);

    s3.createBucket(bucketName);
    Thread t1 = s3WritterThread(s3, transferState, in);
    Thread t2 = pipedOutputThread(transferState, n, randomData(new Random(123)), out);
    t1.start();
    t2.start();
    t1.join();
    t2.join();
  }
}
