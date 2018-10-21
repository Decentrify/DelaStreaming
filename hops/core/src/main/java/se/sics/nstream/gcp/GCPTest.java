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

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.WriteChannel;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;
import java.util.Random;
import se.sics.ktoolbox.util.trysf.Try;
import se.sics.nstream.hops.storage.hops.ManifestHelper;
import se.sics.nstream.hops.storage.hops.ManifestJSON;
import se.sics.nstream.hops.storage.gcp.GCPEndpoint;
import se.sics.nstream.hops.storage.gcp.GCPHelper;
import se.sics.nstream.hops.storage.gcp.GCPResource;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class GCPTest {

  public static void main(String[] args) throws FileNotFoundException, IOException {
    test2();
  }

  public static void test1() throws FileNotFoundException, IOException {
    BlobId blobId = BlobId.of("dela_bucket", "test");
    GoogleCredentials credentials
      = GoogleCredentials.fromStream(new FileInputStream("/Users/Alex/Desktop/gcp_dela.json"));
    String project = "dela-197715";
    Storage storage = GCPHelper.getStorage(credentials, project);
    Try<Blob> blob = new Try.Success(storage)
      .map(GCPHelper.createBlob(blobId));
    BlobInfo blobInfo = BlobInfo.newBuilder(blobId).setContentType("text/plain").build();
    try (WriteChannel writer = storage.writer(blobInfo)) {
      Random rand = new Random();
      for (int i = 0; i < 200; i++) {
        byte[] data = new byte[1024*1024];
        rand.nextBytes(data);
        Try<Integer> write = new Try.Success(true)
          .flatMap(GCPHelper.writeToBlob(writer, data));
      }
    }
  }
  
  public static void test2() throws FileNotFoundException, IOException {
    GoogleCredentials credentials
      = GoogleCredentials.fromStream(new FileInputStream("/Users/Alex/Desktop/gcp_dela.json"));
    String project = "dela-197715";
    GCPEndpoint endpoint = new GCPEndpoint(credentials, project);
    GCPResource resource = new GCPResource("dela_bucket", "test_dataset", "manifest.json");
    ManifestJSON manifest = ManifestHelper.dummyManifest();
    Try<Integer> writtenBytes = DelaGCPHelper.tryWriteManifest(endpoint, resource, manifest);
    Try<ManifestJSON> readManifest = DelaGCPHelper.tryReadManifest(endpoint, resource);
    System.out.println(writtenBytes.get());
  }
  
  public static void test3() {
    byte[] manifest1 = ManifestHelper.getManifestByte(ManifestHelper.dummyManifest());
    byte[] manifest2 = ManifestHelper.getManifestByte(ManifestHelper.dummyManifest());
    System.out.println(Arrays.equals(manifest1, manifest2));
  }
  
  public static void test4() throws FileNotFoundException, IOException {
     GoogleCredentials credentials
      = GoogleCredentials.fromStream(new FileInputStream("/Users/Alex/Desktop/gcp_dela.json"));
    String project = "dela-197715";
    GCPEndpoint endpoint = new GCPEndpoint(credentials, project);
    GCPResource resource = new GCPResource("dela_bucket", "test_dataset", "manifest.json");
    Random rand = new Random(123); 
    byte[] val = new byte[300];
    rand.nextBytes(val);
    Storage storage = GCPHelper.getStorage(credentials, project);
    Try<Integer> writeBlob = GCPHelper.writeAllBlob(storage, resource.getBlobId(), val);
    System.out.println(writeBlob.get());
    Try<byte[]> readBlob = GCPHelper.readAllBlob(storage, resource.getBlobId());
    System.out.println(readBlob.get().length);
    System.out.println(Arrays.equals(val, readBlob.get()));
  }
}
