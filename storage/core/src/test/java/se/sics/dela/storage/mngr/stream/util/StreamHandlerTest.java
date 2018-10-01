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
 * GNU General Public License for more defLastBlock.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
 */
package se.sics.dela.storage.mngr.stream.util;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;
import junit.framework.Assert;
import org.junit.After;
import org.junit.Test;
import se.sics.dela.storage.StorageEndpoint;
import se.sics.dela.storage.StorageResource;
import se.sics.dela.storage.StreamStorage;
import se.sics.ktoolbox.util.TupleHelper;
import se.sics.nstream.StreamId;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class StreamHandlerTest {

  Map<String, StreamStorageConnected> pendingConnect = new HashMap<>();
  Map<String, StreamStorageDisconnected> pendingDisconnect = new HashMap<>();
  TupleHelper.TripletConsumer<StreamId, StreamStorage, StreamStorageConnected> connect = TupleHelper.tripletConsumer(
    (id) -> (storage) -> (callback) -> {
    pendingConnect.put(callback.name(), callback);
  });
  TupleHelper.PairConsumer<StreamId, StreamStorageDisconnected> disconnect = TupleHelper.pairConsumer(
    (id) -> (callback) -> {
    pendingDisconnect.put(callback.name(), callback);
  });

  @After
  public void cleanup() {
    pendingConnect.clear();
    pendingDisconnect.clear();
  }

  @Test
  public void test() {
    SetupCallback streamCallback;
    WriteSetupCallback readWriteStreamCallback;
    WriteSetupCallback writeOnlyStreamCallback;
    StreamHandlerImpl.Builder sb = new StreamHandlerImpl.Builder(connect, disconnect);
    StorageResource r = resource("resource");
    StreamStorage s1 = new StreamStorage(endpoint("endpoint1"), r);
    StreamStorage s2 = new StreamStorage(endpoint("endpoint2"), r);
    StreamStorage s3 = new StreamStorage(endpoint("endpoint3"), r);
    StreamStorage s4 = new StreamStorage(endpoint("endpoint4"), r);
    sb.readWrite(null, s1);
    sb.readWrite(null, s2);
    sb.writeOnly(null, s3);
    sb.writeOnly(null, s4);

    StreamHandler handler = sb.build();
    //1
    writeOnlyStreamCallback = new WriteSetupCallback();
    
    setupWriteOnly(handler, writeOnlyStreamCallback);
    storageConnected(s3);
    storageConnected(s4);
    streamConnected(handler, writeOnlyStreamCallback);
    //2
    streamCallback = new SetupCallback();
    
    disconnectWriteOnly(handler, streamCallback);
    storageDisconnected(s3);
    storageDisconnected(s4);
    streamDisconnected(handler, streamCallback);

    //3
    readWriteStreamCallback = new WriteSetupCallback();
    
    setupReadWrite(handler, readWriteStreamCallback);
    storageConnected(s1);
    storageConnected(s2);
    streamConnected(handler, readWriteStreamCallback);

    //4
    streamCallback = new SetupCallback();
    
    disconnectReadWrite(handler, streamCallback);
    storageDisconnected(s1);
    storageDisconnected(s2);
    streamDisconnected(handler, streamCallback);

  }

  private void setupReadWrite(StreamHandler handler, WriteSetupCallback callback) {
    Assert.assertFalse(handler.isConnected());
    Assert.assertFalse(handler.pendingOp());
    handler.connectReadWrite(callback);
    Assert.assertFalse(callback.isReady());
    Assert.assertTrue(handler.pendingOp());
  }

  private void setupWriteOnly(StreamHandler handler, WriteSetupCallback callback) {
    Assert.assertFalse(handler.isConnected());
    Assert.assertFalse(handler.pendingOp());
    handler.connectWriteOnly(callback);
    Assert.assertFalse(callback.isReady());
    Assert.assertTrue(handler.pendingOp());
  }

  private void disconnectWriteOnly(StreamHandler handler, SetupCallback callback) {
    Assert.assertFalse(handler.pendingOp());
    handler.disconnectWriteOnly(callback);
    Assert.assertFalse(callback.isReady());
    Assert.assertTrue(handler.pendingOp());
  }

  private void disconnectReadWrite(StreamHandler handler, SetupCallback callback) {
    Assert.assertFalse(handler.pendingOp());
    handler.disconnectReadWrite(callback);
    Assert.assertFalse(callback.isReady());
    Assert.assertTrue(handler.pendingOp());
  }

  private void streamConnected(StreamHandler handler, Callback callback) {
    Assert.assertTrue(pendingConnect.isEmpty());
    Assert.assertTrue(callback.isReady());
    Assert.assertTrue(handler.isConnected());
  }

  private void streamDisconnected(StreamHandler handler, Callback callback) {
    Assert.assertTrue(pendingDisconnect.isEmpty());
    Assert.assertTrue(callback.isReady());
    Assert.assertFalse(handler.isConnected());
  }

  private void storageConnected(StreamStorage storage) {
    Assert.assertTrue(pendingConnect.containsKey(storage.getName()));
    StreamStorageConnected handler = pendingConnect.remove(storage.getName());
    handler.connected(0);
  }

  private void storageDisconnected(StreamStorage storage) {
    Assert.assertTrue(pendingDisconnect.containsKey(storage.getName()));
    StreamStorageDisconnected handler = pendingDisconnect.remove(storage.getName());
    handler.disconnected();
  }

  private StorageEndpoint endpoint(String name) {
    return new StorageEndpoint() {
      @Override
      public String getEndpointName() {
        return name;
      }
    };
  }

  private StorageResource resource(String name) {
    return new StorageResource() {
      @Override
      public String getSinkName() {
        return name;
      }
    };
  }

  public interface Callback {
    public boolean isReady();
  }
  
  public static class WriteSetupCallback implements Consumer<Map<String, Long>>, Callback {
    public boolean ready = false;

    @Override
    public void accept(Map<String, Long> t) {
      ready = true;
    }

    @Override
    public boolean isReady() {
      return ready;
    }
  }
  
  public static class SetupCallback implements Consumer<Boolean>, Callback {

    public boolean ready = false;

    @Override
    public void accept(Boolean t) {
      ready = true;
    }

    @Override
    public boolean isReady() {
      return ready;
    }
  }
}
