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
package se.sics.dela.storage.mngr.stream.impl;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;
import se.sics.dela.storage.StreamStorage;
import se.sics.dela.storage.mngr.stream.StreamMngrPort;
import se.sics.dela.storage.mngr.stream.events.StreamMngrConnect;
import se.sics.dela.storage.mngr.stream.events.StreamMngrDisconnect;
import se.sics.dela.storage.mngr.stream.util.StreamHandler;
import se.sics.dela.storage.mngr.stream.util.StreamHandlerImpl;
import se.sics.dela.storage.mngr.stream.util.StreamStorageConnected;
import se.sics.dela.storage.mngr.stream.util.StreamStorageDisconnected;
import se.sics.dela.util.ResultCallback;
import se.sics.kompics.ComponentProxy;
import se.sics.kompics.Handler;
import se.sics.kompics.Positive;
import se.sics.kompics.util.Identifier;
import se.sics.ktoolbox.util.TupleHelper;
import se.sics.nstream.FileId;
import se.sics.nstream.StreamId;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class BaseStreamMngrProxy {

  private ComponentProxy proxy;
  private Positive<StreamMngrPort> streamMngr;

  private final Map<FileId, ResultCallback<Boolean>> storageReadyCallbacks = new HashMap<>();

  //<torrentId, fileId, handler>
  private final Table<Identifier, FileId, StreamHandler> streamHandlers = HashBasedTable.create();
  //<eventId, callbacks>
  private final Map<Identifier, StreamStorageConnected> connectedCallbacks = new HashMap<>();
  private final Map<Identifier, StreamStorageDisconnected> disconnectedCallbacks = new HashMap<>();
    
  public BaseStreamMngrProxy() {
  }
  
  public void subscribe(ComponentProxy proxy) {
    this.proxy = proxy;
    streamMngr = proxy.getPositive(StreamMngrPort.class);
    proxy.subscribe(handleConnected, streamMngr);
    proxy.subscribe(handleDisconnected, streamMngr);
  }

  public void prepareFile(Identifier torrentId, FileId fileId, Map<StreamId, StreamStorage> readWrite, 
    Map<StreamId, StreamStorage> writeOnly) {
    StreamHandlerImpl.Builder builder = new StreamHandlerImpl.Builder(
      connectedCallback(torrentId), disconnectedCallback(torrentId));
    readWrite.entrySet().forEach((storage) -> builder.readWrite(storage.getKey(), storage.getValue()));
    writeOnly.entrySet().forEach((storage) -> builder.writeOnly(storage.getKey(), storage.getValue()));
    streamHandlers.put(torrentId, fileId, builder.build());
  }
  
  public void setupReadWrite(Identifier torrentId, FileId fileId, Consumer<Boolean> callback) {
    streamHandlers.get(torrentId, fileId).setupReadWrite(callback);
  }
  
  public void setupReadOnly(Identifier torrentId, FileId fileId, Consumer<Boolean> callback) {
    streamHandlers.get(torrentId, fileId).setupReadOnly(callback);
  }
  
  public void writeComplete(Identifier torrentId, FileId fileId, Consumer<Boolean> callback) {
    streamHandlers.get(torrentId, fileId).writeComplete(callback);
  }
  
  public void readComplete(Identifier torrentId, FileId fileId, Consumer<Boolean> callback) {
    streamHandlers.get(torrentId, fileId).readComplete(callback);
  }
  
  private TupleHelper.TripletConsumer<StreamId, StreamStorage, StreamStorageConnected>
    connectedCallback(Identifier clientId) {
    return TupleHelper.tripletConsumer((streamId) -> (streamStorage) -> (callback) -> {
      StreamMngrConnect.Request req = new StreamMngrConnect.Request(clientId, streamId, streamStorage);
      proxy.trigger(req, streamMngr);
      connectedCallbacks.put(req.getId(), callback);
    });
  }

  private TupleHelper.PairConsumer<StreamId, StreamStorageDisconnected>
    disconnectedCallback(Identifier clientId) {
    return TupleHelper.pairConsumer((streamId) -> (callback) -> {
      StreamMngrDisconnect.Request req = new StreamMngrDisconnect.Request(clientId, streamId);
      proxy.trigger(req, streamMngr);
      disconnectedCallbacks.put(req.getId(), callback);
    });
  }

  Handler<StreamMngrConnect.Success> handleConnected = new Handler<StreamMngrConnect.Success>() {
    @Override
    public void handle(StreamMngrConnect.Success resp) {
      StreamStorageConnected callback = connectedCallbacks.remove(resp.getId());
      if(callback != null) {
        callback.connected();
      }
    }
  };
  
  Handler<StreamMngrConnect.Success> handleDisconnected = new Handler<StreamMngrConnect.Success>() {
    @Override
    public void handle(StreamMngrConnect.Success resp) {
      StreamStorageDisconnected callback = disconnectedCallbacks.remove(resp.getId());
      if(callback != null) {
        callback.disconnected();
      }
    }
  };
}
