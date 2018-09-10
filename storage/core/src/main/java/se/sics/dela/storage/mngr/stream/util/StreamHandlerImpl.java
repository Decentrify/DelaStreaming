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
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import se.sics.dela.storage.StreamStorage;
import se.sics.ktoolbox.util.TupleHelper;
import se.sics.nstream.StreamId;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class StreamHandlerImpl implements StreamHandler {

  Map<String, StreamStorageHandler> readWrite = new HashMap<>();
  Map<String, StreamStorageHandler> writeOnly = new HashMap<>();
  Consumer<Boolean> callback;
  Set<String> pendingOps = new HashSet<>();
  boolean connected = false;

  StreamHandlerImpl() {
  }

  void setStorage(Map<String, StreamStorageHandler> readWrite, Map<String, StreamStorageHandler> writeOnly) {
    this.readWrite = readWrite;
    this.writeOnly = writeOnly;
  }

  @Override
  public boolean pendingOp() {
    return callback != null;
  }

  @Override
  public void setupReadWrite(Consumer<Boolean> callback) {
    this.callback = callback;
    pendingOps.addAll(readWrite.keySet());
    pendingOps.addAll(writeOnly.keySet());
    readWrite.values().forEach((handler) -> handler.connect());
    writeOnly.values().forEach((handler) -> handler.connect());
  }

  @Override
  public void setupReadOnly(Consumer<Boolean> callback) {
    this.callback = callback;
    pendingOps.addAll(readWrite.keySet());
    readWrite.values().forEach((handler) -> handler.connect());
  }

  @Override
  public void writeComplete(Consumer<Boolean> callback) {
    this.callback = callback;
    pendingOps.addAll(writeOnly.keySet());
    writeOnly.values().forEach((handler) -> handler.disconect());
  }

  @Override
  public void readComplete(Consumer<Boolean> callback) {
    this.callback = callback;
    pendingOps.addAll(readWrite.keySet());
    pendingOps.addAll(writeOnly.keySet());
    readWrite.values().forEach((handler) -> handler.disconect());
    writeOnly.values().forEach((handler) -> handler.disconect());
  }
  
  @Override
  public boolean isConnected() {
    return connected;
  }
  
  public void connected(String storage) {
    pendingOps.remove(storage);
    if (pendingOps.isEmpty()) {
      callback.accept(true);
      callback = null;
      connected = true;
    }
  }

  public void disconnected(String storage) {
    pendingOps.remove(storage);
    if (pendingOps.isEmpty()) {
      callback.accept(true);
      callback = null;
      connected = false;
    }
  }

  public static class Builder {

    private final StreamHandlerImpl parent;
    private final TupleHelper.TripletConsumer<StreamId, StreamStorage, StreamStorageConnected> connect;
    private final TupleHelper.PairConsumer<StreamId, StreamStorageDisconnected> disconnect;
    Map<String, StreamStorageHandler> readWrite = new HashMap<>();
    Map<String, StreamStorageHandler> writeOnly = new HashMap<>();

    public Builder(
      TupleHelper.TripletConsumer<StreamId, StreamStorage, StreamStorageConnected> connect,
      TupleHelper.PairConsumer<StreamId, StreamStorageDisconnected> disconnect) {
      this.parent = new StreamHandlerImpl();
      this.connect = connect;
      this.disconnect = disconnect;
    }

    public void readWrite(StreamId streamId, StreamStorage storage) {
      StreamStorageHandler child = new StreamStorageHandler(streamId, storage, parent, connect, disconnect);
      readWrite.put(child.name(), child);
    }

    public void writeOnly(StreamId streamId, StreamStorage storage) {
      StreamStorageHandler child = new StreamStorageHandler(streamId, storage, parent, connect, disconnect);
      writeOnly.put(child.name(), child);
    }

    public StreamHandlerImpl build() {
      parent.setStorage(readWrite, writeOnly);
      return parent;
    }
  }
}
