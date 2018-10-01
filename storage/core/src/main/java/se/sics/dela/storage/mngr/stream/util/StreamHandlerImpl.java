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
  boolean pendingOp = false;
  boolean connected = false;

  StreamHandlerImpl() {
  }

  void setStorage(Map<String, StreamStorageHandler> readWrite, Map<String, StreamStorageHandler> writeOnly) {
    this.readWrite = readWrite;
    this.writeOnly = writeOnly;
  }

  @Override
  public boolean pendingOp() {
    return pendingOp;
  }

  @Override
  public void connectReadWrite(Consumer<Map<String, Long>> callback) {
    pendingOp = true;
    ConnectCollectorImpl collector = new ConnectCollectorImpl(readWrite.keySet(), callback);
    readWrite.values().forEach((handler) -> handler.connect(collector));
  }

  @Override
  public void connectWriteOnly(Consumer<Map<String, Long>> callback) {
    pendingOp = true;
    ConnectCollectorImpl collector = new ConnectCollectorImpl(writeOnly.keySet(), callback);
    writeOnly.values().forEach((handler) -> handler.connect(collector));
  }

  @Override
  public void disconnectWriteOnly(Consumer<Boolean> callback) {
    pendingOp = true;
    DisconnectCollectorImpl collector = new DisconnectCollectorImpl(writeOnly.keySet(), callback);
    writeOnly.values().forEach((handler) -> handler.disconnect(collector));
  }

  @Override
  public void disconnectReadWrite(Consumer<Boolean> callback) {
    pendingOp = true;
    DisconnectCollectorImpl collector = new DisconnectCollectorImpl(readWrite.keySet(), callback);
    readWrite.values().forEach((handler) -> handler.disconnect(collector));
  }

  private void connected() {
    connected = true;
    pendingOp = false;
  }

  private void disconnected() {
    connected = false;
    pendingOp = false;
  }

  @Override
  public boolean isConnected() {
    return connected;
  }

  public static interface ConnectCollector {

    public void collect(String storage, long streamPos);
  }

  public class ConnectCollectorImpl implements ConnectCollector {

    private Consumer<Map<String, Long>> callback;
    private Set<String> pending = new HashSet<>();
    private Map<String, Long> result = new HashMap<>();

    private ConnectCollectorImpl(Set<String> pending, Consumer<Map<String, Long>> callback) {
      this.callback = callback;
      this.pending.addAll(pending);
    }

    @Override
    public void collect(String storage, long streamPos) {
      if (pending.remove(storage)) {
        result.put(storage, streamPos);
      }
      if (pending.isEmpty()) {
        callback.accept(result);
        connected();
      }
    }
  }

  public static interface DisconnectCollector {

    public void collect(String storage);
  }

  public class DisconnectCollectorImpl implements DisconnectCollector {

    private Set<String> pending = new HashSet<>();
    private Consumer<Boolean> callback;

    public DisconnectCollectorImpl(Set<String> pending, Consumer<Boolean> callback) {
      this.callback = callback;
      this.pending.addAll(pending);
    }

    @Override
    public void collect(String storage) {
      pending.remove(storage);
      if (pending.isEmpty()) {
        callback.accept(true);
        disconnected();
      }
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
      StreamStorageHandler child = new StreamStorageHandler(streamId, storage, connect, disconnect);
      readWrite.put(child.name(), child);
    }

    public void writeOnly(StreamId streamId, StreamStorage storage) {
      StreamStorageHandler child = new StreamStorageHandler(streamId, storage, connect, disconnect);
      writeOnly.put(child.name(), child);
    }

    public StreamHandlerImpl build() {
      parent.setStorage(readWrite, writeOnly);
      return parent;
    }
  }
}
