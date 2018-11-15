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
import org.javatuples.Pair;
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
import se.sics.ktoolbox.util.identifiable.IdentifierFactory;
import se.sics.nstream.FileId;
import se.sics.nstream.StreamId;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class StreamMngrProxy {

  private ComponentProxy proxy;
  private Positive<StreamMngrPort> streamMngr;

  private final Map<FileId, ResultCallback<Boolean>> storageReadyCallbacks = new HashMap<>();

  //<torrentId, fileId, handler>
  private final Table<Identifier, FileId, StreamHandler> streamHandlers = HashBasedTable.create();
  //<eventId, callbacks>
  private final Map<Identifier, StreamStorageConnected> connectedCallbacks = new HashMap<>();
  private final Map<Identifier, StreamStorageDisconnected> disconnectedCallbacks = new HashMap<>();
  private IdentifierFactory eventIds;
  public StreamMngrProxy() {
  }

  public StreamMngrProxy setup(ComponentProxy proxy, IdentifierFactory eventIds) {
    this.proxy = proxy;
    streamMngr = proxy.getNegative(StreamMngrPort.class).getPair();
    proxy.subscribe(handleConnected, streamMngr);
    proxy.subscribe(handleDisconnected, streamMngr);
    this.eventIds = eventIds;
    return this;
  }

  public void prepareFile(Identifier torrentId, FileId fileId, Map<StreamId, StreamStorage> readWrite,
    Map<StreamId, StreamStorage> writeOnly) {
    StreamHandlerImpl.Builder builder = new StreamHandlerImpl.Builder(
      connectedCallback(torrentId), disconnectedCallback(torrentId));
    readWrite.entrySet().forEach((storage) -> builder.readWrite(storage.getKey(), storage.getValue()));
    writeOnly.entrySet().forEach((storage) -> builder.writeOnly(storage.getKey(), storage.getValue()));
    streamHandlers.put(torrentId, fileId, builder.build());
  }

  public void setupWrite(Identifier torrentId, FileId fileId,
    TupleHelper.PairConsumer<Pair<String, Long>, Pair<String, Long>> callback) {
    WriteSetupCollector collector = new WriteSetupCollector(callback);
    streamHandlers.get(torrentId, fileId).connectReadWrite((result) -> collector.readWrite(result));
    streamHandlers.get(torrentId, fileId).connectWriteOnly((result) -> collector.writeOnly(result));
  }

  public void setupRead(Identifier torrentId, FileId fileId, Consumer<Boolean> callback) {
    streamHandlers.get(torrentId, fileId).connectReadWrite((result) -> callback.accept(true));
  }

  public void closeWrite(Identifier torrentId, FileId fileId, Consumer<Boolean> callback) {
    streamHandlers.get(torrentId, fileId).disconnectWriteOnly(callback);
  }

  public void closeRead(Identifier torrentId, FileId fileId, Consumer<Boolean> callback) {
    streamHandlers.get(torrentId, fileId).disconnectReadWrite(callback);
  }

  private static class WriteSetupCollector {

    private TupleHelper.PairConsumer<Pair<String, Long>, Pair<String, Long>> callback;
    private Map<String, Long> readWriteResult = null;
    private Map<String, Long> writeOnlyResult = null;

    public WriteSetupCollector(TupleHelper.PairConsumer<Pair<String, Long>, Pair<String, Long>> callback) {
      this.callback = callback;
    }

    public void readWrite(Map<String, Long> result) {
      this.readWriteResult = result;
      if (writeOnlyResult != null) {
        completed();
      }
    }

    public void writeOnly(Map<String, Long> result) {
      this.writeOnlyResult = result;
      if (readWriteResult != null) {
        completed();
      }
    }

    private void completed() {
      Pair<String, Long> writePos = Pair.with("", Long.MAX_VALUE);
      Pair<String, Long> readPos = Pair.with("", Long.MIN_VALUE);
      for (Map.Entry<String, Long> e : readWriteResult.entrySet()) {
        if(e.getValue() < writePos.getValue1()) {
          writePos = Pair.with(e.getKey(), e.getValue());
        }
        if(e.getValue() > readPos.getValue1()) {
          readPos = Pair.with(e.getKey(), e.getValue());
        }
      }
      for (Map.Entry<String, Long> e : writeOnlyResult.entrySet()) {
        if(e.getValue() < writePos.getValue1()) {
          writePos = Pair.with(e.getKey(), e.getValue());
        }
      }
      callback.accept(readPos, writePos);
    }
  }

  private static class WriteCompleteCollector {

    private final Consumer<Boolean> callback;
    private Boolean readWriteResult = null;
    private Boolean writeOnlyResult = null;

    public WriteCompleteCollector(Consumer<Boolean> callback) {
      this.callback = callback;
    }

    public void readWrite(boolean result) {
      this.readWriteResult = result;
      if (writeOnlyResult != null) {
        callback.accept(readWriteResult && writeOnlyResult);
      }
    }

    public void writeOnly(boolean result) {
      this.writeOnlyResult = result;
      if (readWriteResult != null) {
        callback.accept(readWriteResult && writeOnlyResult);
      }
    }
  }

  private TupleHelper.TripletConsumer<StreamId, StreamStorage, StreamStorageConnected>
    connectedCallback(Identifier clientId) {
    return TupleHelper.tripletConsumer((streamId) -> (streamStorage) -> (callback) -> {
      StreamMngrConnect.Request req = new StreamMngrConnect.Request(eventIds.randomId(), clientId, streamId, streamStorage);
      proxy.trigger(req, streamMngr);
      connectedCallbacks.put(req.getId(), callback);
    });
  }

  private TupleHelper.PairConsumer<StreamId, StreamStorageDisconnected>
    disconnectedCallback(Identifier clientId) {
    return TupleHelper.pairConsumer((streamId) -> (callback) -> {
      StreamMngrDisconnect.Request req = new StreamMngrDisconnect.Request(eventIds.randomId(), clientId, streamId);
      proxy.trigger(req, streamMngr);
      disconnectedCallbacks.put(req.getId(), callback);
    });
  }

  Handler<StreamMngrConnect.Success> handleConnected = new Handler<StreamMngrConnect.Success>() {
    @Override
    public void handle(StreamMngrConnect.Success resp) {
      StreamStorageConnected callback = connectedCallbacks.remove(resp.getId());
      if (callback != null) {
        callback.connected(resp.streamPos);
      }
    }
  };

  Handler<StreamMngrConnect.Success> handleDisconnected = new Handler<StreamMngrConnect.Success>() {
    @Override
    public void handle(StreamMngrConnect.Success resp) {
      StreamStorageDisconnected callback = disconnectedCallbacks.remove(resp.getId());
      if (callback != null) {
        callback.disconnected();
      }
    }
  };
}
