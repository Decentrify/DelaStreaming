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
package se.sics.dela.storage.operation;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;
import se.sics.dela.storage.operation.events.StreamStorageOpRead;
import se.sics.dela.storage.operation.events.StreamStorageOpWrite;
import se.sics.kompics.ComponentProxy;
import se.sics.kompics.Handler;
import se.sics.kompics.Positive;
import se.sics.kompics.util.Identifier;
import se.sics.ktoolbox.util.identifiable.IdentifierFactory;
import se.sics.ktoolbox.util.trysf.Try;
import se.sics.nstream.StreamId;
import se.sics.nstream.util.range.KBlock;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class StreamStorageOpProxy {
  private ComponentProxy proxy;
  private Positive<StreamStorageOpPort> streamStorageOp;
  private IdentifierFactory eventIds;
  
  private final Map<Identifier, Consumer<Try<byte[]>>> readCallbacks = new HashMap<>();
  private final Map<Identifier, Consumer<Try<Boolean>>> writeCallbacks = new HashMap<>();
  
  public StreamStorageOpProxy() {
  }
  
  public StreamStorageOpProxy setup(ComponentProxy proxy, IdentifierFactory eventIds) {
    this.proxy = proxy;
    streamStorageOp = this.proxy.getNegative(StreamStorageOpPort.class).getPair();
    proxy.subscribe(handleRead, streamStorageOp);
    proxy.subscribe(handleWrite, streamStorageOp);
    this.eventIds = eventIds;
    return this;
  }
  
  public void read(StreamId streamId, KBlock readRange, Consumer<Try<byte[]>> callback) {
    StreamStorageOpRead.Request req = new StreamStorageOpRead.Request(eventIds.randomId(), streamId, readRange);
    proxy.trigger(req, streamStorageOp);
    readCallbacks.put(req.getId(), callback);
  }
  
  public void write(StreamId streamId, long pos, byte[] value, Consumer<Try<Boolean>> callback) {
    StreamStorageOpWrite.Request req = new StreamStorageOpWrite.Request(eventIds.randomId(), streamId, pos, value);
    proxy.trigger(req, streamStorageOp);
    writeCallbacks.put(req.getId(), callback);
  }
  
  public void readCompleted(StreamId streamId) {
    StreamStorageOpRead.Complete req = new StreamStorageOpRead.Complete(eventIds.randomId(), streamId);
    proxy.trigger(req, streamStorageOp);
  }
  
  public void writeCompleted(StreamId streamId) {
    StreamStorageOpWrite.Complete req = new StreamStorageOpWrite.Complete(eventIds.randomId(), streamId);
    proxy.trigger(req, streamStorageOp);
  }
  
  Handler handleRead = new Handler<StreamStorageOpRead.Response>() {
    @Override
    public void handle(StreamStorageOpRead.Response resp) {
      Consumer<Try<byte[]>> callback = readCallbacks.remove(resp.getId());
      if(callback != null) {
        Try<byte[]> result;
        if(resp.result.isSuccess()) {
          result = new Try.Success(resp.result.getValue());
        } else {
          result = new Try.Failure(resp.result.getException());
        }
        callback.accept(result);
      }
    }
  };
  
  Handler handleWrite = new Handler<StreamStorageOpWrite.Response>() {
    @Override
    public void handle(StreamStorageOpWrite.Response resp) {
      Consumer<Try<Boolean>> callback = writeCallbacks.remove(resp.getId());
      if(callback != null) {
        Try<Boolean> result;
        if(resp.result.isSuccess()) {
          result = new Try.Success(resp.result.getValue());
        } else {
          result = new Try.Failure(resp.result.getException());
        }
        callback.accept(result);
      }
    }
  };
}
