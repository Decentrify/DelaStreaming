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
package se.sics.dela.storage.buffer.append;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;
import org.javatuples.Pair;
import se.sics.dela.storage.buffer.KBuffer;
import se.sics.dela.storage.buffer.KBufferReport;
import se.sics.dela.storage.operation.StreamStorageOpProxy;
import se.sics.ktoolbox.util.reference.KReference;
import se.sics.ktoolbox.util.reference.KReferenceException;
import se.sics.ktoolbox.util.trysf.Try;
import se.sics.nstream.StreamId;
import se.sics.nstream.util.range.KBlock;

/**
 * The Buffer runs in the same component that calls its KBuffer methods. We
 * subscribe the handlers on the proxy of this component, thus they will run on
 * the same thread. There is no need for synchronization
 *
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class SimpleAppendBuffer implements KBuffer {

  private final StreamId streamId;
  //**************************************************************************
  private final StreamStorageOpProxy proxy;
  //**************************************************************************
  private int blockPos;
  private long appendPos;
  //write result callback
  private final Map<Long, Pair<KReference<byte[]>, Consumer<Try<Boolean>>>> buffer = new HashMap<>();

  public SimpleAppendBuffer(StreamStorageOpProxy proxy, StreamId streamId, long appendPos, int writeBlock) {
    this.proxy = proxy;
    this.streamId = streamId;
    this.appendPos = appendPos;
    this.blockPos = writeBlock;
  }

  @Override
  public void start() {
    //nothing here
  }

  @Override
  public boolean isIdle() {
    return buffer.isEmpty();
  }

  @Override
  public void close() throws KReferenceException {
    proxy.writeCompleted(streamId);
    clean();
  }

  @Override
  public void write(KBlock writeRange, KReference<byte[]> val, Consumer<Try<Boolean>> callback) {
    if (!val.retain()) {
      callback.accept(new Try.Failure(new IllegalStateException("buffer can't retain ref")));
      return;
    }
    buffer.put(writeRange.lowerAbsEndpoint(), Pair.with(val, callback));
    if (writeRange.lowerAbsEndpoint() == appendPos) {
      addNewTasks();
    }
  }

  private void addNewTasks() {
    while (true) {
      Pair<KReference<byte[]>, Consumer<Try<Boolean>>> next = buffer.get(appendPos);
      if (next == null) {
        break;
      }
      Consumer<Try<Boolean>> callback = writeCallback(appendPos, next.getValue1());
      byte[] value = next.getValue0().getValue().get();
      proxy.write(streamId, appendPos, value, callback);
      appendPos += next.getValue0().getValue().get().length;
      blockPos++;
    }
  }

  private Consumer<Try<Boolean>> writeCallback(long appendPos, Consumer<Try<Boolean>> callback) {
    return (result) -> {
      Pair<KReference<byte[]>, Consumer<Try<Boolean>>> ref = buffer.remove(appendPos);
      if (ref == null) {
        String msg = "write callback on non existing write - pos:" + appendPos;
        callback.accept(new Try.Failure(new IllegalStateException(msg)));
      } else {
        try {
          ref.getValue0().release();
        } catch (KReferenceException ex) {
          callback.accept(new Try.Failure(ex));
        }
        callback.accept(result);
      }
      addNewTasks();
    };
  }

  private void clean() throws KReferenceException {
    for (Pair<KReference<byte[]>, Consumer<Try<Boolean>>> ref : buffer.values()) {
      ref.getValue0().release();
    }
    buffer.clear();
  }

  @Override
  public KBufferReport report() {
    return new SimpleKBufferReport(blockPos, appendPos, buffer.size());
  }
}
