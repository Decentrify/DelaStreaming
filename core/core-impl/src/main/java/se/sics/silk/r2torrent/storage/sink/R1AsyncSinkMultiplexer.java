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
package se.sics.silk.r2torrent.storage.sink;

import java.util.HashMap;
import java.util.Map;
import se.sics.ktoolbox.util.result.Result;
import se.sics.nstream.StreamId;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class R1AsyncSinkMultiplexer {
  private static final boolean retry = true; //later maybe detect recurring sink failures and maybe backoff
  private final R1SinkSend sinkOp;
  private final Map<StreamId, R1SinkWriteCallback> writeCallbacks = new HashMap<>();
  private final R1SinkWriter sinkWrite = new R1SinkWriter() {

    @Override
    public void write(StreamId streamId, long pos, byte[] value, R1SinkWriteCallback callback) {
      writeCallbacks.put(streamId, callback);
      sinkOp.writeToSink(streamId, pos, value);
    }
  };
  
  public R1AsyncSinkMultiplexer(R1SinkSend sinkOp) {
    this.sinkOp = sinkOp;
  }
  
  public R1SinkWriter getWriteOp() {
    return sinkWrite;
  }
  
  public void completed(StreamId streamId, Result<Boolean> result) {
    R1SinkWriteCallback writeCallback = writeCallbacks.remove(streamId);
    if(result.isSuccess()) {
      writeCallback.completed();
    } else {
      throw new RuntimeException("not yet implemented/tested");
    }
  }
}
