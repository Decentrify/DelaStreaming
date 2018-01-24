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
package se.sics.silk.r2torrent.storage.buffer;

import se.sics.ktoolbox.util.reference.KReference;
import se.sics.ktoolbox.util.reference.KReferenceException;
import se.sics.nstream.StreamId;
import se.sics.silk.r2torrent.storage.sink.R1SinkWriteCallback;
import se.sics.silk.r2torrent.storage.sink.R1SinkWriter;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class R1AsyncAppendBuffer implements R1AsyncBuffer {

  private final StreamId streamId;
  private final R1SinkWriter sinkWriter;

  public R1AsyncAppendBuffer(StreamId streamId, R1SinkWriter sinkWriter) {
    this.streamId = streamId;
    this.sinkWriter = sinkWriter;
  }

  @Override
  public void write(long pos, KReference<byte[]> value, R1AsyncBufferCallback callback) {
    if(!value.retain()) {
      throw new RuntimeException("logic error");
    }
    R1SinkWriteCallback cb = new R1SinkWriteCallback() {

      @Override
      public void completed() {
        try {
          value.release();
        } catch (KReferenceException ex) {
          throw new RuntimeException("logic error");
        }
        callback.completed();
      }
    };
    sinkWriter.write(streamId, pos, value.getValue().get(), cb);
  }
}
