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

import java.util.List;
import se.sics.ktoolbox.util.reference.KReference;
import se.sics.silk.util.Counter;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class R1AsyncMultiBuffer implements R1AsyncBuffer {

  private final List<R1AsyncBuffer> buffers;

  public R1AsyncMultiBuffer(List<R1AsyncBuffer> buffers) {
    this.buffers = buffers;
  }

  @Override
  public void write(long pos, KReference<byte[]> value, R1AsyncBufferCallback callback) {
    final Counter counter = new Counter(buffers.size());
    buffers.stream().forEach((buffer) -> {
      R1AsyncBufferCallback bufferCallback = bufferCallback(counter, callback);
      buffer.write(pos, value, bufferCallback);
    });
  }

  private R1AsyncBufferCallback bufferCallback(Counter counter, R1AsyncBufferCallback callback) {
    return new R1AsyncBufferCallback() {

      @Override
      public void completed() {
        counter.dec();
        if (counter.value() == 0) {
          callback.completed();
        }
      }
    };
  }
}
