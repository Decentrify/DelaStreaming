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
package se.sics.dela.storage.buffer.nx;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import se.sics.dela.storage.buffer.KBuffer;
import se.sics.dela.storage.buffer.KBufferReport;
import se.sics.ktoolbox.util.reference.KReference;
import se.sics.ktoolbox.util.trysf.Try;
import se.sics.ktoolbox.util.trysf.TryHelper;
import se.sics.nstream.util.range.KBlock;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class MultiKBuffer implements KBuffer {

  private final List<KBuffer> buffers;

  public MultiKBuffer(List<KBuffer> buffers) {
    this.buffers = buffers;
  }

  @Override
  public void start() {
    buffers.forEach((buffer) -> buffer.start());
  }

  @Override
  public boolean isIdle() {
    boolean isEmpty = true;
    for (KBuffer buffer : buffers) {
      isEmpty = isEmpty && buffer.isIdle();
    }
    return isEmpty;
  }

  @Override
  public void close() {
    buffers.forEach((buffer) -> buffer.close());
  }

  @Override
  public void write(KBlock writeRange, KReference<byte[]> val, Consumer<Try<Boolean>> callback) {
    TryHelper.SimpleCollector<Boolean> collector = new TryHelper.SimpleCollector<>(buffers.size());
    Consumer<Try<Boolean>> resultConsumer = (writeResult) -> {
      collector.collect(writeResult);
      if(collector.completed()) {
        callback.accept(collector.getResult());
      }
    };
    buffers.forEach((buffer) -> buffer.write(writeRange, val, resultConsumer));
  }

  @Override
  public KBufferReport report() {
    List<KBufferReport> report = new ArrayList<>();
    buffers.forEach((buffer) -> report.add(buffer.report()));
    return new MultiKBufferReport(report);
  }
}
