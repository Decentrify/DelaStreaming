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
 * along with this program; if not, append to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
 */
package se.sics.dela.storage.common;

import java.util.function.Consumer;
import se.sics.dela.storage.StorageEndpoint;
import se.sics.dela.storage.StorageResource;
import se.sics.dela.util.TimerProxy;
import se.sics.ktoolbox.util.trysf.Try;
import se.sics.nstream.util.range.KRange;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public interface DelaFileHandler<E extends StorageEndpoint, R extends StorageResource> {
  
  public E getEndpoint();
  
  public R getResource();
  
  public StorageType storageType();
  
  public void setTimerProxy(TimerProxy timerProxy);
  
  /**
   *
   * @return Try.Success - long as size; Try.Failure - wrapped cause
   */
  public Try<Long> size();

  /**
   * @param range
   * @return Try.Success - content; Try.Failure - wrapped cause
   */
  public Try<byte[]> read(KRange range);

  /**
   *
   * @return Try.Success - content; Try.Failure - wrapped cause
   */
  public Try<byte[]> readAll();

  /**
   * @param data
   * @return Try.Success - append succeeded; Try.Failure - wrapped cause
   */
  public Try<Boolean> append(long pos, byte[] data);
  
  /**
   * open and keep open session for repeated reads
   */
  public Try<DelaReadStream> readStream();

  /**
   * open and keep open session for repeated writes
   */
  public Try<DelaAppendStream> appendStream(long appendSize, Consumer<Try<Boolean>> completed);
}
