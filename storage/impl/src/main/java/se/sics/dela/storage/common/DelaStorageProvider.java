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
package se.sics.dela.storage.common;

import se.sics.dela.storage.StorageEndpoint;
import se.sics.dela.storage.StorageResource;
import se.sics.dela.util.TimerProxy;
import se.sics.ktoolbox.util.trysf.Try;
import se.sics.nstream.util.range.KRange;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public interface DelaStorageProvider<E extends StorageEndpoint, R extends StorageResource> {
  
  public E getEndpoint();
  
  public R getResource();
  
  /**
   *
   * @return Try.Success - true - path created, false - path already existed
   * Try.Failure - wrapped cause
   */
  public Try<Boolean> createPath();

  /**
   * @return Try.Success - true - file exists, false - file does not exist; Try.Failure - wrapped cause
   */
  public Try<Boolean> fileExists();

  /**
   * @return Try.Success - true - file created, false - file already exists; Try.Failure - wrapped cause
   */
  public Try<Boolean> createFile();

  /**
   * @return Try.Success - true - file deleted, false - file already deleted; Try.Failure - wrapped cause
   */
  public Try<Boolean> deleteFile();

  /**
   *
   * @return Try.Success - long as size; Try.Failure - wrapped cause
   */
  public Try<Long> fileSize();

  /**
   * @param range
   * @return Try.Success - content; Try.Failure - wrapped cause
   */
  public Try<byte[]> read(KRange range);

  /**
   *
   * @return Try.Success - content; Try.Failure - wrapped cause
   */
  public Try<byte[]> readAllFile();

  /**
   * @param data
   * @return Try.Success - append succeeded; Try.Failure - wrapped cause
   */
  public Try<Boolean> append(byte[] data);
  
  /**
   * open and keep open session for repeated reads
   */
  public Try<DelaReadStream> readSession(TimerProxy timer);

  /**
   * open and keep open session for repeated reads
   */
  public Try<DelaAppendStream> appendSession(TimerProxy timer);
}
