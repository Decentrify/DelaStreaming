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

import java.util.function.BiFunction;
import java.util.function.Supplier;
import se.sics.dela.storage.StorageResource;
import se.sics.ktoolbox.util.trysf.Try;
import se.sics.ktoolbox.util.trysf.TryHelper;
import se.sics.nstream.util.range.KBlockImpl;
import se.sics.nstream.util.range.KRange;

/**
 *
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class DelaHelper {
  public static BiFunction<Long, Throwable, Try<KRange>> fullRange(StorageType storageType, StorageResource resource) {
    return TryHelper.tryFSucc1((Long fileSize) -> {
      if (fileSize > Integer.MAX_VALUE) {
        String msg = "file:" + resource.getSinkName() + " is too big to read fully";
        return new Try.Failure(new DelaStorageException(msg, storageType));
      } else {
        return new Try.Success(new KBlockImpl(0, 0, fileSize));
      }
    });
  }
  
  public static <I, O> BiFunction<I, Throwable, Try<O>> recoverFrom(StorageType storageType, String cause, 
    Supplier<Try<O>> recoverWith) {
    return TryHelper.tryFFail((Throwable ex) -> {
      if (ex instanceof DelaStorageException && cause.equals(ex.getMessage()) 
        && storageType.equals(((DelaStorageException)ex).storageType)) {
        return recoverWith.get();
      }
      return new Try.Failure(ex);
    });
  }
}
