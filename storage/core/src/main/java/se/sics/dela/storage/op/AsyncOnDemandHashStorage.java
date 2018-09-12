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
package se.sics.dela.storage.op;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;
import org.javatuples.Pair;
import se.sics.dela.storage.StreamStorage;
import se.sics.dela.storage.cache.KHint;
import se.sics.kompics.util.Identifier;
import se.sics.ktoolbox.util.managedStore.core.util.HashUtil;
import se.sics.ktoolbox.util.reference.KReference;
import se.sics.ktoolbox.util.reference.KReferenceException;
import se.sics.ktoolbox.util.reference.KReferenceFactory;
import se.sics.ktoolbox.util.result.DelayedExceptionSyncHandler;
import se.sics.ktoolbox.util.result.Result;
import se.sics.ktoolbox.util.trysf.Try;
import se.sics.nstream.StreamId;
import se.sics.nstream.util.BlockHelper;
import se.sics.nstream.util.FileBaseDetails;
import se.sics.nstream.util.range.KBlock;
import se.sics.nstream.util.range.KRange;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class AsyncOnDemandHashStorage implements AsyncStorage {

  private final FileBaseDetails fileDetails;
  private final Pair<StreamId, StreamStorage> stream;
  private final DelayedExceptionSyncHandler exSyncHandler;
  private final Map<Integer, KReference<byte[]>> hashes = new HashMap<>();
  private final AsyncStorage storage; //someone else is controlling it, I am merely piggy backing

  public AsyncOnDemandHashStorage(FileBaseDetails fileDetails, DelayedExceptionSyncHandler exSyncHandler,
    AsyncStorage storage, Pair<StreamId, StreamStorage> stream) {
    this.fileDetails = fileDetails;
    this.exSyncHandler = exSyncHandler;
    this.storage = storage;
    this.stream = stream;
  }

  @Override
  public void start() {
  }

  @Override
  public boolean isIdle() {
    return true;
  }

  @Override
  public void close() {
    for (KReference<byte[]> hash : hashes.values()) {
      try {
        hash.release();
      } catch (KReferenceException ex) {
        exSyncHandler.fail(Result.internalFailure(ex));
        throw new RuntimeException(ex);
      }
    }
  }

  //**************************************************************************
  @Override
  public void clean(Identifier reader) {
    storage.clean(reader);
  }

  @Override
  public void setFutureReads(Identifier reader, KHint.Expanded hint) {
//        storage.setFutureReads(reader, hint);
  }

  //**************************************************************************
  @Override
  public void read(final KRange readRange, Consumer<Try<KReference<byte[]>>> callback) {
    KReference<byte[]> hash = hashes.get(readRange.parentBlock());
    if (hash != null) {
      callback.accept(new Try.Success(hash));
    } else {
      Consumer<Try<KReference<byte[]>>> readBlockCallback = (result) -> {
        if (result.isSuccess()) {
          byte[] block = result.get().getValue().get();
          KReference<byte[]> computedHash 
            = KReferenceFactory.getReference(HashUtil.makeHash(block, fileDetails.hashAlg));
          hashes.put(readRange.parentBlock(), computedHash);
          callback.accept(new Try.Success(computedHash));
        } else {
          callback.accept(result);
        }
      };
      KBlock blockRange = BlockHelper.getBlockRange(readRange.parentBlock(), fileDetails);
      storage.read(blockRange, readBlockCallback);
    }
  }

  @Override
  public void write(KBlock writeRange, KReference<byte[]> val, Consumer<Try<Boolean>> callback) {
    val.retain();
    hashes.put(writeRange.parentBlock(), val);
    callback.accept(new Try.Success(true));
  }
}
