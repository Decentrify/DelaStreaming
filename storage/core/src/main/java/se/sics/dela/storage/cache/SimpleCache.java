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
package se.sics.dela.storage.cache;

import com.google.common.collect.Sets;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.function.Consumer;
import org.javatuples.Pair;
import org.slf4j.Logger;
import se.sics.dela.storage.operation.StreamStorageOpProxy;
import se.sics.dela.util.TimerProxy;
import se.sics.dela.util.TimerProxyImpl;
import se.sics.kompics.config.Config;
import se.sics.kompics.util.Identifier;
import se.sics.ktoolbox.util.reference.KReference;
import se.sics.ktoolbox.util.reference.KReferenceException;
import se.sics.ktoolbox.util.reference.KReferenceFactory;
import se.sics.ktoolbox.util.trysf.Try;
import se.sics.nstream.StreamId;
import se.sics.nstream.util.range.KBlock;
import se.sics.nstream.util.range.KPiece;
import se.sics.nstream.util.range.KRange;
import se.sics.nstream.util.range.RangeKReference;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class SimpleCache implements KCache {

  private final KCacheConfig cacheConfig;
  private final StreamId streamId;
  private final StreamStorageOpProxy opProxy;
  private final TimerProxy timerProxy;
  //blocks maintained by actual cache reads
  final TreeMap<Long, Pair<KBlock, CacheKReference>> cacheRef = new TreeMap<>();
  //blocks in the system, might as well cache them until no one else uses them
  final Map<Long, Pair<KBlock, KReference<byte[]>>> systemRef = new HashMap<>();
  //metadata about what blocks are cached for particular readers
  final Map<Identifier, ReaderHead> readerHeads = new HashMap<>();
  //<readPos, <readRange, list<readerHeads>>
  final TreeMap<Long, Pair<KBlock, List<Identifier>>> pendingCache = new TreeMap<>();
  final Map<Long, List<Pair<KRange, Consumer<Try<KReference<byte[]>>>>>> delayedReads = new HashMap<>();
  private UUID cacheCleanTid;
  private final Logger logger;

  public SimpleCache(Config config, StreamStorageOpProxy opProxy, TimerProxy timerProxy,
    StreamId streamId, Logger logger) {
    this.cacheConfig = new KCacheConfig(config);
    this.opProxy = opProxy;
    this.timerProxy = timerProxy;
    this.streamId = streamId;
    this.logger = logger;
  }

  //********************************CONTROL***********************************
  @Override
  public void start() {
    cacheCleanTid = timerProxy.schedulePeriodicTimer(
      cacheConfig.extendedCacheCleanPeriod,
      cacheConfig.extendedCacheCleanPeriod,
      cacheClean());
  }

  private Consumer<Boolean> cacheClean() {
    return (nothing) -> {
      Iterator<Map.Entry<Long, Pair<KBlock, KReference<byte[]>>>> it1 = systemRef.entrySet().iterator();
      while (it1.hasNext()) {
        KReference<byte[]> sysRef = it1.next().getValue().getValue1();
        if (!sysRef.isValid()) {
          it1.remove();
        }
      }
    };
  }

  @Override
  public boolean isIdle() {
    return true;
  }

  @Override
  public void close() {
    timerProxy.cancelPeriodicTimer(cacheCleanTid);
    clean();
  }

  //***************************************************************************
  //this should not happen - OUR problem - cRef problem
  private void refProblem(KReferenceException ex) {
    throw new RuntimeException("OUR problem - cRef problem", ex);
  }

  private void refProblem() {
    throw new RuntimeException("OUR problem - cRef problem");
  }

  //*********************************HINTS************************************
  @Override
  public void setFutureReads(Identifier reader, KHint.Expanded hint) {
    ReaderHead rh = readerHeads.get(reader);
    if (rh == null) {
      rh = new ReaderHead(logger);
      readerHeads.put(reader, rh);
    }

    Pair<Map<Long, KBlock>, Set<Long>> processedHint = rh.processHint(hint);

    //clean invalidated cRefs
    processedHint.getValue1().forEach((pos) -> cacheClean(pos));

    //get new blocks
    for (Map.Entry<Long, KBlock> f : processedHint.getValue0().entrySet()) {
      long blockPos = f.getKey();
      if (checkCache(rh, blockPos)) {
        continue;
      }
      if (checkSystem(rh, blockPos)) {
        continue;
      }
      addToPendingFetch(reader, f.getValue());
    }
  }

  private boolean checkCache(ReaderHead rh, long blockPos) {
    Pair<KBlock, CacheKReference> cached = cacheRef.get(blockPos);
    if (cached != null) {
      CacheKReference cRef = cached.getValue1();
      checkCRef(cRef);
      rh.add(blockPos, cRef);
      return true;
    }
    return false;
  }

  private boolean checkSystem(ReaderHead rh, long blockPos) {
    //if it exists, we will move it to cache
    Pair<KBlock, KReference<byte[]>> cached = systemRef.remove(blockPos);
    if (cached != null) {
      KReference<byte[]> base = cached.getValue1();
      if (base.retain()) {
        CacheKReference cRef = CacheKReference.createInstance(base);
        rh.add(blockPos, cRef);
        cacheRef.put(blockPos, Pair.with(cached.getValue0(), cRef));
        silentRelease(cRef);
        silentRelease(base);
        return true;
      }
    }
    return false;
  }

  private void addToPendingFetch(Identifier reader, KBlock blockRange) {
    long blockPos = blockRange.lowerAbsEndpoint();
    Pair<KBlock, List<Identifier>> cacheFetch = pendingCache.get(blockPos);
    if (cacheFetch == null) {
      List<Identifier> rhList = new LinkedList<>();
      cacheFetch = Pair.with(blockRange, rhList);
      pendingCache.put(blockPos, cacheFetch);
      opProxy.read(streamId, blockRange, readCallback(blockRange));
    }
    cacheFetch.getValue1().add(reader);
  }

  private Consumer<Try<byte[]>> readCallback(KBlock blockRange) {
    return (result) -> {
      long blockPos = blockRange.lowerAbsEndpoint();
      //add cache read to waiting readHeads
      Pair<KBlock, List<Identifier>> waitingHeads = pendingCache.remove(blockPos);
      //checking waiting reads
      List<Pair<KRange, Consumer<Try<KReference<byte[]>>>>> waitingReads = delayedReads.remove(blockPos);

      if (result.isSuccess()) {
        //create cache ref
        KReference<byte[]> base = KReferenceFactory.getReference(result.get());
        CacheKReference cRef = CacheKReference.createInstance(base);

        if (waitingHeads != null) {
          waitingHeads.getValue1().stream()
            .map((headId) -> readerHeads.get(headId))
            .filter((head) -> head != null) //might have been closed while waiting for read
            .forEach((head) -> head.add(blockPos, cRef));
        }

        if (waitingReads != null) {
          //we check correct range enclosing when we add to waiting reads
          waitingReads
            .forEach((callback) -> readFromBlock(blockPos, callback.getValue0(), base, callback.getValue1()));
        }

        silentRelease(base);
        silentRelease(cRef);
        if (cRef.isValid()) { //some reader head retained it
          cacheRef.put(blockPos, Pair.with(blockRange, cRef));
        } else if (base.isValid()) { //someone in the system retained it
          systemRef.put(blockPos, Pair.with(blockRange, base));
        }
      } else {
        Try.Failure cause = (Try.Failure) result;
        if (waitingReads != null) {
          waitingReads.forEach((callback) -> callback.getValue1().accept(cause));
        }
      }
    };
  }

  @Override
  public void clean(Identifier reader) {
    ReaderHead rh = readerHeads.remove(reader);
    try {
      rh.releaseAll();
    } catch (KReferenceException ex) {
      refProblem(ex);
      return;
    }
    //the pendingCacheFetches are cleaned when the answers return - no need to clean that here
  }

  //**************************************************************************
  @Override
  public void buffered(KBlock writeRange, KReference<byte[]> ref) {
    systemRef.put(writeRange.lowerAbsEndpoint(), Pair.with(writeRange, ref));
  }

  /**
   * should always hit the cache, or have an outstanding external resource
   * read, as it was preceeded by the appropriate readHint
   * <p>
   */
  @Override
  public void read(KRange readRange, Consumer<Try<KReference<byte[]>>> callback) {
    if (!(readRange instanceof KBlock || readRange instanceof KPiece)) {
      //only blocks or pieces are allowed
      throw new IllegalArgumentException("only blocks or pieces are allowed");
    }

    //check cache
    Map.Entry<Long, Pair<KBlock, CacheKReference>> cached = cacheRef.floorEntry(readRange.lowerAbsEndpoint());
    if (cached != null) {
      KBlock blockRange = cached.getValue().getValue0();
      long blockPos = blockRange.lowerAbsEndpoint();
      CacheKReference cRef = cached.getValue().getValue1();
      checkCRef(cRef);

      if (blockRange.encloses(readRange)) {
        KReference<byte[]> base = cRef.value();
        //being enclosed by a valid cRef, base will remain valid
        readFromBlock(blockPos, readRange, base, callback);
        return;
      }
      if (blockRange.isConnected(readRange)) {
        //not our fault - external Block/Piece problem
        throw new IllegalArgumentException("external Block/Piece problem");
      }
    }

    //check that there is an outstanding external resource read
    Map.Entry<Long, Pair<KBlock, List<Identifier>>> pending = pendingCache.floorEntry(readRange.lowerAbsEndpoint());
    if (pending == null) {
      //not our fault - external Read/Hint problem
      throw new IllegalArgumentException("external Read/Hint problem");
    }
    KBlock blockRange = pending.getValue().getValue0();
    if (!blockRange.encloses(readRange)) {
      //not our fault - external Read/Hint problem or Block/Piece problem
      throw new IllegalArgumentException(
        "external Read/Hint problem or Block/Piece problem");
    }
    long blockPos = blockRange.lowerAbsEndpoint();
    //add read to pending until external resource read completes
    List<Pair<KRange, Consumer<Try<KReference<byte[]>>>>> pendingReads = delayedReads.get(blockPos);
    if (pendingReads == null) {
      pendingReads = new LinkedList<>();
      delayedReads.put(blockPos, pendingReads);
    }
    pendingReads.add(Pair.with(readRange, callback));
  }

  /**
   * expect call for a invalid cache entry at pos
   *
   * @param pos
   */
  private void cacheClean(long pos) {
    Pair<KBlock, CacheKReference> cached = cacheRef.remove(pos);
    if (cached == null) {
      return;
    }
    CacheKReference cRef = cached.getValue1();
    KBlock blockRange = cached.getValue0();
    if (cRef.isValid()) {
      // should not happen. If testing we should detect this and fix it. Nothing is actually broken so we can continue
      assert true == false;
      cacheRef.put(pos, cached);
      return;
    }
    KReference<byte[]> base = cRef.getValue().get();
    if (base.isValid()) {
      if (base.retain()) {
        systemRef.put(pos, Pair.with(blockRange, base));
        silentRelease(base);
      }
    }
  }

  private void clean() {
    pendingCache.clear();
    delayedReads.clear();
    for (ReaderHead rh : readerHeads.values()) {
      try {
        rh.releaseAll();
      } catch (KReferenceException ex) {
        refProblem(ex);
        return;
      }
    }
    readerHeads.clear();
    for (Pair<KBlock, CacheKReference> cached : cacheRef.values()) {
      if (cached.getValue1().isValid()) {
        refProblem();
        return;
      }
    }
    cacheRef.clear();
    systemRef.clear();
  }

  private void readFromBlock(long blockPos, KRange readRange, KReference<byte[]> base,
    Consumer<Try<KReference<byte[]>>> callback) {
    if (readRange instanceof KBlock) {
      //base is enclosed by a cRef - so it is valid
      callback.accept(new Try.Success(base));
    } else if (readRange instanceof KPiece) {
      KReference<byte[]> piece = RangeKReference.createInstance(base, blockPos, (KPiece) readRange);
      callback.accept(new Try.Success(piece));
      silentRelease(piece);
    }
  }

  private void checkCRef(CacheKReference cRef) {
    if (!cRef.isValid()) {
      refProblem();
    }
  }

  /**
   * we assume the base was retained before correctly
   *
   * @param base
   */
  private void silentRelease(KReference<byte[]> base) {
    try {
      base.release();
    } catch (KReferenceException ex) {
      refProblem(ex);
    }
  }

  private void silentRelease(CacheKReference cRef) {
    try {
      cRef.release();
    } catch (KReferenceException ex) {
      refProblem(ex);
    }
  }

  @Override
  public KCacheReport report() {
    return new SimpleCacheReport(cacheRef.size(), systemRef.size());
  }

  static class ReaderHead {

    public long hintLStamp = 0;
    public final Set<Long> pending = new HashSet<>();
    public final Map<Long, CacheKReference> cache = new HashMap<>();
    public final Logger logger;

    public ReaderHead(Logger logger) {
      this.logger = logger;
    }

    public void add(long pos, CacheKReference sRef) {
      if (pending.remove(pos)) {
        sRef.retain();
        cache.put(pos, sRef);
      }
    }

    public void releaseAll() throws KReferenceException {
      for (CacheKReference sRef : cache.values()) {
        sRef.release();
      }
      pending.clear();
      cache.clear();
    }

    /**
     *
     * @param hint
     * @return <fetchMap, cleanSet>
     * @throws KReferenceException
     */
    public Pair<Map<Long, KBlock>, Set<Long>> processHint(KHint.Expanded hint) {
      Map<Long, KBlock> fetch = new HashMap<>();
      Set<Long> clean = new HashSet<>();
      if (hint.lStamp <= hintLStamp) {
        return Pair.with(fetch, clean);
      }
      hintLStamp = hint.lStamp;

      Set<Long> release1 = new HashSet<>(Sets.difference(pending, hint.futureReads.keySet()));
      release1.forEach((pos) -> {
        CacheKReference ref = cache.remove(pos);
        try {
          ref.release();
          if (!ref.isValid()) {
            clean.add(pos);
          }
        } catch (KReferenceException ex) {
          logger.warn("non critical - reference counting logic issue in:{}", this.getClass());
          //some logic is bad somewhere - but it should be ok here.
        }
      });

      Set<Long> release2 = new HashSet<>(Sets.difference(cache.keySet(), hint.futureReads.keySet()));
      release2.forEach((pos) -> pending.remove(pos));

      Sets.difference(hint.futureReads.keySet(), cache.keySet()).stream()
        .forEach((pos) -> {
          pending.add(pos);
          fetch.put(pos, hint.futureReads.get(pos));
        });
      return Pair.with(fetch, clean);
    }
  }
}
