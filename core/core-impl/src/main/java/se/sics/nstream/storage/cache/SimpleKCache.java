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
package se.sics.nstream.storage.cache;

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
import org.javatuples.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.kompics.ComponentProxy;
import se.sics.kompics.Handler;
import se.sics.kompics.Positive;
import se.sics.kompics.config.Config;
import se.sics.kompics.timer.CancelPeriodicTimeout;
import se.sics.kompics.timer.SchedulePeriodicTimeout;
import se.sics.kompics.timer.Timeout;
import se.sics.kompics.timer.Timer;
import se.sics.ktoolbox.util.identifiable.Identifier;
import se.sics.ktoolbox.util.reference.KReference;
import se.sics.ktoolbox.util.reference.KReferenceException;
import se.sics.ktoolbox.util.reference.KReferenceFactory;
import se.sics.ktoolbox.util.result.DelayedExceptionSyncHandler;
import se.sics.ktoolbox.util.result.Result;
import se.sics.nstream.StreamId;
import se.sics.nstream.storage.durable.DStoragePort;
import se.sics.nstream.storage.durable.events.DStorageRead;
import se.sics.nstream.storage.durable.util.MyStream;
import se.sics.nstream.util.actuator.ComponentLoadTracking;
import se.sics.nstream.util.range.KBlock;
import se.sics.nstream.util.range.KPiece;
import se.sics.nstream.util.range.KRange;
import se.sics.nstream.util.range.RangeKReference;
import se.sics.nstream.util.result.ReadCallback;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class SimpleKCache implements KCache {

    private static final Logger LOG = LoggerFactory.getLogger(SimpleKCache.class);
    private String logPrefix = "";

    private final KCacheConfig cacheConfig;
    private final Pair<StreamId, MyStream> stream;
    //**************************************************************************
    private final Positive<DStoragePort> readPort;
    private final Positive<Timer> timerPort;
    private final ComponentProxy proxy;
    private final DelayedExceptionSyncHandler syncExHandling;
    private final ComponentLoadTracking loadTracker;
    //**************************************************************************
    //blocks maintained by actual cache reads
    final TreeMap<Long, Pair<KBlock, CacheKReference>> cacheRef = new TreeMap<>();
    //blocks in the system, might as well cache them until no one else uses them
    final Map<Long, Pair<KBlock, KReference<byte[]>>> systemRef = new HashMap<>();
    //metadata about what blocks are cached for particular readers
    final Map<Identifier, ReaderHead> readerHeads = new HashMap<>();
    //**************************************************************************
    //<readPos, <readRange, list<readerHeads>>
    final TreeMap<Long, Pair<KBlock, List<Identifier>>> pendingCacheFetch = new TreeMap<>();
    final Map<Long, List<Pair<KRange, ReadCallback>>> delayedReads = new HashMap<>();
    //**************************************************************************
    private UUID extendedCacheCleanTid;

    public SimpleKCache(Config config, ComponentProxy proxy, DelayedExceptionSyncHandler syncExHandling, ComponentLoadTracking loadTracker,
            Pair<StreamId, MyStream> stream) {
        this.cacheConfig = new KCacheConfig(config);
        this.proxy = proxy;
        this.stream = stream;
        this.syncExHandling = syncExHandling;
        this.loadTracker = loadTracker;
        this.readPort = proxy.getNegative(DStoragePort.class).getPair();
        this.timerPort = proxy.getNegative(Timer.class).getPair();
        this.proxy.subscribe(handleExtendedCacheClean, timerPort);
        this.proxy.subscribe(handleReadResp, readPort);
        this.logPrefix = "<" + stream.getValue1().endpoint.getEndpointName() + ":" + stream.getValue1().resource.getSinkName() + ">";
        LOG.info("{}initiating...", logPrefix);
    }

    //********************************CONTROL***********************************
    @Override
    public void start() {
        LOG.info("{}starting...", logPrefix);
        proxy.trigger(scheduleExtendedCacheClean(), timerPort);
    }

    private SchedulePeriodicTimeout scheduleExtendedCacheClean() {
        SchedulePeriodicTimeout spt = new SchedulePeriodicTimeout(cacheConfig.extendedCacheCleanPeriod, cacheConfig.extendedCacheCleanPeriod);
        ExtendedCacheClean ecc = new ExtendedCacheClean(spt);
        spt.setTimeoutEvent(ecc);
        extendedCacheCleanTid = ecc.getTimeoutId();
        return spt;
    }

    @Override
    public boolean isIdle() {
        return true;
    }

    @Override
    public void close() {
        CancelPeriodicTimeout cpt = new CancelPeriodicTimeout(extendedCacheCleanTid);
        proxy.trigger(cpt, timerPort);

        proxy.unsubscribe(handleExtendedCacheClean, timerPort);
        proxy.unsubscribe(handleReadResp, readPort);

        Result cleanResult = clean();
        if (!cleanResult.isSuccess()) {
            syncExHandling.fail(cleanResult);
        }
    }

    //*********************************HINTS************************************
    @Override
    public void setFutureReads(Identifier reader, KHint.Expanded hint) {
        ReaderHead rh = readerHeads.get(reader);
        if (rh == null) {
            rh = new ReaderHead();
            readerHeads.put(reader, rh);
        }

        Pair<Map<Long, KBlock>, Set<Long>> processedHint;
        try {
            processedHint = rh.processHint(hint);
        } catch (KReferenceException ex) {
            //OUR fault - cRef problem - ReaderHeads/cRef
            RuntimeException crashingException = new IllegalStateException("OUR cRef problem");
            fail(Result.internalFailure(crashingException));
            //crash marker - things will already crash here, but this is hidden from java so...
            throw crashingException;
        }

        //clean invalidated cRefs
        for (Long pos : processedHint.getValue1()) {
            cacheClean(pos);
        }

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
        Pair<KBlock, List<Identifier>> cacheFetch = pendingCacheFetch.get(blockPos);
        if (cacheFetch == null) {
            List<Identifier> rhList = new LinkedList<>();
            cacheFetch = Pair.with(blockRange, rhList);
            pendingCacheFetch.put(blockPos, cacheFetch);
            //read from external resource
            proxy.trigger(new DStorageRead.Request(stream.getValue0(), blockRange), readPort);
        }
        cacheFetch.getValue1().add(reader);
    }

    @Override
    public void clean(Identifier reader) {
        ReaderHead rh = readerHeads.remove(reader);
        try {
            rh.releaseAll();
        } catch (KReferenceException ex) {
            //should not happen - if it does, it is OUR fault - bad internal cRef manipulation
            RuntimeException crashingException = new IllegalStateException("bad internal cache ref manipulation");
            fail(Result.internalFailure(crashingException));
            //crash marker - things will already crash here, but this is hidden from java so...
            throw crashingException;
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
     *
     */
    @Override
    public void read(KRange readRange, ReadCallback delayedResult) {
        if (!(readRange instanceof KBlock || readRange instanceof KPiece)) {
            RuntimeException crashingException = new IllegalArgumentException("only blocks or pieces are allowed");
            fail(Result.internalFailure(crashingException));
            //crash marker - things will already crash here, but this is hidden from java so...
            throw crashingException;
        }

        loadTracker.setCacheSize(stream, cacheRef.size(), systemRef.size());

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
                readFromBlock(blockPos, readRange, base, delayedResult);
                return;
            }
            if (blockRange.isConnected(readRange)) {
                //not our fault - external Block/Piece problem
                RuntimeException crashingException = new IllegalArgumentException("external Block/Piece problem");
                fail(Result.internalFailure(crashingException));
                //crash marker - things will already crash here, but this is hidden from java so...
                throw crashingException;
            }
        }

        //check that there is an outstanding external resource read
        Map.Entry<Long, Pair<KBlock, List<Identifier>>> pending = pendingCacheFetch.floorEntry(readRange.lowerAbsEndpoint());
        if (pending == null) {
            //not our fault - external Read/Hint problem
            RuntimeException crashingException = new IllegalArgumentException("external Read/Hint problem");
            fail(Result.internalFailure(crashingException));
            //crash marker - things will already crash here, but this is hidden from java so...
            throw crashingException;
        }
        KBlock blockRange = pending.getValue().getValue0();
        if (!blockRange.encloses(readRange)) {
            //not our fault - external Read/Hint problem or Block/Piece problem
            RuntimeException crashingException = new IllegalArgumentException("external Read/Hint problem or Block/Piece problem");
            fail(Result.internalFailure(crashingException));
            //crash marker - things will already crash here, but this is hidden from java so...
            throw crashingException;
        }
        long blockPos = blockRange.lowerAbsEndpoint();
        //add read to pending until external resource read completes
        List<Pair<KRange, ReadCallback>> pendingReads = delayedReads.get(blockPos);
        if (pendingReads == null) {
            pendingReads = new LinkedList<>();
            delayedReads.put(blockPos, pendingReads);
        }
        pendingReads.add(Pair.with(readRange, delayedResult));
    }

    Handler handleExtendedCacheClean = new Handler<ExtendedCacheClean>() {
        @Override
        public void handle(ExtendedCacheClean event) {
            LOG.trace("{}extended cache clean", logPrefix);
            Iterator<Map.Entry<Long, Pair<KBlock, KReference<byte[]>>>> it1 = systemRef.entrySet().iterator();
            while (it1.hasNext()) {
                Map.Entry<Long, Pair<KBlock, KReference<byte[]>>> next = it1.next();
                KReference<byte[]> systemRef = next.getValue().getValue1();
                if (!systemRef.isValid()) {
                    it1.remove();
                } else {
                    LOG.info("{}ref count", logPrefix);
                }
            }
            LOG.info("{}cache size - cache ref:{}, system ref:{}", new Object[]{logPrefix, cacheRef.size(), systemRef.size()});
        }
    };

    Handler handleReadResp = new Handler<DStorageRead.Response>() {
        @Override
        public void handle(DStorageRead.Response resp) {
            LOG.debug("{}received:{}", logPrefix, resp);
            loadTracker.setCacheSize(stream, cacheRef.size(), systemRef.size());
            if (resp.result.isSuccess()) {
                KBlock blockRange = resp.req.readRange;
                long blockPos = blockRange.lowerAbsEndpoint();
                //create cache ref
                KReference<byte[]> base = KReferenceFactory.getReference(resp.result.getValue());
                CacheKReference cRef = CacheKReference.createInstance(base);

                //add cache read to waiting readHeads
                Pair<KBlock, List<Identifier>> waitingH = pendingCacheFetch.remove(blockPos);
                if (waitingH != null) {
                    for (Identifier headId : waitingH.getValue1()) {
                        ReaderHead rh = readerHeads.get(headId);
                        if (rh != null) {
                            //might have been closed while waiting for read
                            rh.add(blockPos, cRef);
                        }
                    }
                }

                //checking waiting reads
                List<Pair<KRange, ReadCallback>> waitingReads = delayedReads.remove(blockPos);
                if (waitingReads != null) {
                    for (Pair<KRange, ReadCallback> wR : waitingReads) {
                        //we check correct range enclosing when we add to waiting reads
                        readFromBlock(blockPos, wR.getValue0(), base, wR.getValue1());
                    }
                }

                silentRelease(base);
                silentRelease(cRef);
                if (cRef.isValid()) {
                    //some reader head retained it
                    cacheRef.put(blockPos, Pair.with(blockRange, cRef));
                } else if (base.isValid()) {
                    //someone in the system retained it
                    systemRef.put(blockPos, Pair.with(blockRange, base));
                }
            } else {
                fail(resp.result);
            }
        }
    };

    private void fail(Result result) {
        Result cleanResult = clean();
        syncExHandling.fail(result);
        if (!cleanResult.isSuccess()) {
            syncExHandling.fail(cleanResult);
        }
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

    private Result clean() {
        pendingCacheFetch.clear();
        delayedReads.clear();
        for (ReaderHead rh : readerHeads.values()) {
            try {
                rh.releaseAll();
            } catch (KReferenceException ex) {
                //this should not happen - OUR problem - cRef problem
                RuntimeException crashingException = new IllegalStateException("OUR problem - cRef problem");
                return Result.internalFailure(crashingException);
            }
        }
        readerHeads.clear();
        for (Pair<KBlock, CacheKReference> cached : cacheRef.values()) {
            if (cached.getValue1().isValid()) {
                //this should not happen - OUR problem - cRef problem
                RuntimeException crashingException = new IllegalStateException("OUR problem - cRef problem");
                return Result.internalFailure(crashingException);
            }
        }
        cacheRef.clear();
        systemRef.clear();
        return Result.success(true);
    }

    private void readFromBlock(long blockPos, KRange readRange, KReference<byte[]> base, ReadCallback delayedResult) {
        if (readRange instanceof KBlock) {
            //base is enclosed by a cRef - so it is valid
            delayedResult.success(Result.success(base));
        } else if (readRange instanceof KPiece) {
            KReference<byte[]> piece = RangeKReference.createInstance(base, blockPos, (KPiece) readRange);
            delayedResult.success(Result.success(piece));
            silentRelease(piece);
        }
    }

    private void checkCRef(CacheKReference cRef) {
        if (!cRef.isValid()) {
            //OUR fault - cRef problem - ReaderHead/cRef
            RuntimeException crashingException = new IllegalStateException("OUR cRef problem");
            fail(Result.internalFailure(crashingException));
            //crash marker - things will already crash here, but this is hidden from java so...
            throw crashingException;
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
            //not our fault - external ref problem - someone double release
            RuntimeException crashingException = new IllegalStateException("external ref problem - someone double release");
            fail(Result.internalFailure(crashingException));
            //crash marker - things will already crash here, but this is hidden from java so...
            throw crashingException;
        }
    }

    private void silentRelease(CacheKReference cRef) {
        try {
            cRef.release();
        } catch (KReferenceException ex) {
            //OUR fault - cRef problem
            RuntimeException crashingException = new IllegalStateException("OUR cRef problem");
            fail(Result.internalFailure(crashingException));
            //crash marker - things will already crash here, but this is hidden from java so...
            throw crashingException;
        }
    }

    @Override
    public KCacheReport report() {
        return new SimpleKCacheReport(cacheRef.size(), systemRef.size());
    }

    public static class ExtendedCacheClean extends Timeout {

        public ExtendedCacheClean(SchedulePeriodicTimeout spt) {
            super(spt);
        }
    }

    private static class ReaderHead {

        public long hintLStamp;
        public final Set<Long> preCaching = new HashSet<>();
        public final Map<Long, CacheKReference> caching = new HashMap<>();

        public void add(long pos, CacheKReference sRef) {
            if (preCaching.remove(pos)) {
                sRef.retain();
                caching.put(pos, sRef);
            }
        }

        public void releaseAll() throws KReferenceException {
            for (CacheKReference sRef : caching.values()) {
                sRef.release();
            }
            preCaching.clear();
            caching.clear();
        }

        /**
         *
         * @param hint
         * @return <fetchMap, cleanSet>
         * @throws KReferenceException
         */
        public Pair<Map<Long, KBlock>, Set<Long>> processHint(KHint.Expanded hint) throws KReferenceException {
            Map<Long, KBlock> fetchMap = new HashMap<>();
            Set<Long> cleanSet = new HashSet<>();
            if (hint.lStamp <= hintLStamp) {
                return Pair.with(fetchMap, cleanSet);
            }

            Set<Long> releaseSet = new HashSet<>(Sets.difference(caching.keySet(), hint.futureReads.keySet()));
            releaseSet.addAll(Sets.difference(preCaching, hint.futureReads.keySet()));
            Set<Long> fetchSet = new HashSet<>(Sets.difference(hint.futureReads.keySet(), caching.keySet()));

            for (Long pos : releaseSet) {
                preCaching.remove(pos);
                CacheKReference sRef = caching.remove(pos);
                if (sRef != null) {
                    sRef.release();
                    if (!sRef.isValid()) {
                        cleanSet.add(pos);
                    }
                }
            }

            for (Long pos : fetchSet) {
                preCaching.add(pos);
                fetchMap.put(pos, hint.futureReads.get(pos));
            }
            return Pair.with(fetchMap, cleanSet);
        }
    }
}
