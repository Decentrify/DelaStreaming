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

import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.kompics.config.Config;
import se.sics.kompics.config.TypesafeConfig;
import se.sics.kompics.timer.CancelPeriodicTimeout;
import se.sics.kompics.timer.SchedulePeriodicTimeout;
import se.sics.kompics.timer.Timer;
import se.sics.ktoolbox.util.identifiable.BasicIdentifiers;
import se.sics.ktoolbox.util.identifiable.Identifier;
import se.sics.ktoolbox.util.identifiable.IdentifierFactory;
import se.sics.ktoolbox.util.identifiable.IdentifierRegistry;
import se.sics.ktoolbox.util.identifiable.basic.IntIdFactory;
import se.sics.ktoolbox.util.identifiable.overlay.OverlayId;
import se.sics.ktoolbox.util.identifiable.overlay.OverlayIdFactory;
import se.sics.ktoolbox.util.identifiable.overlay.OverlayRegistry;
import se.sics.ktoolbox.util.reference.KReference;
import se.sics.ktoolbox.util.reference.KReferenceException;
import se.sics.ktoolbox.util.reference.KReferenceFactory;
import se.sics.ktoolbox.util.result.Result;
import se.sics.ktoolbox.util.test.EventClassValidator;
import se.sics.ktoolbox.util.test.EventContentValidator;
import se.sics.ktoolbox.util.test.MockComponentProxy;
import se.sics.ktoolbox.util.test.MockExceptionHandler;
import se.sics.ktoolbox.util.test.PortValidator;
import se.sics.ktoolbox.util.test.Validator;
import se.sics.nstream.FileId;
import se.sics.nstream.StreamId;
import se.sics.nstream.TorrentIds;
import se.sics.nstream.storage.StorageRead;
import se.sics.nstream.test.MockStreamEndpoint;
import se.sics.nstream.test.MockStreamPort;
import se.sics.nstream.test.MockStreamResource;
import se.sics.nstream.test.StreamReadReqEC;
import se.sics.nstream.util.MyStream;
import se.sics.nstream.util.actuator.ComponentLoadTracking;
import se.sics.nstream.util.range.KBlock;
import se.sics.nstream.util.range.KBlockImpl;
import se.sics.nstream.util.range.KPieceImpl;
import se.sics.nstream.util.range.KRange;
import se.sics.nstream.util.result.ReadCallback;
import se.sics.nutil.tracking.load.QueueLoadConfig;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class TestSimpleKCache {

    private static final Logger LOG = LoggerFactory.getLogger(TestSimpleKCache.class);

    private static MyStream readStream;
    private static Identifier readerId;

    private static KBlock b0, b1, b2;
    private static Map<Long, KBlock> hintE = new TreeMap<>();
    private static Map<Long, KBlock> hint0 = new TreeMap<>();
    private static Map<Long, KBlock> hint0_1 = new TreeMap<>();
    private static Map<Long, KBlock> hint1_2 = new TreeMap<>();
    private static SimpleKCache.ExtendedCacheClean timeout;
    private static byte[] r = new byte[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9};

    private static StreamReadReqEC comparator = new StreamReadReqEC();
    private static StorageRead.Request req0;
    private static StorageRead.Response resp0;
    private static StorageRead.Request req1;
    private static StorageRead.Response resp1;
    private static StorageRead.Request req2;
    private static StorageRead.Response resp2;

    @BeforeClass
    public static void setup() {
        systemSetup();
        experimentSetup();
    }

    private static void systemSetup() {
        TorrentIds.registerDefaults(1234l);
        OverlayRegistry.initiate(new OverlayId.BasicTypeFactory((byte) 0), new OverlayId.BasicTypeComparator());
    }

    private static void experimentSetup() {
        IntIdFactory endpointIdFactory = new IntIdFactory(new Random(1234));
        Identifier endpointId = endpointIdFactory.randomId();

        IdentifierFactory nodeIdFactory = IdentifierRegistry.lookup(BasicIdentifiers.Values.NODE.toString());
        readerId = nodeIdFactory.randomId();

        byte owner = 1;
        IdentifierFactory baseIdFactory = IdentifierRegistry.lookup(BasicIdentifiers.Values.OVERLAY.toString());
        OverlayIdFactory overlayIdFactory = new OverlayIdFactory(baseIdFactory, OverlayId.BasicTypes.OTHER, owner);

        OverlayId torrentId = overlayIdFactory.randomId();
        FileId fileId = TorrentIds.fileId(torrentId, 1);
        StreamId streamId = TorrentIds.streamId(endpointId, fileId);

        MockStreamEndpoint readEndpoint = new MockStreamEndpoint();
        MockStreamResource readResource = new MockStreamResource("mock1");
        readStream = new MyStream(streamId, readEndpoint, readResource);

        SchedulePeriodicTimeout spt = new SchedulePeriodicTimeout(0, 0);
        timeout = new SimpleKCache.ExtendedCacheClean(spt);

        b0 = new KBlockImpl(0, 0, 9);
        b1 = new KBlockImpl(1, 10, 19);
        b2 = new KBlockImpl(2, 20, 29);
        hint0.put(b0.lowerAbsEndpoint(), b0);
        hint0_1.put(b0.lowerAbsEndpoint(), b0);
        hint0_1.put(b1.lowerAbsEndpoint(), b1);
        hint1_2.put(b1.lowerAbsEndpoint(), b1);
        hint1_2.put(b2.lowerAbsEndpoint(), b2);

        req0 = new StorageRead.Request(readStream, b0);
        resp0 = req0.respond(Result.success(r));
        req1 = new StorageRead.Request(readStream, b1);
        resp1 = req1.respond(Result.success(r));
        req2 = new StorageRead.Request(readStream, b2);
        resp2 = req2.respond(Result.success(r));
    }

    @Test
    public void simpleClose() {
        LOG.info("***********************************************************");
        LOG.info("simple close");
        Config config = TypesafeConfig.load();
        MockComponentProxy proxy = new MockComponentProxy();
        MockExceptionHandler syncExHandler = new MockExceptionHandler();

        SimpleKCache skCache = buildCache(config, proxy, syncExHandler);
        startCache(skCache, proxy, syncExHandler);
        closeCache(skCache, proxy, syncExHandler);
    }

    @Test
    public void simpleCleanClose() {
        LOG.info("***********************************************************");
        LOG.info("simple clean close");
        Config config = TypesafeConfig.load();
        MockComponentProxy proxy = new MockComponentProxy();
        MockExceptionHandler syncExHandler = new MockExceptionHandler();

        SimpleKCache skCache = buildCache(config, proxy, syncExHandler);
        startCache(skCache, proxy, syncExHandler);
        skCache.handleExtendedCacheClean.handle(timeout);
        skCache.handleExtendedCacheClean.handle(timeout);
        closeCache(skCache, proxy, syncExHandler);
    }

    /**
     * NONE->PENDING_FETCH->CACHE->READ->SYSTEM->CACHE->SYSTEM->SYSTEM_INVALID->CLEAN->END
     */
    @Test
    public void testChain1() throws KReferenceException {
        LOG.info("***********************************************************");
        LOG.info("chain1");
        //setup
        Config config = TypesafeConfig.load();
        MockComponentProxy proxy = new MockComponentProxy();
        MockExceptionHandler syncExHandler = new MockExceptionHandler();

        MockDelayedRead read = new MockDelayedRead(new KPieceImpl(0, -1, 0l, 3l));
        Validator validator;
        //**********************************************************************
        int hintStamp = 1;
        SimpleKCache skCache = buildCache(config, proxy, syncExHandler);
        startCache(skCache, proxy, syncExHandler);
        //***********************PENDING_FETCH**********************************
        //hint0 - R0 fetch 
        proxy.expect(new EventContentValidator(comparator, req0));
        skCache.setFutureReads(readerId, new KHint.Expanded(hintStamp++, hint0));
        validator = proxy.validateNext();
        Assert.assertTrue(validator.toString(), validator.isValid());
        checkCacheState(skCache, new CacheState(0, 0, 1, 0));
        //***************************CACHE**************************************
        //respond fetch
        skCache.handleReadResp.handle(resp0);
        checkCacheState(skCache, new CacheState(1, 0, 0, 0));
        //***************************SYSTEM*************************************
        //read R0 subrange
        skCache.read(read.range, read);
        Assert.assertTrue(read.done);
        //hintE - R0 invalidated, move to system(there is a retain on it)
        skCache.setFutureReads(readerId, new KHint.Expanded(hintStamp++, hintE));
        checkCacheState(skCache, new CacheState(0, 1, 0, 0));
        //*****************************CACHE************************************
        skCache.setFutureReads(readerId, new KHint.Expanded(hintStamp++, hint0));
        checkCacheState(skCache, new CacheState(1, 0, 0, 0));
        //*****************************SYSTEM***********************************
        skCache.setFutureReads(readerId, new KHint.Expanded(hintStamp++, hintE));
        checkCacheState(skCache, new CacheState(0, 1, 0, 0));
        //*************************SYSTEM_INVALID*******************************
        //release last retain - invalidating base ref
        read.returned.release();
        skCache.handleExtendedCacheClean.handle(timeout);
        checkCacheState(skCache, new CacheState(0, 0, 0, 0));
        //**********************************************************************
        closeCache(skCache, proxy, syncExHandler);
    }

    /**
     * NONE->SYSTEM(BUFFER)->CACHE->CACHE_INVALID->CLEAN->END
     */
    @Test
    public void testChain2() throws KReferenceException {
        LOG.info("***********************************************************");
        LOG.info("chain2");
        //setup
        Config config = TypesafeConfig.load();
        MockComponentProxy proxy = new MockComponentProxy();
        MockExceptionHandler syncExHandler = new MockExceptionHandler();

        //**********************************************************************
        int hintStamp = 1;
        SimpleKCache skCache = buildCache(config, proxy, syncExHandler);
        startCache(skCache, proxy, syncExHandler);
        //*******************************BUFFERED*******************************
        KReference<byte[]> baseRef = KReferenceFactory.getReference(r);
        skCache.buffered(b0, baseRef);
        checkCacheState(skCache, new CacheState(0, 1, 0, 0));
        //********************************CACHE*********************************
        skCache.setFutureReads(readerId, new KHint.Expanded(hintStamp++, hint0));
        checkCacheState(skCache, new CacheState(1, 0, 0, 0));
        //****************************CACHE_INVALID*****************************
        baseRef.release();
        skCache.setFutureReads(readerId, new KHint.Expanded(hintStamp++, hintE));
        checkCacheState(skCache, new CacheState(0, 0, 0, 0));
        //**********************************************************************
        closeCache(skCache, proxy, syncExHandler);
    }

    /**
     * NONE->PENDING_FETCH->DELAYED_READ(REQ)->PENDING_FETCH->CACHE->DELAYED_READ(RESP)->CLEAN->END
     */
    public void testChain3() throws KReferenceException {
        LOG.info("***********************************************************");
        LOG.info("chain3");
        //setup
        Config config = TypesafeConfig.load();
        MockComponentProxy proxy = new MockComponentProxy();
        MockExceptionHandler syncExHandler = new MockExceptionHandler();

        Validator validator;
        MockDelayedRead read = new MockDelayedRead(new KPieceImpl(0, -1, 0l, 3l));

        //**********************************************************************
        int hintStamp = 1;
        SimpleKCache skCache = buildCache(config, proxy, syncExHandler);
        startCache(skCache, proxy, syncExHandler);
        //***********************PENDING_FETCH**********************************
        //hint0 - R0 fetch 
        proxy.expect(new EventContentValidator(comparator, req0));
        skCache.setFutureReads(readerId, new KHint.Expanded(hintStamp++, hint0));
        validator = proxy.validateNext();
        Assert.assertTrue(validator.toString(), validator.isValid());
        checkCacheState(skCache, new CacheState(0, 0, 1, 0));
        //**************************DELAYED_READ(REQ)***************************
        skCache.read(read.range, read);
        checkCacheState(skCache, new CacheState(0, 0, 1, 1));
        Assert.assertFalse(read.done);
        //***************************CACHE**************************************
        //respond fetch
        skCache.handleReadResp.handle(resp0);
        checkCacheState(skCache, new CacheState(1, 0, 0, 0));
        //*************************DELAYED_READ(RESP)***************************
        Assert.assertTrue(read.done);
        read.returned.release();
        //*******************************CLEAN**********************************
        skCache.setFutureReads(readerId, new KHint.Expanded(hintStamp++, hintE));
        checkCacheState(skCache, new CacheState(0, 0, 0, 0));
        //**********************************************************************
        closeCache(skCache, proxy, syncExHandler);
    }

    /**
     * NONE->PENDING_FETCH->DELAYED_READ(REQ)(2 solved by 1
     * fetch)->PENDING_FETCH->CACHE->DELAYED_READ(RESP)->CLEAN->END
     */
    public void testChain4() throws KReferenceException {
        LOG.info("***********************************************************");
        LOG.info("chain4");
        //setup
        Config config = TypesafeConfig.load();
        MockComponentProxy proxy = new MockComponentProxy();
        MockExceptionHandler syncExHandler = new MockExceptionHandler();

        Validator validator;
        MockDelayedRead read1 = new MockDelayedRead(new KPieceImpl(0, -1, 0l, 3l));
        MockDelayedRead read2 = new MockDelayedRead(new KPieceImpl(0, -1, 1l, 7l));

        //**********************************************************************
        int hintStamp = 1;
        SimpleKCache skCache = buildCache(config, proxy, syncExHandler);
        startCache(skCache, proxy, syncExHandler);
        //***********************PENDING_FETCH**********************************
        //hint0 - R0 fetch 
        proxy.expect(new EventContentValidator(comparator, req0));
        skCache.setFutureReads(readerId, new KHint.Expanded(hintStamp++, hint0));
        validator = proxy.validateNext();
        Assert.assertTrue(validator.toString(), validator.isValid());
        checkCacheState(skCache, new CacheState(0, 0, 1, 0));
        //**************************DELAYED_READ(REQ)***************************
        skCache.read(read1.range, read1);
        skCache.read(read2.range, read2);
        checkCacheState(skCache, new CacheState(0, 0, 1, 2));
        Assert.assertFalse(read1.done);
        Assert.assertFalse(read2.done);
        //***************************CACHE**************************************
        //respond fetch
        skCache.handleReadResp.handle(resp0);
        checkCacheState(skCache, new CacheState(1, 0, 0, 0));
        //*************************DELAYED_READ(RESP)***************************
        Assert.assertTrue(read1.done);
        Assert.assertTrue(read2.done);
        read1.returned.release();
        read2.returned.release();
        //*******************************CLEAN**********************************
        skCache.setFutureReads(readerId, new KHint.Expanded(hintStamp++, hintE));
        checkCacheState(skCache, new CacheState(0, 0, 0, 0));
        //**********************************************************************
        closeCache(skCache, proxy, syncExHandler);
    }

    /**
     * NONE->PENDING_FETCH->DELAYED_READ(REQ)(2 solved by 2
     * fetch)->PENDING_FETCH->CACHE->DELAYED_READ(RESP)->CLEAN->END
     */
    public void testChain5() throws KReferenceException {
        LOG.info("***********************************************************");
        LOG.info("chain5");
        //setup
        Config config = TypesafeConfig.load();
        MockComponentProxy proxy = new MockComponentProxy();
        MockExceptionHandler syncExHandler = new MockExceptionHandler();

        Validator validator;
        MockDelayedRead read1 = new MockDelayedRead(new KPieceImpl(0, -1, 0l, 3l));
        MockDelayedRead read2 = new MockDelayedRead(new KPieceImpl(1, -1, 0l, 17l));

        //**********************************************************************
        int hintStamp = 1;
        SimpleKCache skCache = buildCache(config, proxy, syncExHandler);
        startCache(skCache, proxy, syncExHandler);
        //***********************PENDING_FETCH**********************************
        //hint0_1 - R0,R1 fetch 
        proxy.expect(new EventContentValidator(comparator, req0));
        proxy.expect(new EventContentValidator(comparator, req1));
        skCache.setFutureReads(readerId, new KHint.Expanded(hintStamp++, hint0_1));
        validator = proxy.validateNext();
        Assert.assertTrue(validator.toString(), validator.isValid());
        Assert.assertTrue(validator.toString(), validator.isValid());
        checkCacheState(skCache, new CacheState(0, 0, 2, 0));
        //**************************DELAYED_READ(REQ)***************************
        skCache.read(read1.range, read1);
        skCache.read(read2.range, read2);
        checkCacheState(skCache, new CacheState(0, 0, 2, 2));
        Assert.assertFalse(read1.done);
        Assert.assertFalse(read2.done);
        //***************************CACHE**************************************
        //respond fetch
        skCache.handleReadResp.handle(resp0);
        checkCacheState(skCache, new CacheState(1, 0, 1, 1));
        skCache.handleReadResp.handle(resp1);
        checkCacheState(skCache, new CacheState(2, 0, 0, 0));
        //*************************DELAYED_READ(RESP)***************************
        Assert.assertTrue(read1.done);
        Assert.assertTrue(read2.done);
        read1.returned.release();
        read2.returned.release();
        //*******************************CLEAN**********************************
        skCache.setFutureReads(readerId, new KHint.Expanded(hintStamp++, hintE));
        checkCacheState(skCache, new CacheState(0, 0, 0, 0));
        //**********************************************************************
        closeCache(skCache, proxy, syncExHandler);

    }

    private SimpleKCache buildCache(Config config, MockComponentProxy proxy, MockExceptionHandler syncExHandler) {
        proxy.expect(new PortValidator(MockStreamPort.class, false));
        proxy.expect(
                new PortValidator(Timer.class, false));
        SimpleKCache skCache = new SimpleKCache(config, proxy, syncExHandler, new ComponentLoadTracking("test", proxy, new QueueLoadConfig(config)), readStream);

        Assert.assertTrue(proxy.validateNext().isValid());
        Assert.assertTrue(proxy.validateNext().isValid());
        Assert.assertEquals(0, syncExHandler.getExceptionCounter());
        return skCache;
    }

    private void startCache(SimpleKCache skCache, MockComponentProxy proxy, MockExceptionHandler syncExHandler) {
        proxy.expect(new EventClassValidator(SchedulePeriodicTimeout.class));
        skCache.start();
        Assert.assertTrue(proxy.validateNext().isValid());
        Assert.assertEquals(0, syncExHandler.getExceptionCounter());
    }

    private void closeCache(SimpleKCache skCache, MockComponentProxy proxy, MockExceptionHandler syncExHandler) {
        proxy.expect(new EventClassValidator(CancelPeriodicTimeout.class));
        skCache.close();
        Assert.assertTrue(proxy.validateNext().isValid());
        Assert.assertEquals(0, syncExHandler.getExceptionCounter());
        checkCacheState(skCache, new CacheState(0, 0, 0, 0));
    }

    private void checkCacheState(SimpleKCache skCache, CacheState state) {
        Assert.assertEquals(state.cacheRefSize, skCache.cacheRef.size());
        Assert.assertEquals(state.systemRefSize, skCache.systemRef.size());
        Assert.assertEquals(state.pendingFetchSize, skCache.pendingCacheFetch.size());
        Assert.assertEquals(state.delayedReadsSize, skCache.delayedReads.size());
    }

    private void readCheckAndRelease(KCache kCache, KRange r) throws KReferenceException {
        MockDelayedRead dr = new MockDelayedRead(r);
        kCache.read(dr.range, dr);
        Assert.assertTrue(dr.done);
        dr.returned.release();
    }

    public static class MockDelayedRead implements ReadCallback {

        public final KRange range;
        public boolean done = false;
        public KReference<byte[]> returned;

        public MockDelayedRead(KRange range) {
            this.range = range;
        }

        @Override
        public boolean fail(Result<KReference<byte[]>> result) {
            throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        }

        @Override
        public boolean success(Result<KReference<byte[]>> result) {
            if (!result.isSuccess()) {
                return false;
            }
            done = true;
            returned = result.getValue();
            returned.retain();
            return true;
        }
    }

    public static class CacheState {

        public final int cacheRefSize;
        public final int systemRefSize;
        public final int pendingFetchSize;
        public final int delayedReadsSize;

        public CacheState(int cacheRefSize, int systemRefSize, int pendingFetchSize, int delayedReadsSize) {
            this.cacheRefSize = cacheRefSize;
            this.systemRefSize = systemRefSize;
            this.pendingFetchSize = pendingFetchSize;
            this.delayedReadsSize = delayedReadsSize;
        }
    }
}
