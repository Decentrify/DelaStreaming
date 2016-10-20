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
package se.sics.nstream.storage.buffer;

import java.util.Random;
import org.javatuples.Pair;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.kompics.config.Config;
import se.sics.kompics.config.TypesafeConfig;
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
import se.sics.ktoolbox.util.test.EventContentValidator;
import se.sics.ktoolbox.util.test.MockComponentProxy;
import se.sics.ktoolbox.util.test.MockExceptionHandler;
import se.sics.ktoolbox.util.test.PortValidator;
import se.sics.ktoolbox.util.test.Validator;
import se.sics.nstream.FileId;
import se.sics.nstream.StreamId;
import se.sics.nstream.TorrentIds;
import se.sics.nstream.storage.durable.events.DStorageWrite;
import se.sics.nstream.storage.durable.util.MyStream;
import se.sics.nstream.test.DStorageWriteReqEC;
import se.sics.nstream.test.MockStreamEndpoint;
import se.sics.nstream.test.MockStreamPort;
import se.sics.nstream.test.MockStreamResource;
import se.sics.nstream.test.MockWC;
import se.sics.nstream.util.actuator.ComponentLoadTracking;
import se.sics.nstream.util.range.KBlock;
import se.sics.nstream.util.range.KBlockImpl;
import se.sics.nutil.tracking.load.QueueLoadConfig;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class TestSimpleAppendKBuffer {

    private static final Logger LOG = LoggerFactory.getLogger(TestSimpleAppendKBuffer.class);

    private static Pair<StreamId, MyStream> writeStream;
    private static Identifier readerId;

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

        MockStreamEndpoint writeEndpoint = new MockStreamEndpoint();
        MockStreamResource writeResource = new MockStreamResource("mock1");
        writeStream = Pair.with(streamId, new MyStream(writeEndpoint, writeResource));
    }

    @Test
    public void simpleTest() throws KReferenceException {
        LOG.info("simple test");
        //setup
        Config config = TypesafeConfig.load();
        MockComponentProxy proxy = new MockComponentProxy();
        MockExceptionHandler syncExHandler = new MockExceptionHandler();

        MockWC allWriteResult = new MockWC();

        long appendPos = 0;
        Validator validator;
        KReference<byte[]> ref1, ref2, ref3;
        DStorageWrite.Request swr1, swr2, swr3;

        KBlock b1 = new KBlockImpl(0, 0, 2);
        ref1 = KReferenceFactory.getReference(new byte[]{1, 2, 3});
        swr1 = new DStorageWrite.Request(writeStream.getValue0(), b1.lowerAbsEndpoint(), ref1.getValue().get());

        KBlock b2 = new KBlockImpl(2, 6, 8);
        ref2 = KReferenceFactory.getReference(new byte[]{1, 2, 3});
        swr2 = new DStorageWrite.Request(writeStream.getValue0(), b2.lowerAbsEndpoint(), ref2.getValue().get());

        KBlock b3 = new KBlockImpl(1, 3, 5);
        ref3 = KReferenceFactory.getReference(new byte[]{1, 2, 3});
        swr3 = new DStorageWrite.Request(writeStream.getValue0(), b3.lowerAbsEndpoint(), ref3.getValue().get());

        //settig up validators;
        proxy.expect(new PortValidator(MockStreamPort.class, false));
        EventContentValidator ecv1 = new EventContentValidator(new DStorageWriteReqEC(), swr1);
        EventContentValidator ecv3 = new EventContentValidator(new DStorageWriteReqEC(), swr3);
        EventContentValidator ecv2 = new EventContentValidator(new DStorageWriteReqEC(), swr2);
        proxy.expect(ecv1);
        proxy.expect(ecv3);
        proxy.expect(ecv2);

        //actual run
        SimpleAppendKBuffer sakBuf = new SimpleAppendKBuffer(config, proxy, syncExHandler, new ComponentLoadTracking("test", proxy, new QueueLoadConfig(config)), writeStream, 0);

        //write1
        sakBuf.write(b1, ref1, allWriteResult);
        ref1.release();
        Assert.assertTrue(ref1.isValid());
        //answer to write1
        DStorageWrite.Request req1 = (DStorageWrite.Request) ecv1.getFound();
        sakBuf.handleWriteResp.handle(req1.respond(Result.success(true)));
        Assert.assertFalse(ref1.isValid());
        Assert.assertTrue(sakBuf.isIdle());
        //write2
        sakBuf.write(b2, ref2, allWriteResult);
        ref2.release();
        Assert.assertTrue(ref2.isValid());
        //write3
        sakBuf.write(b3, ref3, allWriteResult);
        ref3.release();
        Assert.assertTrue(ref3.isValid());
        //answer to write3
        DStorageWrite.Request req3 = (DStorageWrite.Request) ecv3.getFound();
        sakBuf.handleWriteResp.handle(req3.respond(Result.success(true)));
        Assert.assertTrue(ref2.isValid());
        Assert.assertFalse(ref3.isValid());
        Assert.assertFalse(sakBuf.isIdle());
        //answer to write2
        DStorageWrite.Request req2 = (DStorageWrite.Request) ecv2.getFound();
        sakBuf.handleWriteResp.handle(req2.respond(Result.success(true)));
        Assert.assertFalse(ref2.isValid());
        Assert.assertTrue(sakBuf.isIdle());

        sakBuf.close();
        Assert.assertEquals(0, syncExHandler.getExceptionCounter());

        //validation
        validator = proxy.validate();
        if (validator != null) {
            Assert.fail(validator.toString());
        }
    }

    @Test
    public void closeBeforeFinish() throws KReferenceException {
        LOG.info("close before finish");
        //setup
        Config config = TypesafeConfig.load();
        MockComponentProxy proxy = new MockComponentProxy();
        MockExceptionHandler syncExHandler = new MockExceptionHandler();
        MockWC allWriteResult = new MockWC();

        long appendPos = 0;
        Validator validator;
        KReference<byte[]> ref1, ref2, ref3;
        DStorageWrite.Request swr1, swr2, swr3;

        KBlock b1 = new KBlockImpl(0, 0, 2);
        ref1 = KReferenceFactory.getReference(new byte[]{1, 2, 3});
        swr1 = new DStorageWrite.Request(writeStream.getValue0(), b1.lowerAbsEndpoint(), ref1.getValue().get());

        KBlock b2 = new KBlockImpl(2, 6, 8);
        ref2 = KReferenceFactory.getReference(new byte[]{1, 2, 3});
        swr2 = new DStorageWrite.Request(writeStream.getValue0(), b2.lowerAbsEndpoint(), ref2.getValue().get());

        KBlock b3 = new KBlockImpl(1, 3, 5);
        ref3 = KReferenceFactory.getReference(new byte[]{1, 2, 3});
        swr3 = new DStorageWrite.Request(writeStream.getValue0(), b3.lowerAbsEndpoint(), ref3.getValue().get());

        //settig up validators;
        proxy.expect(new PortValidator(MockStreamPort.class, false));
        EventContentValidator ecv1 = new EventContentValidator(new DStorageWriteReqEC(), swr1);
        EventContentValidator ecv3 = new EventContentValidator(new DStorageWriteReqEC(), swr3);
        EventContentValidator ecv2 = new EventContentValidator(new DStorageWriteReqEC(), swr2);
        proxy.expect(ecv1);
        proxy.expect(ecv3);
        proxy.expect(ecv2);

        //actual run
        SimpleAppendKBuffer sakBuf = new SimpleAppendKBuffer(config, proxy, syncExHandler, new ComponentLoadTracking("test", proxy, new QueueLoadConfig(config)), writeStream, 0);

        //write1
        sakBuf.write(b1, ref1, allWriteResult);
        ref1.release();
        Assert.assertTrue(ref1.isValid());
        //answer to write1
        DStorageWrite.Request req1 = (DStorageWrite.Request) ecv1.getFound();
        sakBuf.handleWriteResp.handle(req1.respond(Result.success(true)));
        Assert.assertFalse(ref1.isValid());
        Assert.assertTrue(sakBuf.isIdle());
        //write2
        sakBuf.write(b2, ref2, allWriteResult);
        ref2.release();
        Assert.assertTrue(ref2.isValid());
        //write3
        sakBuf.write(b3, ref3, allWriteResult);
        ref3.release();
        Assert.assertTrue(ref3.isValid());
        //answer to write3
        DStorageWrite.Request req3 = (DStorageWrite.Request) ecv3.getFound();
        sakBuf.handleWriteResp.handle(req3.respond(Result.success(true)));
        Assert.assertTrue(ref2.isValid());
        Assert.assertFalse(ref3.isValid());
        Assert.assertFalse(sakBuf.isIdle());
        //close before answer to write2
        sakBuf.close();
        Assert.assertFalse(ref2.isValid());
        Assert.assertEquals(0, syncExHandler.getExceptionCounter());

        //validation
        validator = proxy.validate();
        if (validator != null) {
            Assert.fail(validator.toString());
        }
    }

    @Test
    public void errorOnWrite() throws KReferenceException {
        LOG.info("error on write");
        //setup
        Config config = TypesafeConfig.load();
        MockComponentProxy proxy = new MockComponentProxy();
        MockExceptionHandler syncExHandler = new MockExceptionHandler();

        MockWC allWriteResult = new MockWC();
        long appendPos = 0;
        Validator validator;
        KReference<byte[]> ref1, ref2, ref3;
        DStorageWrite.Request swr1, swr2, swr3;

        KBlock b1 = new KBlockImpl(0, 0, 2);
        ref1 = KReferenceFactory.getReference(new byte[]{1, 2, 3});
        swr1 = new DStorageWrite.Request(writeStream.getValue0(), b1.lowerAbsEndpoint(), ref1.getValue().get());

        KBlock b2 = new KBlockImpl(2, 6, 8);
        ref2 = KReferenceFactory.getReference(new byte[]{1, 2, 3});
        swr2 = new DStorageWrite.Request(writeStream.getValue0(), b2.lowerAbsEndpoint(), ref2.getValue().get());

        KBlock b3 = new KBlockImpl(1, 3, 5);
        ref3 = KReferenceFactory.getReference(new byte[]{1, 2, 3});
        swr3 = new DStorageWrite.Request(writeStream.getValue0(), b3.lowerAbsEndpoint(), ref3.getValue().get());

        //settig up validators;
        proxy.expect(new PortValidator(MockStreamPort.class, false));
        EventContentValidator ecv1 = new EventContentValidator(new DStorageWriteReqEC(), swr1);
        EventContentValidator ecv3 = new EventContentValidator(new DStorageWriteReqEC(), swr3);
        EventContentValidator ecv2 = new EventContentValidator(new DStorageWriteReqEC(), swr2);
        proxy.expect(ecv1);
        proxy.expect(ecv3);
        proxy.expect(ecv2);

        //actual run
        SimpleAppendKBuffer sakBuf = new SimpleAppendKBuffer(config, proxy, syncExHandler, new ComponentLoadTracking("test", proxy, new QueueLoadConfig(config)), writeStream, 0);

        //write1
        sakBuf.write(b1, ref1, allWriteResult);
        ref1.release();
        Assert.assertTrue(ref1.isValid());
        //answer to write1
        DStorageWrite.Request req1 = (DStorageWrite.Request) ecv1.getFound();
        sakBuf.handleWriteResp.handle(req1.respond(Result.success(true)));
        Assert.assertFalse(ref1.isValid());
        Assert.assertTrue(sakBuf.isIdle());
        //write2
        sakBuf.write(b2, ref2, allWriteResult);
        ref2.release();
        Assert.assertTrue(ref2.isValid());
        //write3
        sakBuf.write(b3, ref3, allWriteResult);
        ref3.release();
        Assert.assertTrue(ref3.isValid());
        //answer to write3
        DStorageWrite.Request req3 = (DStorageWrite.Request) ecv3.getFound();
        sakBuf.handleWriteResp.handle(req3.respond(Result.externalUnsafeFailure(new IllegalStateException("test failure"))));
        Assert.assertFalse(ref2.isValid());
        Assert.assertFalse(ref3.isValid());
        Assert.assertTrue(sakBuf.isIdle());
        Assert.assertEquals(1, syncExHandler.getExceptionCounter());

        //validation
        validator = proxy.validate();
        if (validator != null) {
            Assert.fail(validator.toString());
        }
    }
}
