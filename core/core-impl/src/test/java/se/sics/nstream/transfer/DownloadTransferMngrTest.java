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
package se.sics.nstream.transfer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import org.javatuples.Pair;
import org.junit.Assert;
import org.junit.Test;
import se.sics.kompics.config.Config;
import se.sics.kompics.config.TypesafeConfig;
import se.sics.kompics.timer.CancelPeriodicTimeout;
import se.sics.kompics.timer.Timer;
import se.sics.ktoolbox.util.managedStore.core.util.HashUtil;
import se.sics.ktoolbox.util.result.Result;
import se.sics.ktoolbox.util.test.EventClassValidator;
import se.sics.ktoolbox.util.test.EventRetainValidator;
import se.sics.ktoolbox.util.test.MockComponentProxy;
import se.sics.ktoolbox.util.test.MockExceptionHandler;
import se.sics.ktoolbox.util.test.PortValidator;
import se.sics.ktoolbox.util.test.Validator;
import se.sics.ktoolbox.util.test.WrongValidator;
import se.sics.nstream.storage.AsyncIncompleteStorage;
import se.sics.nstream.storage.AsyncOnDemandHashStorage;
import se.sics.nstream.storage.buffer.KBuffer;
import se.sics.nstream.storage.buffer.MultiKBuffer;
import se.sics.nstream.storage.buffer.SimpleAppendKBuffer;
import se.sics.nstream.storage.buffer.SimpleAppendKBufferTest;
import se.sics.nstream.storage.cache.KHint;
import se.sics.nstream.storage.cache.SimpleKCache;
import se.sics.nstream.storage.StorageWrite;
import se.sics.nstream.storage.managed.AppendFileMngr;
import se.sics.nstream.test.MockBWC;
import se.sics.nstream.test.MockHWC;
import se.sics.nstream.test.MockPWC;
import se.sics.nstream.test.MockStreamEndpoint;
import se.sics.nstream.test.MockStreamPort;
import se.sics.nstream.test.MockStreamResource;
import se.sics.nstream.test.MockWC;
import se.sics.nstream.util.BlockDetails;
import se.sics.nstream.util.FileBaseDetails;
import se.sics.nstream.util.FileDetails;
import se.sics.nstream.util.StreamEndpoint;
import se.sics.nstream.util.StreamResource;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class DownloadTransferMngrTest {

    Config config = TypesafeConfig.load();
    FileDetails fileDetails1;
    byte[] defBlock, lastBlock;
    byte[] defHash, lastHash;
    List<Pair<Long, byte[]>> p0, p1, p2, p3, p4, p5;
    Pair<Long, byte[]> p1_l, p3_l_faulty, p3_l;

    {
        BlockDetails defaultBlock = new BlockDetails(10, 2, 5, 5);
        String hashAlg = HashUtil.getAlgName(HashUtil.SHA);
        BlockDetails lastBlock1 = new BlockDetails(6, 2, 5, 1);
        StreamEndpoint writeEndpoint = new MockStreamEndpoint();
        StreamResource writeResource = new MockStreamResource("mock1");
        FileBaseDetails fileBaseDetails1 = new FileBaseDetails(56, 6, defaultBlock, lastBlock1, hashAlg);
        fileDetails1 = new FileDetails(fileBaseDetails1, Pair.with(writeEndpoint, writeResource), new ArrayList<Pair<StreamEndpoint, StreamResource>>());

        defBlock = new byte[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
        defHash = HashUtil.makeHash(defBlock, fileDetails1.base.hashAlg);
        lastBlock = new byte[]{0, 1, 2, 3, 4, 5};
        lastHash = HashUtil.makeHash(lastBlock, fileDetails1.base.hashAlg);

        p0 = new LinkedList<>();
        p0.add(Pair.with(0l, new byte[]{0, 1, 2, 3, 4}));
        p0.add(Pair.with(1l, new byte[]{5, 6, 7, 8, 9}));
        p1 = new LinkedList<>();
        p1.add(Pair.with(2l, new byte[]{0, 1, 2, 3, 4}));
        p1_l = Pair.with(3l, new byte[]{5, 6, 7, 8, 9});
        p2 = new LinkedList<>();
        p2.add(Pair.with(4l, new byte[]{0, 1, 2, 3, 4}));
        p2.add(Pair.with(5l, new byte[]{5, 6, 7, 8, 9}));
        p3 = new LinkedList<>();
        p3.add(Pair.with(6l, new byte[]{0, 1, 2, 3, 4}));
        p3_l_faulty = Pair.with(7l, new byte[]{0, 6, 7, 8, 9});
        p3_l = Pair.with(7l, new byte[]{5, 6, 7, 8, 9});
        p4 = new LinkedList<>();
        p4.add(Pair.with(8l, new byte[]{0, 1, 2, 3, 4}));
        p4.add(Pair.with(9l, new byte[]{5, 6, 7, 8, 9}));
        p5 = new LinkedList<>();
        p5.add(Pair.with(10l, new byte[]{0, 1, 2, 3, 4}));
        p5.add(Pair.with(11l, new byte[]{5}));
    }

    @Test
    public void simpleTest() {
        MockComponentProxy proxy = new MockComponentProxy();
        MockExceptionHandler syncExHandler = new MockExceptionHandler();
        Validator validator;

        Pair<DownloadTransferMngr, SimpleAppendKBuffer> system = buildTransferMngr(proxy, syncExHandler);
        DownloadTransferMngr dtm = system.getValue0();
        SimpleAppendKBufferTest mb = new SimpleAppendKBufferTest(system.getValue1());

        startTransferMngr(dtm, proxy, syncExHandler);

        MockWC delayedWrite;
        KHint.Summary hint;
        Set<Integer> expectedHint;

        //hint
        hint = dtm.getFutureReads(3);
        expectedHint = new HashSet<>(Arrays.asList(0, 1, 2));
        Assert.assertEquals(expectedHint, hint.blocks);
        //
        Assert.assertTrue(dtm.moreWork());
        Assert.assertFalse(dtm.pendingWork());
        Assert.assertEquals(new TreeSet<Long>(), dtm.nextPieces(100));
        Assert.assertEquals(0, dtm.nextBlock());
        //write block 0 as pieces
        writeHash(proxy, syncExHandler, dtm, 0, defHash);
        appendCompleteBlock(proxy, syncExHandler, dtm, mb, p0);
        //hint
        hint = dtm.getFutureReads(3);
        expectedHint = new HashSet<>(Arrays.asList(1, 2, 3));
        Assert.assertEquals(expectedHint, hint.blocks);
        //b1, b2
        Assert.assertEquals(1, dtm.nextBlock());
        Assert.assertEquals(2, dtm.nextBlock());
        //write block 2
        writeHash(proxy, syncExHandler, dtm, 2, defHash);
        MockBWC b2WC = writeCompleteBlock(proxy, syncExHandler, dtm, mb, p2);
        //hint
        hint = dtm.getFutureReads(3);
        //1 is pending, next 3 - 3,4,5
        expectedHint = new HashSet<>(Arrays.asList(1, 3, 4, 5));
        Assert.assertEquals(expectedHint, hint.blocks);
        //write block 1 - block 2 is buffered
        writeHash(proxy, syncExHandler, dtm, 1, defHash);
        writeIncompleteBlock(proxy, syncExHandler, dtm, mb, p1);
        proxy.expect(new EventRetainValidator(StorageWrite.Request.class)); //b1
        proxy.expect(new EventRetainValidator(StorageWrite.Request.class)); //b2
        MockPWC p1WC = new MockPWC();
        dtm.writePiece(p1_l.getValue0(), p1_l.getValue1(), p1WC); //write last piece of b1
        Assert.assertTrue(p1WC.done);
        Assert.assertTrue(p1WC.waitingOnBlock);
        validator = proxy.validateNext();
        Assert.assertTrue(validator.toString(), validator.isValid()); //expected req for b1
        mb.handleWriteResp(((StorageWrite.Request) ((EventRetainValidator) validator).event).respond(Result.success(true)));
        Assert.assertTrue(p1WC.blockCallback.done);
        validator = proxy.validateNext();
        Assert.assertTrue(validator.toString(), validator.isValid()); //expected req for b2
        mb.handleWriteResp(((StorageWrite.Request) ((EventRetainValidator) validator).event).respond(Result.success(true)));
        Assert.assertTrue(b2WC.done);
        //hint
        hint = dtm.getFutureReads(3);
        //next 3 - 3,4,5
        expectedHint = new HashSet<>(Arrays.asList(3, 4, 5));
        Assert.assertEquals(expectedHint, hint.blocks);
        //b3 - faulty piece
        Assert.assertEquals(3, dtm.nextBlock());
        writeHash(proxy, syncExHandler, dtm, 3, defHash);
        writeIncompleteBlock(proxy, syncExHandler, dtm, mb, p3);
        MockPWC p3WC = new MockPWC();
        dtm.writePiece(p3_l_faulty.getValue0(), p3_l_faulty.getValue1(), p3WC); //write last, faulty, piece of b3
        Assert.assertTrue(p3WC.done);
        Assert.assertTrue(p3WC.waitingOnBlock);
        Assert.assertTrue(p3WC.blockCallback.done);
        //b3 - correct
        Assert.assertEquals(3, dtm.nextBlock());
        proxy.expect(new EventRetainValidator(StorageWrite.Request.class)); //b3
        writeIncompleteBlock(proxy, syncExHandler, dtm, mb, p3);
        p3WC = new MockPWC();
        dtm.writePiece(p3_l.getValue0(), p3_l.getValue1(), p3WC); //write last piece of b3
        Assert.assertTrue(p3WC.done);
        Assert.assertTrue(p3WC.waitingOnBlock);
        validator = proxy.validateNext();
        Assert.assertTrue(validator.toString(), validator.isValid()); //expected req for b3
        mb.handleWriteResp(((StorageWrite.Request) ((EventRetainValidator) validator).event).respond(Result.success(true)));
        Assert.assertTrue(p3WC.blockCallback.done);
        //b4
        //reset piece
        dtm.resetPiece(9l);
        Set<Long> nextPieces = new TreeSet<>(Arrays.asList(9l));
        //complete
        Assert.assertEquals(nextPieces, dtm.nextPieces(5));
        writeHash(proxy, syncExHandler, dtm, 4, defHash);
        Assert.assertEquals(4, dtm.nextBlock());
        appendCompleteBlock(proxy, syncExHandler, dtm, mb, p4);
        //b5 - last block - different than default
        writeHash(proxy, syncExHandler, dtm, 5, lastHash);
        Assert.assertEquals(5, dtm.nextBlock());
        appendCompleteBlock(proxy, syncExHandler, dtm, mb, p5);
        
        Assert.assertTrue(dtm.isComplete());
        closeTransferMngr(dtm, proxy, syncExHandler);
    }

    private void writeHash(MockComponentProxy proxy, MockExceptionHandler syncExHandler, DownloadTransferMngr dtm, int blockNr, byte[] hash) {
        MockHWC hashWC = new MockHWC();
        dtm.writeHash(blockNr, hash, hashWC);
        Assert.assertTrue(hashWC.done);
        Assert.assertEquals(0, syncExHandler.getExceptionCounter());
    }

    private void appendCompleteBlock(MockComponentProxy proxy, MockExceptionHandler syncExHandler, DownloadTransferMngr dtm, SimpleAppendKBufferTest mb,
            List<Pair<Long, byte[]>> pieces) {
        Iterator<Pair<Long, byte[]>> it = pieces.iterator();
        while (it.hasNext()) {
            Pair<Long, byte[]> piece = it.next();
            MockPWC pieceWC = new MockPWC();

            if (it.hasNext()) {
                dtm.writePiece(piece.getValue0(), piece.getValue1(), pieceWC);
                Assert.assertTrue(pieceWC.done);
                Assert.assertFalse(pieceWC.waitingOnBlock);
            } else {
                Validator validator;
                proxy.expect(new EventRetainValidator(StorageWrite.Request.class));
                dtm.writePiece(piece.getValue0(), piece.getValue1(), pieceWC);
                validator = proxy.validateNext();
                Assert.assertTrue(validator.toString(), validator.isValid());
                Assert.assertTrue(pieceWC.waitingOnBlock);
                mb.handleWriteResp(((StorageWrite.Request) ((EventRetainValidator) validator).event).respond(Result.success(true)));
                Assert.assertTrue(pieceWC.blockCallback.done);
            }
        }
        Assert.assertEquals(0, syncExHandler.getExceptionCounter());
    }

    private MockBWC writeCompleteBlock(MockComponentProxy proxy, MockExceptionHandler syncExHandler, DownloadTransferMngr dtm, SimpleAppendKBufferTest mb,
            List<Pair<Long, byte[]>> pieces) {
        Iterator<Pair<Long, byte[]>> it = pieces.iterator();
        while (it.hasNext()) {
            Pair<Long, byte[]> piece = it.next();
            MockPWC pieceWC = new MockPWC();

            if (it.hasNext()) {
                dtm.writePiece(piece.getValue0(), piece.getValue1(), pieceWC);
                Assert.assertTrue(pieceWC.done);
                Assert.assertFalse(pieceWC.waitingOnBlock);
            } else {
                dtm.writePiece(piece.getValue0(), piece.getValue1(), pieceWC);
                Assert.assertEquals(0, syncExHandler.getExceptionCounter());
                Assert.assertTrue(pieceWC.waitingOnBlock);
                return pieceWC.blockCallback;
            }
        }
        return null;
    }

    private void writeIncompleteBlock(MockComponentProxy proxy, MockExceptionHandler syncExHandler, DownloadTransferMngr dtm, SimpleAppendKBufferTest mb,
            List<Pair<Long, byte[]>> pieces) {
        Iterator<Pair<Long, byte[]>> it = pieces.iterator();
        while (it.hasNext()) {
            Pair<Long, byte[]> piece = it.next();
            MockPWC pieceWC = new MockPWC();

            dtm.writePiece(piece.getValue0(), piece.getValue1(), pieceWC);
            Assert.assertTrue(pieceWC.done);
            Assert.assertFalse(pieceWC.waitingOnBlock);
        }
    }

    private Pair<DownloadTransferMngr, SimpleAppendKBuffer> buildTransferMngr(MockComponentProxy proxy, MockExceptionHandler syncExHandler) {
        proxy.expect(new PortValidator(MockStreamPort.class, false));
        proxy.expect(new PortValidator(Timer.class, false));
        proxy.expect(new PortValidator(MockStreamPort.class, false));

        SimpleKCache cache = new SimpleKCache(config, proxy, syncExHandler, fileDetails1.mainResource.getValue0(), fileDetails1.mainResource.getValue1());
        List<KBuffer> bufs = new ArrayList<>();
        SimpleAppendKBuffer mainBuffer = new SimpleAppendKBuffer(config, proxy, syncExHandler, 
                fileDetails1.mainResource.getValue0(), fileDetails1.mainResource.getValue1(), 0);
        bufs.add(mainBuffer);
        for (Pair<StreamEndpoint, StreamResource> writeResource : fileDetails1.secondaryResources) {
            bufs.add(new SimpleAppendKBuffer(config, proxy, syncExHandler, writeResource.getValue0(), writeResource.getValue1(), 0));
        }
        KBuffer buffer = new MultiKBuffer(bufs);
        AsyncIncompleteStorage file = new AsyncIncompleteStorage(cache, buffer);
        AsyncOnDemandHashStorage hash = new AsyncOnDemandHashStorage(fileDetails1.base, syncExHandler, file);
        AppendFileMngr afm = new AppendFileMngr(fileDetails1.base, file, hash);
        DownloadTransferMngr dtm = new DownloadTransferMngr(fileDetails1.base, afm);

        Validator validator;
        validator = proxy.validateNext();
        Assert.assertTrue(validator.toString(), validator.isValid());
        validator = proxy.validateNext();
        Assert.assertTrue(validator.toString(), validator.isValid());
        validator = proxy.validateNext();
        Assert.assertTrue(validator.toString(), validator.isValid());
        Assert.assertEquals(0, syncExHandler.getExceptionCounter());
        return Pair.with(dtm, mainBuffer);
    }

    private void startTransferMngr(DownloadTransferMngr dtMngr, MockComponentProxy proxy, MockExceptionHandler syncExHandler) {
        proxy.expect(new EventClassValidator(SimpleKCache.ExtendedCacheClean.class));

        dtMngr.start();

        Validator validator;
        validator = proxy.validateNext();
        Assert.assertTrue(validator.toString(), validator.isValid());
        Assert.assertEquals(0, syncExHandler.getExceptionCounter());
    }

    private void closeTransferMngr(DownloadTransferMngr dtMngr, MockComponentProxy proxy, MockExceptionHandler syncExHandler) {
        proxy.expect(new EventClassValidator(CancelPeriodicTimeout.class));

        dtMngr.close();

        Validator validator;
        validator = proxy.validateNext();
        Assert.assertTrue(validator.toString(), validator.isValid());
        validator = proxy.validateNext();
        Assert.assertTrue(validator instanceof WrongValidator);
        Assert.assertEquals(0, syncExHandler.getExceptionCounter());
    }
}
