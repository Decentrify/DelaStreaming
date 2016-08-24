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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import org.javatuples.Pair;
import se.sics.ktoolbox.util.identifiable.Identifier;
import se.sics.ktoolbox.util.reference.KReference;
import se.sics.ktoolbox.util.reference.KReferenceException;
import se.sics.ktoolbox.util.reference.KReferenceFactory;
import se.sics.ktoolbox.util.result.Result;
import se.sics.nstream.storage.buffer.WriteResult;
import se.sics.nstream.storage.cache.KHint;
import se.sics.nstream.storage.managed.AppendFileMngr;
import se.sics.nstream.storage.managed.FileBWC;
import se.sics.nstream.util.BlockDetails;
import se.sics.nstream.util.FileBaseDetails;
import se.sics.nstream.util.StreamControl;
import se.sics.nstream.util.actuator.ComponentLoad;
import se.sics.nstream.util.actuator.ComponentLoadConfig;
import se.sics.nstream.util.range.KBlock;
import se.sics.nstream.util.range.KPiece;
import se.sics.nstream.util.result.HashReadCallback;
import se.sics.nstream.util.result.HashWriteCallback;
import se.sics.nstream.util.result.PieceReadCallback;
import se.sics.nstream.util.result.PieceWriteCallback;
import se.sics.nstream.util.result.ReadCallback;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class DownloadTransferMngr implements StreamControl, TransferMngr.Writer, TransferMngr.Reader {

    private final TransferMngrConfig tmConfig;
    private final FileBaseDetails fileDetails;
    private final ComponentLoad loadTracker;
    private final String fileName;
    //**************************************************************************
    private final AppendFileMngr file;
    //**************************************************************************
    private KHint.Summary oldHint;
    private final TreeMap<Integer, LinkedList<Integer>> workPieces = new TreeMap<>();
    private final Map<Integer, BlockMngr> pendingBlocks = new HashMap<>();
    private final Set<Integer> workHashes = new TreeSet<>();
    private final Set<Integer> nextHashes = new TreeSet<>();
    private final LinkedList<Integer> workBlocks = new LinkedList<>();
    private final LinkedList<Integer> cacheBlocks = new LinkedList<>();
    private final LinkedList<Integer> nextBlocks = new LinkedList<>();
    private int workPos = 0;
    private int hashPos = 0;
    private int cachePos = 0;
    //**************************************************************************
    private final Map<Integer, FileBWC> pendingStorageWrites = new HashMap<>();

    public DownloadTransferMngr(String fileName, ComponentLoad loadTracker, FileBaseDetails fileDetails, AppendFileMngr file) {
        this.fileName = fileName;
        this.loadTracker = loadTracker;
        this.fileDetails = fileDetails;
        this.file = file;
        this.tmConfig = new TransferMngrConfig();
        oldHint = new KHint.Summary(0, new TreeSet<Integer>());
    }

    //*********************************CONTROL**********************************
    @Override
    public void start() {
        file.start();
        startNextBlocks();
        startWorkBlocks();
    }

    @Override
    public boolean isIdle() {
        return file.isIdle();
    }

    @Override
    public void close() {
        file.close();
    }

    public UploadTransferMngr complete() {
        return new UploadTransferMngr(fileDetails, file.complete());
    }

    //**************************************************************************
    private void rebuildCacheHint() {
        Set<Integer> hint = new TreeSet<>();
        hint.addAll(pendingBlocks.keySet());
        hint.addAll(workBlocks);
        hint.addAll(cacheBlocks);
        oldHint = new KHint.Summary(oldHint.lStamp + 1, hint);
    }

    private void newWorkPieces() {
        if (workBlocks.isEmpty()) {
            newWorkBlocks();
        }
        if (!workBlocks.isEmpty()) {
            newPendingBlock(workBlocks.removeFirst());
        }
    }

    private void newPendingBlock(int blockNr) {
        BlockDetails blockDetails = fileDetails.getBlockDetails(blockNr);
        BlockMngr blockMngr = new InMemoryBlockMngr(blockDetails);
        pendingBlocks.put(blockNr, blockMngr);
        loadTracker.setTransferSize(fileName, pendingBlocks.size());
        LinkedList<Integer> pieceList = new LinkedList<>();
        for (int i = 0; i < blockDetails.nrPieces; i++) {
            pieceList.add(i);
        }
        workPieces.put(blockNr, pieceList);
    }

    private void newWorkHashes() {
        assert workHashes.isEmpty();
        if (!nextHashes.isEmpty()) {
            Iterator<Integer> aux = nextHashes.iterator();
            int auxCounter = tmConfig.batchSize;
            while (aux.hasNext() && auxCounter > 0) {
                Integer nB = aux.next();
                aux.remove();
                workHashes.add(nB);
                auxCounter--;
            }
        }
    }

    private void startWorkBlocks() {
        Iterator<Integer> aux;
        int auxCounter;

        aux = nextBlocks.iterator();
        auxCounter = tmConfig.workBatch;
        while (aux.hasNext() && auxCounter > 0) {
            Integer nB = aux.next();
            aux.remove();
            workPos++;
            hashPos++;
            workBlocks.add(nB);
            nextHashes.add(nB);
            auxCounter--;
        }

        aux = nextBlocks.iterator();
        auxCounter = tmConfig.hashesAhead;
        while (aux.hasNext() && auxCounter > 0) {
            Integer nB = aux.next();
            aux.remove();
            hashPos++;
            nextHashes.add(nB);
            cacheBlocks.add(nB);
            auxCounter--;
        }

        aux = nextBlocks.iterator();
        auxCounter = tmConfig.cacheAhead;
        cachePos = hashPos;
        while (aux.hasNext() && auxCounter > 0) {
            Integer nB = aux.next();
            aux.remove();
            cacheBlocks.add(nB);
            cachePos++;
            auxCounter--;
        }
        rebuildCacheHint();
    }

    private void newWorkBlocks() {
        assert workBlocks.isEmpty();

        if (nextBlocks.isEmpty()) {
            newNextBlocks();
        }

        Iterator<Integer> aux;
        int auxCounter;

        aux = cacheBlocks.iterator();
        auxCounter = tmConfig.workBatch;
        while (aux.hasNext() && auxCounter > 0) {
            Integer nB = aux.next();
            aux.remove();
            workBlocks.add(nB);
            workPos++;
            auxCounter--;
        }
        if (workPos + tmConfig.hashesAhead >= hashPos) {
            auxCounter = tmConfig.hashBatch;
            while (aux.hasNext() && auxCounter > 0) {
                Integer nb = aux.next();
                nextHashes.add(nb);
                hashPos++;
                auxCounter--;
            }
        }
        //replenish cash ahead by the amount the work queue took
        aux = nextBlocks.iterator();
        auxCounter = tmConfig.workBatch;
        while (aux.hasNext() && auxCounter > 0) {
            Integer nb = aux.next();
            aux.remove();
            cacheBlocks.add(nb);
            cachePos++;
            auxCounter--;
        }

        rebuildCacheHint();
    }

    private void startNextBlocks() {
        int nextBlocksSize = tmConfig.workBatch + tmConfig.hashesAhead + tmConfig.cacheAhead + tmConfig.nextBatch;
        nextBlocks.addAll(file.nextBlocksMissing(0, nextBlocksSize, new HashSet<Integer>()));
    }

    private void newNextBlocks() {
        assert nextBlocks.isEmpty();

        Set<Integer> exclude = new TreeSet<>();
        exclude.addAll(pendingBlocks.keySet());
        exclude.addAll(workBlocks);
        exclude.addAll(cacheBlocks);

        nextBlocks.addAll(file.nextBlocksMissing(0, tmConfig.nextBatch, exclude));
    }

    private void writeBlock(final int blockNr, BlockMngr block) {
        if (blockNr == 1) {
            int x = 1;
        }
        final KBlock blockRange = BlockHelper.getBlockRange(blockNr, fileDetails);
        final byte[] blockBytes = block.getBlock();
        final KReference<byte[]> blockRef = KReferenceFactory.getReference(blockBytes);
        FileBWC fileBWC = new FileBWC() {
            @Override
            public void hashResult(Result<Boolean> result) {
                if (result.isSuccess()) {
                    if (result.getValue()) {
//                        finishingWork.remove(blockRange.parentBlock());
                    } else {
                        pendingStorageWrites.remove(blockNr);
                        silentRelease(blockRef);
                        pendingBlocks.remove(blockNr);
                        newPendingBlock(blockNr);
                    }
                }
            }

            @Override
            public boolean fail(Result<WriteResult> result) {
                throw new RuntimeException("failed to write into storage - " + result.getException().getMessage());
            }

            @Override
            public boolean success(Result<WriteResult> result) {
                pendingStorageWrites.remove(blockNr);
                silentRelease(blockRef);
                pendingBlocks.remove(blockNr);
                loadTracker.setTransferSize(fileName, pendingBlocks.size());
                rebuildCacheHint();
                return true;
            }
        };
        pendingStorageWrites.put(blockNr, fileBWC);
        file.writeBlock(blockRange, blockRef, fileBWC);
    }

    private void silentRelease(KReference<byte[]> ref) {
        try {
            ref.release();
        } catch (KReferenceException ex) {
            throw new RuntimeException(ex);
        }
    }

    //****************************TRANSFER_HINT_WRITE***************************
    @Override
    public KHint.Summary getFutureReads() {
        return oldHint;
    }

    //*************************TRANSFER_HINT_READ*******************************
    @Override
    public void clean(Identifier reader) {
        file.clean(reader);
    }

    @Override
    public void setFutureReads(Identifier reader, KHint.Expanded hint) {
        file.setFutureReads(reader, hint);
    }

    //*********************************WRITER***********************************
    @Override
    public boolean workAvailable() {
        if (!workPieces.isEmpty() || !workHashes.isEmpty()) {
            return true;
        }
        if (pendingBlocks.size() > ComponentLoadConfig.maxTransfer) {
            return false;
        }
        newWorkPieces();
        newWorkHashes();
        return !(workPieces.isEmpty() && workHashes.isEmpty());
    }

    @Override
    public boolean hashesAvailable() {
        return !workHashes.isEmpty();
    }

    @Override
    public boolean pendingWork() {
        return !workHashes.isEmpty() || !nextHashes.isEmpty() || !workPieces.isEmpty() 
                || !pendingBlocks.isEmpty() || !workBlocks.isEmpty() || !cacheBlocks.isEmpty() || !nextBlocks.isEmpty();
    }
    
    @Override
    public boolean finishingWork() {
        return !pendingBlocks.isEmpty() || !file.pendingBlocks();
    }

    @Override
    public boolean isComplete() {
        return file.isComplete();
    }

    /**
     * @param writeRange
     * @param value
     * @return true if block is complete, false otherwise
     */
    @Override
    public void writePiece(Pair<Integer, Integer> pieceNr, byte[] value, final PieceWriteCallback pieceWC) {
        WriteResult pieceResult;
        KPiece piece = BlockHelper.getPieceRange(pieceNr, fileDetails);
        BlockMngr block = pendingBlocks.get(piece.parentBlock());
        if (block != null) {
            block.writePiece(piece.blockPieceNr(), value);
            if (block.isComplete() && file.hasHash(piece.parentBlock())) {
                writeBlock(piece.parentBlock(), block);
            }
            pieceResult = new WriteResult(piece.lowerAbsEndpoint(), value.length, "downloadTransferMngr");
        } else {
            pieceResult = new WriteResult(piece.lowerAbsEndpoint(), 0, "downloadTransferMngr");
        }
        pieceWC.success(Result.success(pieceResult));
    }

    @Override
    public void writeHash(int blockNr, byte[] value, HashWriteCallback hashWC) {
        KBlock hashRange = BlockHelper.getHashRange(blockNr, fileDetails);
        KReference<byte[]> hashVal = KReferenceFactory.getReference(value);
        file.writeHash(hashRange, hashVal, hashWC);
        //maybe late hash?
        BlockMngr block = pendingBlocks.get(blockNr);
        if (block != null && block.isComplete()) {
            writeBlock(blockNr, block);
        }
        WriteResult hashResult = new WriteResult(hashRange.lowerAbsEndpoint(), value.length, "downloadTransferMngr");
        hashWC.success(Result.success(hashResult));
    }

    @Override
    public void resetPiece(Pair<Integer, Integer> pieceNr) {
        LinkedList<Integer> aux = workPieces.get(pieceNr.getValue0());
        if (aux == null) {
            aux = new LinkedList<>();
            workPieces.put(pieceNr.getValue0(), aux);
        }
        aux.add(pieceNr.getValue1());
    }

    @Override
    public void resetHashes(Set<Integer> missingHashes) {
        nextHashes.addAll(missingHashes);
    }

    @Override
    public Pair<Integer, Integer> nextPiece() {
        Map.Entry<Integer, LinkedList<Integer>> aux = workPieces.firstEntry();
        Pair<Integer, Integer> piece = Pair.with(aux.getKey(), aux.getValue().removeFirst());
        if (aux.getValue().isEmpty()) {
            workPieces.remove(aux.getKey());
        }
        return piece;
    }

    @Override
    public Pair<Integer, Set<Integer>> nextHashes() {
        TreeSet<Integer> hashes = new TreeSet<>(workHashes);
        workHashes.clear();
        int nextHashPos = hashes.first();
        return Pair.with(nextHashPos, (Set<Integer>) hashes);
    }

    @Override
    public double percentageComplete() {
        int currentBlock = file.filePos();
        int totalBlocks = fileDetails.nrBlocks;
        double percentage;
        if (currentBlock == -1) {
            percentage = 100;
        } else {
            percentage = currentBlock;
            percentage = percentage * 100 / totalBlocks;
        }
        return percentage;
    }

    //*********************************READER***********************************
    @Override
    public boolean hasBlock(int blockNr) {
        return file.hasBlock(blockNr);
    }

    @Override
    public boolean hasHash(int blockNr) {
        return file.hasBlock(blockNr);
    }

    @Override
    public void readHash(int blockNr, HashReadCallback delayedResult) {
        KBlock hashRange = BlockHelper.getHashRange(blockNr, fileDetails);
        file.readHash(hashRange, delayedResult);
    }

    @Override
    public void readBlock(int blockNr, ReadCallback delayedResult) {
        KBlock blockRange = BlockHelper.getBlockRange(blockNr, fileDetails);
        file.read(blockRange, delayedResult);
    }

    @Override
    public void readPiece(Pair<Integer, Integer> pieceNr, PieceReadCallback pieceRC) {
        KPiece pieceRange = BlockHelper.getPieceRange(pieceNr, fileDetails);
        file.read(pieceRange, pieceRC);
    }

    //**************************************************************************
    public DownloadTMReport report() {
        return new DownloadTMReport(file.report(), workPos, hashPos, cachePos, pendingBlocks.size());
    }
    
    public long fileLength() {
        return fileDetails.length;
    }
}
