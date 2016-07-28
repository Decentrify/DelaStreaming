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
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import se.sics.ktoolbox.util.identifiable.Identifier;
import se.sics.ktoolbox.util.reference.KReference;
import se.sics.ktoolbox.util.reference.KReferenceException;
import se.sics.ktoolbox.util.reference.KReferenceFactory;
import se.sics.ktoolbox.util.result.Result;
import se.sics.nstream.util.StreamControl;
import se.sics.nstream.storage.buffer.WriteResult;
import se.sics.nstream.storage.cache.DelayedRead;
import se.sics.nstream.storage.cache.KHint;
import se.sics.nstream.storage.managed.AppendFileMngr;
import se.sics.nstream.storage.managed.FileBWC;
import se.sics.nstream.util.result.BlockWriteCallback;
import se.sics.nstream.util.FileBaseDetails;
import se.sics.nstream.util.result.HashWriteCallback;
import se.sics.nstream.util.result.PieceWriteCallback;
import se.sics.nstream.util.range.KBlock;
import se.sics.nstream.util.range.KPiece;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class DownloadTransferMngr implements StreamControl, TransferMngr.Writer, TransferMngr.Reader {

    private final FileBaseDetails fileDetails;
    private final AppendFileMngr file;

    private final Map<Integer, BlockMngr> pendingBlocks = new HashMap<>();
    private final TreeSet<Long> nextPieces = new TreeSet<>();
    private final TreeSet<Integer> nextBlocks = new TreeSet<>();
    private KHint.Summary oldHint;

    public DownloadTransferMngr(FileBaseDetails fileDetails, AppendFileMngr file) {
        this.fileDetails = fileDetails;
        this.file = file;
        oldHint = new KHint.Summary(0, new TreeSet<Integer>());
    }

    //*********************************CONTROL**********************************
    @Override
    public void start() {
        file.start();
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

    //****************************TRANSFER_HINT_WRITE***************************
    @Override
    public KHint.Summary getFutureReads(int hintedBlockSpeed) {
        int newNext = hintedBlockSpeed - nextBlocks.size();
        if (newNext > 0) {
            Set<Integer> hintSet = new TreeSet<>();
            hintSet.addAll(pendingBlocks.keySet());
            hintSet.addAll(nextBlocks);
            Set<Integer> newBlocks = file.nextBlocksMissing(0, newNext, hintSet);
            newBlocks.addAll(file.nextHashesMissing(0, newNext, hintSet));
            if (!newBlocks.isEmpty()) {
                nextBlocks.addAll(newBlocks);
                hintSet.addAll(newBlocks);
                oldHint = new KHint.Summary(oldHint.lStamp + 1, hintSet);
            }
        } else {
            //maybe i finished something
            Set<Integer> hintSet = new TreeSet<>();
            hintSet.addAll(pendingBlocks.keySet());
            hintSet.addAll(nextBlocks);
            if (oldHint.blocks.size() > nextBlocks.size() + pendingBlocks.size()) {
                oldHint = new KHint.Summary(oldHint.lStamp + 1, hintSet);
            }
        }
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
    public boolean moreWork() {
        return !nextPieces.isEmpty() || !nextBlocks.isEmpty();
    }

    @Override
    public boolean pendingWork() {
        return !pendingBlocks.isEmpty();
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
    public void writePiece(long pieceNr, byte[] value, final PieceWriteCallback delayedResult) {
        KPiece piece = BlockHelper.getPieceRange(pieceNr, fileDetails);
        BlockMngr block = pendingBlocks.get(piece.parentBlock());
        block.writePiece(piece.blockPieceNr(), value);
        if (block.isComplete()) {
            final KBlock blockRange = BlockHelper.getBlockRange(piece.parentBlock(), fileDetails);
            byte[] blockBytes = block.getBlock();
            final KReference<byte[]> blockRef = KReferenceFactory.getReference(blockBytes);
            final BlockWriteCallback blockWC = delayedResult.getBlockCallback();
            FileBWC fileBWC = new FileBWC() {
                @Override
                public void hashResult(Result<Boolean> result) {
                    if (result.isSuccess()) {
                        if (result.getValue()) {
                            pendingBlocks.remove(blockRange.parentBlock());
                        } else {
                            BlockMngr blockMngr = new InMemoryBlockMngr(fileDetails.getBlockDetails(blockRange.parentBlock()));
                            pendingBlocks.put(blockRange.parentBlock(), blockMngr);
                            nextBlocks.add(blockRange.parentBlock());
                            blockWC.success(Result.success(new WriteResult(blockRange.lowerAbsEndpoint(), 0, "downloadTransferMngr")));
                            silentRelease(blockRef);
                        }
                    }
                }

                @Override
                public boolean fail(Result<WriteResult> result) {
                    silentRelease(blockRef);
                    return blockWC.fail(result);
                }

                @Override
                public boolean success(Result<WriteResult> result) {
                    silentRelease(blockRef);
                    return blockWC.success(result);
                }
            };
            file.writeBlock(blockRange, blockRef, fileBWC);
        }
        WriteResult pieceResult = new WriteResult(piece.lowerAbsEndpoint(), 0, "transfer");
        delayedResult.success(Result.success(pieceResult));
    }

    private void silentRelease(KReference<byte[]> ref) {
        try {
            ref.release();
        } catch (KReferenceException ex) {
            throw new RuntimeException(ex);
        }
    }

    @Override
    public void writeHash(int blockNr, byte[] value, HashWriteCallback delayedResult) {
        KBlock hashRange = BlockHelper.getHashRange(blockNr, fileDetails);
        KReference<byte[]> hashVal = KReferenceFactory.getReference(value);
        file.writeHash(hashRange, hashVal, delayedResult);
    }

    @Override
    public void resetPiece(long pieceId) {
        nextPieces.add(pieceId);
    }

    @Override
    public int nextBlock() {
        int next = nextBlocks.first();
        nextBlocks.remove(next);
        BlockMngr blockMngr = new InMemoryBlockMngr(fileDetails.getBlockDetails(next));
        pendingBlocks.put(next, blockMngr);
        return next;
    }

    @Override
    public Set<Long> nextPieces(int nrPieces) {
        Set<Long> next = new TreeSet<>();
        Iterator<Long> it = nextPieces.iterator();
        while (it.hasNext() && nrPieces > 0) {
            next.add(it.next());
            it.remove();
            nrPieces--;
        }
        return next;
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
    public void readHash(int blockNr, DelayedRead delayedResult) {
        KBlock hashRange = BlockHelper.getHashRange(blockNr, fileDetails);
        file.readHash(hashRange, delayedResult);
    }

    @Override
    public void readBlock(int blockNr, DelayedRead delayedResult) {
        KBlock blockRange = BlockHelper.getBlockRange(blockNr, fileDetails);
        file.read(blockRange, delayedResult);
    }
}
