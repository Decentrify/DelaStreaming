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

import java.util.Set;
import se.sics.dela.storage.buffer.WriteCallback;
import se.sics.dela.storage.buffer.WriteResult;
import se.sics.dela.storage.cache.KHint;
import se.sics.dela.storage.cache.ReadCallback;
import se.sics.kompics.util.Identifier;
import se.sics.ktoolbox.util.managedStore.core.util.HashUtil;
import se.sics.ktoolbox.util.reference.KReference;
import se.sics.ktoolbox.util.result.Result;
import se.sics.nstream.tracker.ComponentTracker;
import se.sics.nstream.tracker.IncompleteTracker;
import se.sics.nstream.util.BlockHelper;
import se.sics.nstream.util.FileBaseDetails;
import se.sics.nstream.util.range.KBlock;
import se.sics.nstream.util.range.KRange;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class AppendFileMngr implements StreamControl, FileMngr.Reader, FileMngr.Writer {

    private final FileBaseDetails fileDetails;
    private final AsyncIncompleteStorage file;
    private final ComponentTracker fileTracker;
    private final AsyncOnDemandHashStorage hash;
    private final ComponentTracker hashTracker;

    public AppendFileMngr(FileBaseDetails fileDetails, AsyncIncompleteStorage file, AsyncOnDemandHashStorage hash, 
      int lastContainedDataBlock, int lastContainedHashBlock) {
        this.fileDetails = fileDetails;
        this.file = file;
        this.fileTracker = IncompleteTracker.create(fileDetails.nrBlocks, lastContainedDataBlock);
        this.hash = hash;
        this.hashTracker = IncompleteTracker.create(fileDetails.nrBlocks, lastContainedHashBlock);
    }

    @Override
    public void start() {
        file.start();
        hash.start();
    }

    @Override
    public boolean isIdle() {
        return file.isIdle() && hash.isIdle();
    }
    
    @Override
    public void close() {
        file.close();
        hash.close();
    }

    public CompleteFileMngr complete() {
        return new CompleteFileMngr(fileDetails, file.complete(), hash);
    }

    //*******************************BASIC_READ*********************************
    @Override
    public void read(KRange readRange, ReadCallback delayedResult) {
        file.read(readRange, delayedResult);
    }

    //***************************CACHE_HINT_READ*****************************

    @Override
    public void clean(Identifier reader) {
        file.clean(reader);
        hash.clean(reader);
    }

    @Override
    public void setFutureReads(Identifier reader, KHint.Expanded hint) {
        file.setFutureReads(reader, hint);
        hash.setFutureReads(reader, hint);
    }

    //*********************************READER***********************************
    @Override
    public boolean hasBlock(int blockNr) {
        return fileTracker.hasComponent(blockNr);
    }

    @Override
    public boolean hasHash(int blockNr) {
        return hashTracker.hasComponent(blockNr);
    }

    @Override
    public Set<Integer> nextBlocksMissing(int fromBlock, int nrBlocks, Set<Integer> except) {
        return fileTracker.nextComponentMissing(fromBlock, nrBlocks, except);
    }

    @Override
    public Set<Integer> nextHashesMissing(int fromBlock, int nrBlocks, Set<Integer> except) {
        return fileTracker.nextComponentMissing(fromBlock, nrBlocks, except);
    }

    @Override
    public void readHash(KBlock readRange, HashReadCallback delayedResult) {
        hash.read(readRange, delayedResult);
    }

    //*******************************WRITER*************************************
    @Override
    public boolean pendingBlocks() {
        return file.pendingBlocks();
    }
    
    @Override
    public void writeHash(final KBlock writeRange, KReference<byte[]> val, final WriteCallback delayedResult) {
        WriteCallback hashResult = new WriteCallback() {

            @Override
            public boolean success(Result<WriteResult> result) {
                hashTracker.addComponent(writeRange.parentBlock());
                return delayedResult.success(result);
            }

            @Override
            public boolean fail(Result<WriteResult> result) {
                return delayedResult.fail(result);
            }
        };
        hash.write(writeRange, val, hashResult);
    }

    @Override
    public void writeBlock(final KBlock writeRange, final KReference<byte[]> val, final FileBWC blockWC) {
        final int blockNr = writeRange.parentBlock();
        ReadCallback hashRead = new ReadCallback() {

            @Override
            public boolean fail(Result<KReference<byte[]>> result) {
                blockWC.hashResult((Result) result);
                return false;
            }

            @Override
            public boolean success(Result<KReference<byte[]>> result) {
                validatedWrite(writeRange, result.getValue(), val, blockWC);
                return true;
            }
        };

        KBlock hashRange = BlockHelper.getHashRange(blockNr, fileDetails);
        hash.read(hashRange, hashRead);
    }

    private void validatedWrite(final KBlock writeRange, KReference<byte[]> hash, KReference<byte[]> val, final FileBWC blockWC) {
        if (HashUtil.checkHash(fileDetails.hashAlg, val.getValue().get(), hash.getValue().get())) {
            fileTracker.addComponent(writeRange.parentBlock());
            blockWC.hashResult(Result.success(true));
            WriteCallback pieceWrite = new WriteCallback() {

                @Override
                public boolean fail(Result<WriteResult> result) {
                    return blockWC.fail(result);
                }

                @Override
                public boolean success(Result<WriteResult> result) {
                    return blockWC.success(result);
                }
            };
            file.write(writeRange, val, pieceWrite);
        } else {
            blockWC.hashResult(Result.success(false));
        }
    }
    
    //TODO Alex - pos 0
    @Override
    public boolean isComplete() {
        return hashTracker.isComplete() && fileTracker.isComplete();
    }

    @Override
    public int filePos() {
        return fileTracker.nextComponentMissing(0);
    }

    @Override
    public int hashPos() {
        return hashTracker.nextComponentMissing(0);
    }
    
    //**************************************************************************
    public AppendFMReport report() {
        return new AppendFMReport(fileTracker.nextComponentMissing(0), hashTracker.nextComponentMissing(0), file.report());
    }
}
