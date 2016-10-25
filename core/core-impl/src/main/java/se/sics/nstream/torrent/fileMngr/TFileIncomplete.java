/*
 * Copyright (C) 2009 Swedish Institute of Computer Science (SICS) Copyright (C)
 * 2009 Royal Institute of Technology (KTH)
 *
 * GVoD is free software; you can redistribute it and/or
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
package se.sics.nstream.torrent.fileMngr;

import com.google.common.base.Optional;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
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
import se.sics.nstream.util.BlockHelper;
import se.sics.nstream.util.FileBaseDetails;
import se.sics.nstream.util.range.KBlock;
import se.sics.nstream.util.result.HashReadCallback;
import se.sics.nstream.util.result.HashWriteCallback;
import se.sics.nstream.util.result.ReadCallback;

/**
 *
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class TFileIncomplete implements TFileWrite, TFileRead {

    private static final int NEXT_BATCH_SIZE = 20;
    private static final int HASH_BATCH_SIZE = 20;
    //**************************************************************************
    private final FileBaseDetails fileDetails;
    //**************************************************************************
    private final AppendFileMngr file;
    //**************************************************************************
    private final Set<Integer> ongoingHashes = new TreeSet<>();
    private final Set<Integer> nextHashes = new TreeSet<>();
    private final TreeSet<Integer> ongoingBlocks = new TreeSet<>();
    private final TreeSet<Integer> nextBlocks = new TreeSet<>();
    private final Map<Integer, FileBWC> pendingStorageWrites = new HashMap<>();

    public TFileIncomplete(AppendFileMngr file, FileBaseDetails fileDetails) {
        this.file = file;
        this.fileDetails = fileDetails;
    }

    //******************************TFileMngr***********************************
    @Override
    public void start() {
        file.start();
        newNextBlocks();
    }

    @Override
    public boolean isIdle() {
        return file.isIdle();
    }

    @Override
    public void close() {
        file.close();
    }

    //*******************************CACHE_HINT*********************************
    @Override
    public void clean(Identifier reader) {
        file.clean(reader);
    }

    @Override
    public void setCacheHint(Identifier reader, KHint.Summary hint) {
        file.setFutureReads(reader, hint.expand(fileDetails));
    }

    //********************************READ_DATA*********************************
    @Override
    public boolean hasBlock(int blockNr) {
        return file.hasBlock(blockNr);
    }

    @Override
    public boolean hasHash(int blockNr) {
        return file.hasHash(blockNr);
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
    public Map<Integer, BlockDetails> getIrregularBlocks() {
        Map<Integer, BlockDetails> irregularBlocks = new HashMap<>();
        irregularBlocks.put(fileDetails.nrBlocks - 1, fileDetails.lastBlock);
        return irregularBlocks;
    }

    //*******************************WRITE_DATA*********************************
    @Override
    public boolean isComplete() {
        return file.isComplete();
    }

    @Override
    public TFileComplete complete() {
        return new TFileComplete(file.complete(), fileDetails);
    }

    @Override
    public boolean hasHashes() {
        if (nextBlocks.isEmpty()) {
            newNextBlocks();
        }
        return !nextHashes.isEmpty();
    }

    @Override
    public boolean hasBlocks() {
        if (nextBlocks.isEmpty()) {
            newNextBlocks();
        }
        return !nextBlocks.isEmpty();
    }

    @Override
    public Set<Integer> requestHashes() {
        Set<Integer> hashes = new TreeSet<>();
        Iterator<Integer> it = nextHashes.iterator();
        while (it.hasNext() && hashes.size() < HASH_BATCH_SIZE) {
            int hIdx = it.next();
            hashes.add(hIdx);
            ongoingHashes.add(hIdx);
            it.remove();
        }
        return hashes;
    }

    @Override
    public void hashes(Map<Integer, byte[]> hashes, Set<Integer> missingHashes) {
        nextHashes.removeAll(hashes.keySet()); //if we do not use the file hash request management
        nextHashes.addAll(missingHashes);
        ongoingHashes.removeAll(missingHashes);
        ongoingHashes.removeAll(hashes.keySet());
        for (Map.Entry<Integer, byte[]> hash : hashes.entrySet()) {
            final KReference<byte[]> hashRef = KReferenceFactory.getReference(hash.getValue());
            KBlock hashRange = BlockHelper.getHashRange(hash.getKey(), fileDetails);
            HashWriteCallback hashWC = new HashWriteCallback() {

                @Override
                public boolean fail(Result<WriteResult> result) {
                    throw new RuntimeException(result.getException());
                }

                @Override
                public boolean success(Result<WriteResult> result) {
                    silentRelease(hashRef);
                    return true;
                }
            };
            file.writeHash(hashRange, hashRef, hashWC);
        }
    }

    @Override
    public Pair<Integer, Optional<BlockDetails>> requestBlock() {
        int blockNr = nextBlocks.pollFirst();
        Optional<BlockDetails> irregularBlock = Optional.absent();
        if (blockNr == fileDetails.nrBlocks - 1) {
            irregularBlock = Optional.of(fileDetails.lastBlock);
        }
        ongoingBlocks.add(blockNr);
        return Pair.with(blockNr, irregularBlock);
    }

    @Override
    public void block(final int blockNr, final KReference<byte[]> block) {
        final KBlock blockRange = BlockHelper.getBlockRange(blockNr, fileDetails);
        block.retain();
        FileBWC fileBWC = new FileBWC() {
            @Override
            public void hashResult(Result<Boolean> result) {
                if (result.isSuccess()) {
                    //nothing - we write it
                } else {
                    //didn't hash or didn't get the hash - redownload - might also consider storing and retrying, but easier to just redownload
                    ongoingBlocks.remove(blockNr);
                    silentRelease(block);
                    nextBlocks.add(blockNr);
                }
            }

            @Override
            public boolean fail(Result<WriteResult> result) {
                throw new RuntimeException("failed to write into storage - " + result.getException().getMessage());
            }

            @Override
            public boolean success(Result<WriteResult> result) {
                ongoingBlocks.remove(blockNr);
                silentRelease(block);
                return true;
            }
        };
        file.writeBlock(blockRange, block, fileBWC);
    }

    @Override
    public void resetBlock(int blockNr) {
        ongoingBlocks.remove(blockNr);
        nextBlocks.add(blockNr);
    }

    //**************************************************************************
    private void newNextBlocks() {
        Set<Integer> nb = file.nextBlocksMissing(0, NEXT_BATCH_SIZE, ongoingBlocks);
        nextBlocks.addAll(nb);
        nextHashes.addAll(nb);
    }

    private void silentRelease(KReference<byte[]> ref) {
        try {
            ref.release();
        } catch (KReferenceException ex) {
            throw new RuntimeException("ref logic");
        }
    }

    // <totalSize, currentSize>
    public Pair<Long, Long> report() {
        long currentSize;
        if (!file.isComplete()) {
            currentSize = (long) file.filePos() * fileDetails.defaultBlock.blockSize;
        } else {
            currentSize = fileDetails.length;
        }
        return Pair.with(fileDetails.length, currentSize);
    }
}
