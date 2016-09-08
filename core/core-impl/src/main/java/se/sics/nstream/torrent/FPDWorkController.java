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
package se.sics.nstream.torrent;

import com.google.common.base.Optional;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import org.javatuples.Pair;
import se.sics.nstream.storage.cache.KHint;
import se.sics.nstream.transfer.BlockMngr;
import se.sics.nstream.transfer.InMemoryBlockMngr;
import se.sics.nstream.util.BlockDetails;
import se.sics.nstream.util.FileBaseDetails;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class FPDWorkController {

    private final static int ONGOING_SIZE = 10;
    private final static int CACHE_SIZE = 20;//work+cached+pendingCache
    //**************************************************************************
    private final FileBaseDetails fileDetails;
    //**************************************************************************
    private KHint.Summary oldHint;
    /**
     * cachedBlocks - blocks that have been confirmed as being available and
     * cached on the seeder side
     */
    private final LinkedList<Integer> cachedBlocks = new LinkedList<>();
    /**
     * pendingCacheBlocks - blocks that I would like, but not sure yet if the
     * seeder has it available in cache (or at all)
     */
    private final LinkedList<Integer> pendingCacheBlocks = new LinkedList<>();
    /**
     * nextBlocks - blocks the application wants me to get and I didn't get a
     * change to even ask the seeder if it has them
     */
    private final LinkedList<Integer> nextBlocks = new LinkedList<>();
    //**************************************************************************
    private final Map<Integer, BlockMngr> ongoingBlocks = new HashMap<>();
    private final TreeMap<Integer, LinkedList<Integer>> nextPieces = new TreeMap<>();
    private final TreeMap<Integer, LinkedList<Integer>> pendingPieces = new TreeMap<>();

    public FPDWorkController(FileBaseDetails fileDetails) {
        this.fileDetails = fileDetails;
        oldHint = new KHint.Summary(0, new TreeSet<Integer>());
    }

    public void add(List<Integer> newBlocks) {
        nextBlocks.addAll(newBlocks);
    }

    public void cached(List<Integer> confirmed) {
        cachedBlocks.addAll(confirmed);
        pendingCacheBlocks.removeAll(confirmed);
    }

    public Pair<Integer, Optional<KHint.Summary>> next() {
        Optional<KHint.Summary> nextHint = Optional.absent();
        if (nextPieces.isEmpty()) {
            nextHint = newWorkPieces();
        }
    }

    //**************************************************************************
    private Optional<KHint.Summary> newWorkPieces() {
        assert nextPieces.isEmpty();

        Optional<KHint.Summary> nextHint = Optional.absent();
        nextHint = moreWork();
        
        if (!cachedBlocks.isEmpty()) {
            if (ongoingBlocks.size() < ONGOING_SIZE) {
                newPendingBlock(cachedBlocks.removeFirst());
            }
        }
        return nextHint;
    }

    private void newPendingBlock(int blockNr) {
        BlockDetails blockDetails = fileDetails.getBlockDetails(blockNr);
        BlockMngr blockMngr = new InMemoryBlockMngr(blockDetails);
        ongoingBlocks.put(blockNr, blockMngr);
        LinkedList<Integer> pieceList = new LinkedList<>();
        for (int i = 0; i < blockDetails.nrPieces; i++) {
            pieceList.add(i);
        }
        nextPieces.put(blockNr, pieceList);
    }

    private Optional<KHint.Summary> moreWork() {
        boolean changed;
        int maxNewPendingCacheBlocks = CACHE_SIZE - ongoingBlocks.size() - cachedBlocks.size() - pendingCacheBlocks.size();
        changed = move(pendingCacheBlocks, nextBlocks, maxNewPendingCacheBlocks);

        Optional<KHint.Summary> nextHint = Optional.absent();
        if (changed) {
            nextHint = Optional.of(rebuildCacheHint());
        }
        return nextHint;
    }

    private KHint.Summary rebuildCacheHint() {
        Set<Integer> hint = new TreeSet<>();
        hint.addAll(ongoingBlocks.keySet());
        hint.addAll(cachedBlocks);
        hint.addAll(pendingCacheBlocks);
        oldHint = new KHint.Summary(oldHint.lStamp + 1, hint);
        return oldHint;
    }

    private boolean move(LinkedList<Integer> to, LinkedList<Integer> from, int maxAmount) {
        boolean changed = false;
        Iterator<Integer> aux = from.iterator();
        while (aux.hasNext() && to.size() <= maxAmount) {
            Integer nB = aux.next();
            aux.remove();
            to.add(nB);
            changed = true;
        }
        return changed;
    }
}
