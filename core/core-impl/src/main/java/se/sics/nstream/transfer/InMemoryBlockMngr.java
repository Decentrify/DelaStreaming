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

package se.sics.nstream.transfer;

import java.util.HashSet;
import java.util.Set;
import se.sics.nstream.tracker.ComponentTracker;
import se.sics.nstream.tracker.IncompleteTracker;
import se.sics.nstream.util.BlockDetails;

/**
 * @author Alex Ormenisan <aaor@sics.se>
 */
public class InMemoryBlockMngr implements BlockMngr {
    private final BlockDetails blockDetails;
    private final RWByteBuffer storage;
    private final ComponentTracker tracker;
    
    public InMemoryBlockMngr(BlockDetails blockDetails) {
        this.blockDetails = blockDetails;
        this.storage = new RWByteBuffer(blockDetails.blockSize);
        this.tracker = IncompleteTracker.create(blockDetails.nrPieces);
        
    }

    @Override
    public boolean hasPiece(int pieceNr) {
        return tracker.hasComponent(pieceNr);
    }

    @Override
    public int writePiece(int pieceNr, byte[] piece) {
        tracker.addComponent(pieceNr);
        long writePos = pieceNr * blockDetails.defaultPieceSize;
        return storage.write(writePos, piece);
    }

    @Override
    public boolean isComplete() {
        return tracker.isComplete();
    }

    @Override
    public byte[] getBlock() {
        return storage.read(null, 0, blockDetails.blockSize, null);
    }

    @Override
    public int nrPieces() {
        return blockDetails.nrPieces;
    }

    @Override
    public Set<Integer> pendingPieces() {
        return tracker.nextComponentMissing(0, blockDetails.nrPieces, new HashSet<Integer>());
    }
}
