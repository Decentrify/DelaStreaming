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

import java.util.Set;
import org.javatuples.Pair;
import se.sics.nstream.storage.cache.CacheHint;
import se.sics.nstream.util.result.HashReadCallback;
import se.sics.nstream.util.result.HashWriteCallback;
import se.sics.nstream.util.result.PieceReadCallback;
import se.sics.nstream.util.result.PieceWriteCallback;
import se.sics.nstream.util.result.ReadCallback;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class TransferMngr {

    public static interface Reader extends CacheHint.Read {

        public boolean hasBlock(int blockNr);

        public boolean hasHash(int blockNr);

        public void readHash(int blockNr, HashReadCallback delayedResult);
        
        public void readPiece(Pair<Integer, Integer> pieceNr, PieceReadCallback pieceRC);
        
        public void readBlock(int blockNr, ReadCallback delayedResult);
    }

    public static interface Writer extends CacheHint.Write {

        public boolean workAvailable();
        
        public boolean hashesAvailable();

        public boolean pendingBlocks();
        
        public boolean isComplete();

        public void writeHash(int blockNr, byte[] hash, HashWriteCallback delayedResult);
        
        public void writePiece(Pair<Integer, Integer> pieceNr, byte[] val, PieceWriteCallback delayedResult);
        
        public void resetHashes(Set<Integer> missingHashes);
        
        public void resetPiece(Pair<Integer, Integer> pieceNr);
        
        public Pair<Integer, Integer> nextPiece();
        
        public Pair<Integer, Set<Integer>> nextHashes();
        
        public double percentageComplete();
    }
}
