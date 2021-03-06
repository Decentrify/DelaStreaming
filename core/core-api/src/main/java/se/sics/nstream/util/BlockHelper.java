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
package se.sics.nstream.util;

import org.javatuples.Pair;
import se.sics.ktoolbox.util.managedStore.core.util.HashUtil;
import se.sics.nstream.util.range.KBlock;
import se.sics.nstream.util.range.KBlockImpl;
import se.sics.nstream.util.range.KPiece;
import se.sics.nstream.util.range.KPieceImpl;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class BlockHelper {

    public static Pair<Integer, BlockDetails> getFileDetails(long length, BlockDetails defaultBlock) {
        if (length == 0) {
            return Pair.with(0, new BlockDetails(0, 0, defaultBlock.defaultPieceSize, 0));
        }
        int nrBlocks = length % defaultBlock.blockSize == 0 ? (int) (length / defaultBlock.blockSize) : (int) (length / defaultBlock.blockSize + 1);
        BlockDetails lastBlock;
        if (length % defaultBlock.blockSize == 0) {
            lastBlock = defaultBlock;
        } else {
            int lastBlockSize = (int) (length % defaultBlock.blockSize);
            int nrPieces, lastPieceSize;
            if (lastBlockSize % defaultBlock.defaultPieceSize == 0) {
                nrPieces = (int) (lastBlockSize / defaultBlock.defaultPieceSize);
                lastPieceSize = defaultBlock.defaultPieceSize;
            } else {
                nrPieces = (int) (lastBlockSize / defaultBlock.defaultPieceSize + 1);
                lastPieceSize = lastBlockSize % defaultBlock.defaultPieceSize;
            }
            lastBlock = new BlockDetails(lastBlockSize, nrPieces, defaultBlock.defaultPieceSize, lastPieceSize);
        }
        
        return Pair.with(nrBlocks, lastBlock);
    }
    
    public static int getBlockNrFromPos(long pos, FileBaseDetails fileDetails) {
        int blockNr = (int)(pos / fileDetails.defaultBlock.blockSize);
        if (blockNr == fileDetails.nrBlocks - 1) {
            int lastBlockSize = (int)(pos % fileDetails.defaultBlock.blockSize);
            if(lastBlockSize == fileDetails.lastBlock.blockSize) {
                blockNr++;
            }
        }
        return blockNr;
    }

    public static int getBlockNr(long pieceNr, FileBaseDetails fileDetails) {
        int blockNr = (int) (pieceNr / fileDetails.defaultBlock.nrPieces);
        return blockNr;
    }

    public static int getBlockPieceNr(long pieceNr, FileBaseDetails fileDetails) {
        int bpNr = (int) (pieceNr % fileDetails.defaultBlock.nrPieces);
        return bpNr;
    }
    
    public static long getBlockPos(int blockNr, BlockDetails defaultBlock) {
        return blockNr * defaultBlock.blockSize;
    }

    public static KBlock getBlockRange(int blockNr, FileBaseDetails fileDetails) {
        BlockDetails blockDetails = fileDetails.getBlockDetails(blockNr);
        long lower = blockNr * fileDetails.defaultBlock.blockSize;
        long higher = lower + blockDetails.blockSize - 1;
        return new KBlockImpl(blockNr, lower, higher);
    }

    public static KPiece getPieceRange(Pair<Integer, Integer> pieceNr, BlockDetails blockDetails, BlockDetails defaultBlockDetails) {
        return getPieceRange(pieceNr.getValue0(), pieceNr.getValue1(), blockDetails, defaultBlockDetails);
    }
    
    public static KPiece getPieceRange(int blockNr, int pieceBlockNr, BlockDetails blockDetails, BlockDetails defaultBlockDetails) {
        int pieceSize = blockDetails.getPieceSize(pieceBlockNr);
        long lower = blockNr * defaultBlockDetails.blockSize + pieceBlockNr * blockDetails.defaultPieceSize;
        long higher = lower + pieceSize - 1;
        
        return new KPieceImpl(blockNr, pieceBlockNr, lower, higher);
    }
    
    public static KBlock getHashRange(int blockNr, FileBaseDetails fileDetails) {
        int hashSize = HashUtil.getHashSize(fileDetails.hashAlg);
        long lower = blockNr * hashSize;
        long higher = lower + hashSize - 1;
        return new KBlockImpl(blockNr, lower, higher);
    }
}
