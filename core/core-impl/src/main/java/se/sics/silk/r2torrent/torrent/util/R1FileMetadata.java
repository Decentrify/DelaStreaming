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
package se.sics.silk.r2torrent.torrent.util;

import se.sics.nstream.util.BlockDetails;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class R1FileMetadata {

  public final int finalBlock;
  public final long length;
  public final int nrBlocks;
  public final BlockDetails defaultBlock;
  public final BlockDetails lastBlock;

  private R1FileMetadata(long length, int nrBlocks, BlockDetails defaultBlock, BlockDetails lastBlock) {
    this.length = length;
    this.nrBlocks = nrBlocks;
    this.defaultBlock = defaultBlock;
    this.lastBlock = lastBlock;
    this.finalBlock = nrBlocks-1;
  }

  public static R1FileMetadata instance(long fileLength, BlockDetails defaultBlock) {
    int nrBlocks;
    int lastBlockSize = (int)(fileLength % defaultBlock.blockSize);
    BlockDetails lastBlock;
    if(lastBlockSize == 0) {
      nrBlocks = (int)(fileLength / defaultBlock.blockSize); 
      lastBlock = defaultBlock;
    } else{
      nrBlocks = (int)(fileLength / defaultBlock.blockSize + 1);
      int lastPieceSize = lastBlockSize % defaultBlock.defaultPieceSize;
      int nrPieces;
      if(lastPieceSize == 0) {
        lastPieceSize = defaultBlock.defaultPieceSize;
        nrPieces = (int)(lastBlockSize / defaultBlock.defaultPieceSize);
      } else {
        nrPieces = (int)(lastBlockSize / defaultBlock.defaultPieceSize) + 1;
      }
      lastBlock = new BlockDetails(lastBlockSize, nrPieces, defaultBlock.defaultPieceSize, lastPieceSize);
    }
    return new R1FileMetadata(fileLength, nrBlocks, defaultBlock, lastBlock);
  }
}
