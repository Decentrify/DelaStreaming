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
package se.sics.dela.channel.bss;

import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class BlockSelectionStrategies {
  public static class AllInOrder implements BlockSelectionStrategy {
    private final TreeSet<Integer> hashes = new TreeSet<>();
    private final TreeSet<Integer> blocks = new TreeSet<>();
    private final int hashBatchSize;
    private final int hashAheadWindow;
    
    public AllInOrder(int nrBlocks, int hashBatchSize, int hashAheadWindow) {
      for(int i = 0; i < nrBlocks; i++) {
        hashes.add(i);
        blocks.add(i);
      }
      this.hashBatchSize = hashBatchSize;
      this.hashAheadWindow = hashAheadWindow;
    }

    @Override
    public Set<Integer> nextHashes() {
      Set<Integer> nextHashes = new TreeSet<>();
      if(!hashes.isEmpty() && !blocks.isEmpty() 
        && hashes.first() < blocks.first() + hashAheadWindow) {
        for(int i = 0; i < hashBatchSize; i++) {
          if(hashes.isEmpty()) {
            break;
          }
          nextHashes.add(hashes.pollFirst());
        }
      }
      return nextHashes;
    }

    @Override
    public void resetHashes(Set<Integer> resetHashes) {
      hashes.addAll(resetHashes);
    }

    @Override
    public Optional<Integer> nextBlock() {
      if(blocks.isEmpty()) {
        return Optional.empty();
      } else {
        return Optional.of(blocks.pollFirst());
      }
    }

    @Override
    public void resetBlock(int block) {
      blocks.add(block);
    }

    @Override
    public boolean isComplete() {
      return hashes.isEmpty() && blocks.isEmpty();
    }
  }
}
