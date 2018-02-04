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

import com.google.common.collect.Sets;
import java.util.HashSet;
import java.util.Set;
import java.util.TreeSet;
import se.sics.nstream.tracker.ComponentTracker;
import se.sics.nstream.tracker.IncompleteTracker;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class R1FileDownloadTracker {

  private final ComponentTracker fileTracker;
  private final TreeSet<Integer> ongoingBlocks = new TreeSet<>();
  private final TreeSet<Integer> pendingBlocks = new TreeSet<>();
  private boolean finishingFile = false;

  public R1FileDownloadTracker(int startBlock, int nrBlocks) {
    fileTracker = IncompleteTracker.create(nrBlocks, startBlock);
  }

  public boolean hasBlock(int blockNr) {
    return fileTracker.hasComponent(blockNr);
  }

  public Set<Integer> nextBlocks(int nrBlocks) {
    Set<Integer> blocks = new HashSet<>();
    if (finishingFile) {
      return blocks;
    }
    if(pendingBlocks.size() <= nrBlocks) {
      blocks.addAll(pendingBlocks);
      pendingBlocks.clear();
    }
    int newBlocks = nrBlocks - blocks.size();
    blocks.addAll(fileTracker.nextComponentMissing(0, newBlocks, Sets.union(ongoingBlocks, blocks)));
    if (blocks.isEmpty()) {
      finishingFile = true;
      return blocks;
    }
    ongoingBlocks.addAll(blocks);
    return blocks;
  }
  
  public void completeBlock(Integer block) {
    ongoingBlocks.remove(block);
    fileTracker.addComponent(block);
  }
  
  public void resetBlocks(Set<Integer> blocks) {
    pendingBlocks.addAll(blocks);
    finishingFile = false;
  }

  public boolean isComplete() {
    return fileTracker.isComplete();
  }
}
