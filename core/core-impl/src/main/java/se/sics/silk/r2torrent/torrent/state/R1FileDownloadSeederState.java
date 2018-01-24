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
package se.sics.silk.r2torrent.torrent.state;

import java.util.HashSet;
import java.util.Set;
import se.sics.kompics.Component;
import se.sics.ktoolbox.util.network.KAddress;
import se.sics.ktoolbox.util.reference.KReferenceFactory;
import se.sics.silk.r2torrent.torrent.R1FileDownload.HardCodedConfig;
import se.sics.silk.r2torrent.torrent.util.R1BlockHelper;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class R1FileDownloadSeederState {

  //ok to access externally
  public final KAddress seeder;
  //
  private final R1FileDownloadSeedersState parent;
  //only access through actions
  public final Set<Integer> ongoingBlocks = new HashSet<>();
  public Component comp;

  public R1FileDownloadSeederState(KAddress seeder, R1FileDownloadSeedersState parent) {
    this.seeder = seeder;
    this.parent = parent;
  }

  public void connected() {
    parent.actions.createDownloadComp(this);
    tryDownload();
  }

  public void disconnect() {
    parent.fileTracker.resetBlocks(ongoingBlocks);
    parent.actions.killDownloadComp(this);
    parent.actions.disconnect(seeder);
  }

  public void disconnected() {
    parent.fileTracker.resetBlocks(ongoingBlocks);
    parent.actions.killDownloadComp(this);
    parent.actions.disconnected(seeder);
  }

  public void clearPending() {
    parent.actions.disconnect(seeder);
  }

  public void complete(Integer block, byte[] value, byte[] hash) {
    if (ongoingBlocks.remove(block)) {
      parent.fileTracker.completeBlock(block);
      long pos = R1BlockHelper.posFromBlockNr(block, parent.fileMetadata);
      parent.buffer.writeBlock(pos, KReferenceFactory.getReference(value));
      parent.buffer.writeHash(pos, KReferenceFactory.getReference(hash));
    }
    tryDownload();
  }
  
  private void tryDownload() {
    if (ongoingBlocks.size() < HardCodedConfig.DOWNLOAD_COMP_BUFFER_SIZE - HardCodedConfig.BLOCK_BATCH_REQUEST) {
      Set<Integer> newBlocks = parent.fileTracker.nextBlocks(HardCodedConfig.BLOCK_BATCH_REQUEST); 
      ongoingBlocks.addAll(newBlocks);
      parent.actions.download(seeder, newBlocks);
    }
  }
}
