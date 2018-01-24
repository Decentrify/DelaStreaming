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

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import se.sics.kompics.util.Identifier;
import se.sics.ktoolbox.util.network.KAddress;
import se.sics.silk.r2torrent.storage.buffer.R1AsyncCheckedBuffer;
import se.sics.silk.r2torrent.torrent.R1FileDownload;
import se.sics.silk.r2torrent.torrent.util.R1FileDownloadTracker;
import se.sics.silk.r2torrent.torrent.util.R1FileMetadata;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class R1FileDownloadSeedersState {

  //to be used just by children;
  public final R1FileMetadata fileMetadata;
  public final R1FileDownloadTracker fileTracker;
  public final R1AsyncCheckedBuffer buffer; 
  public final R1FileDownload.SeederActions actions;
  //
  Map<Identifier, R1FileDownloadSeederState> pendingSeeders = new HashMap<>();
  Map<Identifier, R1FileDownloadSeederState> connectedSeeders = new HashMap<>();

  public R1FileDownloadSeedersState(R1FileMetadata fileMetadata, R1FileDownloadTracker fileTracker, 
    R1AsyncCheckedBuffer buffer, R1FileDownload.SeederActions actions) {
    this.fileMetadata = fileMetadata;
    this.fileTracker = fileTracker;
    this.buffer = buffer;
    this.actions = actions;
  }
  
  public void pending(KAddress seeder) {
    pendingSeeders.put(seeder.getId(), new R1FileDownloadSeederState(seeder, this));
  }

  public boolean connectPending(KAddress seeder) {
    R1FileDownloadSeederState fss = pendingSeeders.remove(seeder.getId());
    if (fss == null) {
      return false;
    }
    connectedSeeders.put(seeder.getId(), fss);
    fss.connected();
    return true;
  }
  
  public boolean connected(KAddress seeder) {
    R1FileDownloadSeederState fss = new R1FileDownloadSeederState(seeder, this);
    connectedSeeders.put(seeder.getId(), fss);
    fss.connected();
    return true;
  }
  
  public void disconnectPending() {
    pendingSeeders.values().stream().forEach((fss) -> {
      fss.clearPending();
    });
    pendingSeeders.clear();
  }

  public void disconnect() {
    connectedSeeders.values().stream().forEach((fss) -> {
      fss.disconnect();
    });
    connectedSeeders.clear();
  }
  
  public boolean disconnect(Identifier seederId) {
    R1FileDownloadSeederState fss = connectedSeeders.remove(seederId);
    if (fss == null) {
      return false;
    }
    fss.disconnect();
    return true;
  }
  
  public boolean disconnected(Identifier seederId) {
    R1FileDownloadSeederState fss = connectedSeeders.remove(seederId);
    if (fss == null) {
      return false;
    }
    fss.disconnected();
    return true;
  }
  
  public Optional<R1FileDownloadSeederState> state(Identifier seederId) {
    return Optional.ofNullable(connectedSeeders.get(seederId));
  }

  public boolean inactive() {
    return pendingSeeders.isEmpty() && connectedSeeders.isEmpty();
  }
}
