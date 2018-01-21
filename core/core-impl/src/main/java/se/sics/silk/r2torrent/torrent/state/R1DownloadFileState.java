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
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;
import se.sics.kompics.util.Identifier;
import se.sics.ktoolbox.util.network.KAddress;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class R1DownloadFileState {

  Map<Identifier, FileSeederState> pendingSeeders = new HashMap<>();
  Map<Identifier, FileSeederState> connectedSeeders = new HashMap<>();

  public R1DownloadFileState() {
  }

  public void pendingSeeder(KAddress seeder) {
    pendingSeeders.put(seeder.getId(), new FileSeederState(seeder));
  }

  public void connectedSeeder(KAddress seeder) {
    connectedSeeders.put(seeder.getId(), pendingSeeders.remove(seeder.getId()));
  }

  public void disconnectedSeeder(Identifier seederId, Consumer<KAddress> action) {
    FileSeederState seederState = pendingSeeders.remove(seederId);
    if (seederState == null) {
      seederState = connectedSeeders.remove(seederId);
    }
    if (seederState != null) {
      action.accept(seederState.seeder);
    }
  }

  public void clearPending() {
    pendingSeeders.values().stream().forEach((fss) -> {
      fss.clear();
    });
    pendingSeeders.clear();
  }

  public void clearConnected() {
    connectedSeeders.values().stream().forEach((fss) -> {
      fss.clear();
    });
    connectedSeeders.clear();
  }

  public void connectedToPending() {
    connectedSeeders.values().stream().forEach((fss) -> {
      fss.pending();
      pendingSeeders.put(fss.seeder.getId(), fss);
    });
    connectedSeeders.clear();
  }

  public void allPending(Consumer<KAddress> action) {
    pendingSeeders.values().stream().forEach((fss) -> {
      action.accept(fss.seeder);
    });
  }

  public void allConnected(Consumer<KAddress> action) {
    connectedSeeders.values().stream().forEach((fss) -> {
      action.accept(fss.seeder);
    });
  }

  public Optional<KAddress> nextConnect() {
    Iterator<FileSeederState> it = pendingSeeders.values().iterator();
    if (it.hasNext()) {
      return Optional.of(it.next().seeder);
    } else {
      return Optional.empty();
    }
  }

  public boolean inactive() {
    return pendingSeeders.isEmpty() && connectedSeeders.isEmpty();
  }
}
