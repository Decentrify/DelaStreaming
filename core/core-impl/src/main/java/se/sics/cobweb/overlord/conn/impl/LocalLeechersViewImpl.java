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
package se.sics.cobweb.overlord.conn.impl;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import se.sics.cobweb.overlord.conn.api.ConnectionState;
import se.sics.cobweb.overlord.conn.api.LocalLeechersView;
import se.sics.cobweb.overlord.conn.api.SeederState;
import se.sics.cobweb.overlord.conn.api.TorrentState;
import se.sics.cobweb.util.FileId;
import se.sics.cobweb.util.HandleId;
import se.sics.ktoolbox.util.identifiable.overlay.OverlayId;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class LocalLeechersViewImpl implements LocalLeechersView {
  private final Map<OverlayId, Set<FileId>> ongoing = new HashMap<>();
  
  public void newFile(OverlayId torrentId, FileId fileId) {
    Set<FileId> aux = ongoing.get(torrentId);
    if(aux == null) {
      aux = new HashSet<>();
      ongoing.put(torrentId, aux);
    }
    aux.add(fileId);
  }
  
  public void completeFile(OverlayId torrentId, FileId fileId) {
    Set<FileId> aux = ongoing.get(torrentId);
    if(aux == null) {
      return;
    }
    aux.remove(fileId);
    if(aux.isEmpty()) {
      ongoing.remove(torrentId);
    }
  }
  
  public void setConnection(ConnectionState state, SeederState leecher, TorrentState torrent) {
  }

  @Override
  public Set<HandleId> interested(SeederState seeder, TorrentState torrent) {
    Set<HandleId> handles = new TreeSet<>();
    if(!ongoing.containsKey(torrent.torrentId)) {
      return handles;
    }
    for(FileId file : ongoing.get(torrent.torrentId)) {
      HandleId handle = file.seeder(seeder.peer.getId());
      handles.add(handle);
    }
    return handles;
  }
}
