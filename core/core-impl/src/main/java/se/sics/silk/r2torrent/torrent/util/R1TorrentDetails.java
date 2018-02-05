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

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import se.sics.kompics.util.Identifier;
import se.sics.ktoolbox.util.identifiable.overlay.OverlayId;
import se.sics.nstream.StreamId;
import se.sics.nstream.storage.durable.util.MyStream;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class R1TorrentDetails {
  public static class Mngr {
    public final Map<OverlayId, R1TorrentDetails> torrents = new HashMap<>();
    
    public Optional<R1TorrentDetails> getTorrent(OverlayId torrentId) {
      return Optional.ofNullable(torrents.get(torrentId));
    }
    
    public void addTorrent(OverlayId torrentId, R1TorrentDetails torrentDetails) {
      torrents.put(torrentId, torrentDetails);
    }
  }
  
  public static enum FileStatus {
    INACTIVE,
    DOWNLOAD,
    UPLOAD
  }
  public final String hashAlg;
  public final Map<Identifier, R1FileMetadata> metadata = new HashMap<>();
  public final Map<Identifier, R1FileStorage> storage = new HashMap<>();
  public final TreeMap<Identifier, FileStatus> status = new TreeMap<>();

  public R1TorrentDetails(String hashAlg) {
    this.hashAlg = hashAlg;
  }

  public void addMetadata(Identifier fileId, R1FileMetadata m) {
    metadata.put(fileId, m);
  }

  public Optional<R1FileMetadata> getMetadata(Identifier fileId) {
    return Optional.ofNullable(metadata.get(fileId));
  }

  public void addStorage(Identifier fileId, StreamId streamId, MyStream stream) {
    storage.put(fileId, new R1FileStorage(streamId, stream));
  }

  public Optional<R1FileStorage> getStorage(Identifier fileId) {
    return Optional.ofNullable(storage.get(fileId));
  }
  
  public void download() {
    metadata.keySet().stream().forEach((f) -> status.put(f, FileStatus.INACTIVE));
  }
  
  public void upload() {
    metadata.keySet().stream().forEach((f) -> status.put(f, FileStatus.UPLOAD));
  }
  
  public void download(Identifier fileId) {
    status.put(fileId, FileStatus.DOWNLOAD);
  }
  
  public void completed(Identifier fileId) {
    status.put(fileId, FileStatus.UPLOAD);
  }
  public boolean isComplete() {
    for(FileStatus fs : status.values()) {
      if(!FileStatus.UPLOAD.equals(fs)) {
        return false;
      } 
    }
    return true;
  }
  
  public boolean isComplete(Identifier fileId) {
    return FileStatus.UPLOAD.equals(status.get(fileId));
  }
  
  public Optional<Identifier> nextInactive() {
    for(Map.Entry<Identifier, FileStatus> e : status.entrySet()) {
      if(FileStatus.INACTIVE.equals(e.getValue())) {
        return Optional.of(e.getKey());
      }
    }
    return Optional.empty();
  }
}
