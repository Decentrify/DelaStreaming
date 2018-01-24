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
package se.sics.silk.r2torrent.transfer.events;

import java.util.Map;
import java.util.Set;
import org.javatuples.Pair;
import se.sics.kompics.util.Identifier;
import se.sics.ktoolbox.util.identifiable.BasicIdentifiers;
import se.sics.ktoolbox.util.identifiable.overlay.OverlayId;
import se.sics.silk.event.SilkEvent;
import se.sics.silk.r2torrent.torrent.R1FileDownload;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class R1DownloadEvents {
  public static class GetBlocks extends SilkEvent.E2 implements R1DownloadEvent {
    public final Set<Integer> blocks;
    public GetBlocks(OverlayId torrentId, Identifier fileId, Identifier seederId, 
      Set<Integer> blocks) {
      super(BasicIdentifiers.eventId(), torrentId, fileId, seederId);
      this.blocks = blocks;
    }
  }
  
  public static class Completed extends SilkEvent.E2 implements R1FileDownload.DownloadEvent {
    public final int block; 
    public final byte[] value;
    public final byte[] hash;
    public Completed(OverlayId torrentId, Identifier fileId, Identifier seederId, 
      int block, byte[] value, byte[] hash) {
      super(BasicIdentifiers.eventId(), torrentId, fileId, seederId);
      this.block = block;
      this.value = value;
      this.hash = hash;
    }
    
    public Completed(OverlayId torrentId, Identifier fileId, Identifier seederId, 
      Map.Entry<Integer, Pair<byte[], byte[]>> block) {
      this(torrentId, fileId, seederId, block.getKey(), block.getValue().getValue0(), block.getValue().getValue1());
    }
  }
}
