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
import se.sics.kompics.util.Identifier;
import se.sics.ktoolbox.util.identifiable.BasicIdentifiers;
import se.sics.ktoolbox.util.identifiable.overlay.OverlayId;
import se.sics.nstream.util.BlockDetails;
import se.sics.silk.event.SilkEvent;
import se.sics.silk.r2torrent.torrent.R1FileGet;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class R1DownloadEvents {
  public static class GetBlock extends SilkEvent.E2 implements R1DownloadEvent {
    public final Set<Integer> blocks;
    public final Map<Integer, BlockDetails> irregularBlocks;
    public GetBlock(Identifier eventId, OverlayId torrentId, Identifier fileId, Identifier seederId, 
      Set<Integer> blocks, Map<Integer, BlockDetails> irregularBlocks) {
      super(eventId, torrentId, fileId, seederId);
      this.blocks = blocks;
      this.irregularBlocks = irregularBlocks;
    }
  }
  
  public static class Completed extends SilkEvent.E2 implements R1FileGet.DownloadEvent {
    public final Map<Integer, byte[]> hashes;
    public final Map<Integer, byte[]> blocks;
    public Completed(OverlayId torrentId, Identifier fileId, Identifier seederId, 
      Map<Integer, byte[]> hashes, Map<Integer, byte[]> blocks) {
      super(BasicIdentifiers.eventId(), torrentId, fileId, seederId);
      this.hashes = hashes;
      this.blocks = blocks;
    }
  }
}
