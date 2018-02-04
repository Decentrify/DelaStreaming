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
import se.sics.ktoolbox.util.reference.KReference;
import se.sics.nstream.storage.cache.KHint;
import se.sics.nstream.util.BlockDetails;
import se.sics.silk.event.SilkEvent;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class R1UploadEvents {

  public static class BlocksReq extends SilkEvent.E2 implements R1UploadEvent {

    public final Set<Integer> blocks;
    public final KHint.Summary cacheHint;

    public BlocksReq(OverlayId torrentId, Identifier fileId, Identifier nodeId, Set<Integer> blocks,
      KHint.Summary cacheHint) {
      super(BasicIdentifiers.eventId(), torrentId, fileId, nodeId);
      this.blocks = blocks;
      this.cacheHint = cacheHint;
    }
    
    public BlocksResp accept(Map<Integer, byte[]> hashes, Map<Integer, BlockDetails> irregularBlocks, 
      Map<Integer, KReference<byte[]>> blocks) {
      return new BlocksResp(eventId, torrentId, fileId, nodeId, hashes, irregularBlocks, blocks);
    }
  }

  public static class BlocksResp extends SilkEvent.E2 implements R1UploadEvent {

    public final Map<Integer, byte[]> hashes;
    public final Map<Integer, BlockDetails> irregularBlocks;
    public final Map<Integer, KReference<byte[]>> blocks;

    public BlocksResp(Identifier eventId, OverlayId torrentId, Identifier fileId, Identifier nodeId,
      Map<Integer, byte[]> hashes, Map<Integer, BlockDetails> irregularBlocks, Map<Integer, KReference<byte[]>> blocks) {
      super(eventId, torrentId, fileId, nodeId);
      this.hashes = hashes;
      this.irregularBlocks = irregularBlocks;
      this.blocks = blocks;
    }
  }
}
