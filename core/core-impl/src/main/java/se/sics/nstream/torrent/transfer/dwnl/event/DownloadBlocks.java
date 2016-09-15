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
package se.sics.nstream.torrent.transfer.dwnl.event;

import java.util.Map;
import java.util.Set;
import se.sics.ktoolbox.util.identifiable.Identifier;
import se.sics.ktoolbox.util.identifiable.basic.UUIDIdentifier;
import se.sics.nstream.torrent.transfer.TorrentConnEvent;
import se.sics.nstream.torrent.util.TorrentConnId;
import se.sics.nstream.util.BlockDetails;

/**
 *
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class DownloadBlocks implements TorrentConnEvent {
    public final Identifier eventId;
    public final TorrentConnId connId;
    public final Set<Integer> blocks;
    public final Map<Integer, BlockDetails> irregularBlocks;
    
    public DownloadBlocks(TorrentConnId connId, Set<Integer> blocks, Map<Integer, BlockDetails> irregularBlocks) {
        this.eventId = UUIDIdentifier.randomId();
        this.connId = connId;
        this.blocks = blocks;
        this.irregularBlocks = irregularBlocks;
    }
    
    @Override
    public Identifier overlayId() {
        return connId.fileId.overlayId;
    }

    @Override
    public Identifier getId() {
        return eventId;
    }

    @Override
    public Identifier connId() {
        return connId;
    }
}
