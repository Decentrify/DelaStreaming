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
import se.sics.kompics.util.Identifier;
import se.sics.ktoolbox.util.identifiable.BasicIdentifiers;
import se.sics.ktoolbox.util.identifiable.overlay.OverlayId;
import se.sics.nstream.ConnId;
import se.sics.nstream.torrent.transfer.TorrentConnEvent;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class CompletedBlocks implements TorrentConnEvent {
    public final Identifier eventId;
    public final ConnId connId;
    public final Map<Integer, byte[]> hashes;
    public final Map<Integer, byte[]> blocks;
    
    public CompletedBlocks(ConnId connId, Map<Integer, byte[]> hashes, Map<Integer, byte[]> blocks) {
        this.eventId = BasicIdentifiers.eventId();
        this.connId = connId;
        this.hashes = hashes;
        this.blocks = blocks;
    }
    
    @Override
    public OverlayId overlayId() {
        return connId.fileId.torrentId;
    }

    @Override
    public Identifier getId() {
        return eventId;
    }

    @Override
    public ConnId connId() {
        return connId;
    }
}