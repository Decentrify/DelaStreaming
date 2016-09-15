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
package se.sics.nstream.torrent.conn.msg;

import se.sics.ktoolbox.util.identifiable.Identifier;
import se.sics.ktoolbox.util.identifiable.basic.UUIDIdentifier;
import se.sics.ktoolbox.util.overlays.OverlayEvent;
import se.sics.nstream.torrent.FileIdentifier;

/**
 *
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class NetCloseTransfer implements OverlayEvent {

    public final Identifier eventId;
    public final FileIdentifier fileId;
    public final boolean leecher;

    protected NetCloseTransfer(Identifier eventId, FileIdentifier fileId, boolean leecher) {
        this.eventId = eventId;
        this.fileId = fileId;
        this.leecher = leecher;
    }
    
    public NetCloseTransfer(FileIdentifier fileId, boolean leecher) {
        this(UUIDIdentifier.randomId(), fileId, leecher);
    }

    @Override
    public Identifier getId() {
        return eventId;
    }

    @Override
    public Identifier overlayId() {
        return fileId.overlayId;
    }
}
