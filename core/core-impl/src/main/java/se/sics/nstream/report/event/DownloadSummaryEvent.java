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
package se.sics.nstream.report.event;

import se.sics.nstream.StreamEvent;
import se.sics.ktoolbox.util.identifiable.Identifier;
import se.sics.ktoolbox.util.identifiable.basic.UUIDIdentifier;
import se.sics.ktoolbox.util.overlays.OverlayEvent;

/**
 *
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class DownloadSummaryEvent implements StreamEvent, OverlayEvent {
    public final Identifier eventId;
    public final Identifier torrentId;
    public final long transferSize;
    public final long transferTime;

    public DownloadSummaryEvent(Identifier eventId, Identifier torrentId, long transferSize, long transferTime) {
        this.eventId = eventId;
        this.transferSize = transferSize;
        this.transferTime = transferTime;
        this.torrentId = torrentId;
    }
    
    public DownloadSummaryEvent(Identifier torrentId, long transferSize, long transferTime) {
        this(UUIDIdentifier.randomId(), torrentId, transferSize, transferTime);
    }
    
    @Override
    public Identifier getId() {
        return eventId;
    }
    
    @Override
    public Identifier overlayId() {
        return torrentId;
    }
    
    @Override
    public String toString() {
        return "Download<" + torrentId + ">SummaryEvent<" + getId() + ">";
    }
}
