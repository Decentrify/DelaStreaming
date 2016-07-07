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
package se.sics.gvod.stream.mngr.hops;

import se.sics.gvod.stream.mngr.event.ContentsSummaryEvent;
import se.sics.gvod.stream.mngr.event.TorrentExtendedStatusEvent;
import se.sics.gvod.stream.mngr.hops.torrent.event.HopsTorrentDownloadEvent;
import se.sics.gvod.stream.mngr.hops.torrent.event.HopsTorrentStopEvent;
import se.sics.gvod.stream.mngr.hops.torrent.event.HopsTorrentUploadEvent;
import se.sics.kompics.PortType;

/**
 *
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class HopsTorrentPort extends PortType {

    {
        request(HopsTorrentDownloadEvent.Request.class);
        indication(HopsTorrentDownloadEvent.Response.class);
        request(HopsTorrentUploadEvent.Request.class);
        indication(HopsTorrentUploadEvent.Response.class);
        request(HopsTorrentStopEvent.Request.class);
        indication(HopsTorrentStopEvent.Response.class);

        request(ContentsSummaryEvent.Request.class);
        indication(ContentsSummaryEvent.Response.class);

        request(TorrentExtendedStatusEvent.Request.class);
        indication(TorrentExtendedStatusEvent.Response.class);
    }
}
