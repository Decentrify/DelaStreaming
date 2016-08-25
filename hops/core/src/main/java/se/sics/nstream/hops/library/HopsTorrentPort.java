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
package se.sics.nstream.hops.library;

import se.sics.kompics.PortType;
import se.sics.nstream.hops.library.event.core.HopsTorrentDownloadEvent;
import se.sics.nstream.hops.library.event.core.HopsTorrentStopEvent;
import se.sics.nstream.hops.library.event.core.HopsTorrentUploadEvent;
import se.sics.nstream.library.event.torrent.HopsContentsEvent;
import se.sics.nstream.library.event.torrent.TorrentExtendedStatusEvent;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class HopsTorrentPort extends PortType {
    {
        request(HopsTorrentDownloadEvent.StartRequest.class);
        indication(HopsTorrentDownloadEvent.Starting.class);
        indication(HopsTorrentDownloadEvent.AlreadyExists.class);
        request(HopsTorrentDownloadEvent.AdvanceRequest.class);
        indication(HopsTorrentDownloadEvent.AdvanceResponse.class);
        request(HopsTorrentUploadEvent.Request.class);
        indication(HopsTorrentUploadEvent.Uploading.class);
        indication(HopsTorrentUploadEvent.AlreadyExists.class);
        request(HopsTorrentStopEvent.Request.class);
        indication(HopsTorrentStopEvent.Response.class);
        request(HopsContentsEvent.Request.class);
        indication(HopsContentsEvent.Response.class);
        request(TorrentExtendedStatusEvent.Request.class);
        indication(TorrentExtendedStatusEvent.Response.class);
    }
}
