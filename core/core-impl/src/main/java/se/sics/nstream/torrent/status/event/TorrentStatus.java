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
package se.sics.nstream.torrent.status.event;

import se.sics.kompics.Direct;
import se.sics.ktoolbox.util.identifiable.BasicIdentifiers;
import se.sics.ktoolbox.util.identifiable.Identifiable;
import se.sics.ktoolbox.util.identifiable.Identifier;
import se.sics.ktoolbox.util.identifiable.overlay.OverlayId;
import se.sics.ktoolbox.util.overlays.OverlayEvent;
import se.sics.ktoolbox.util.result.Result;
import se.sics.nstream.torrent.tracking.event.TorrentTracking;
import se.sics.nstream.transfer.MyTorrent;
import se.sics.nstream.transfer.MyTorrent.Manifest;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class TorrentStatus {
    public static class DownloadedManifest extends Direct.Request<AdvanceDownload> implements OverlayEvent {
        public final Identifier eventId;
        public final OverlayId torrentId;
        public final Result<Manifest> manifest;
        
        public DownloadedManifest(TorrentTracking.DownloadedManifest req) {
            this.eventId = BasicIdentifiers.eventId();
            this.torrentId = req.torrentId;
            this.manifest = req.manifest;
        }
        
        @Override
        public Identifier getId() {
            return eventId;
        }
        
        @Override
        public OverlayId overlayId() {
            return torrentId;
        }
        
        public AdvanceDownload success(MyTorrent torrent) {
            return new AdvanceDownload(this, torrent);
        }

    }
    
    public static class AdvanceDownload implements Direct.Response, Identifiable {
        public final Identifier eventId;
        public final MyTorrent torrent;

        private AdvanceDownload(DownloadedManifest req, MyTorrent torrent) {
            this.eventId = req.eventId;
            this.torrent = torrent;
        }
        
        @Override
        public Identifier getId() {
            return eventId;
        }
    }
}
