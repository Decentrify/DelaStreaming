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
package se.sics.nstream.torrent.tracking.event;

import se.sics.kompics.Direct;
import se.sics.kompics.Direct.Response;
import se.sics.kompics.KompicsEvent;
import se.sics.ktoolbox.util.identifiable.BasicIdentifiers;
import se.sics.ktoolbox.util.identifiable.Identifier;
import se.sics.ktoolbox.util.identifiable.overlay.OverlayId;
import se.sics.ktoolbox.util.result.Result;
import se.sics.nstream.StreamEvent;
import se.sics.nstream.torrent.core.DataReport;
import se.sics.nstream.torrent.transfer.tracking.DownloadReport;
import se.sics.nstream.transfer.MyTorrent;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class TorrentTracking {

    public static class DownloadedManifest extends Direct.Request<Response> implements StreamEvent {
        public final Identifier eventId;
        public final OverlayId torrentId;
        public final Result<MyTorrent.Manifest> manifest;
        
        public DownloadedManifest(OverlayId torrentId, Result<MyTorrent.Manifest> manifest) {
            this.eventId = BasicIdentifiers.eventId();
            this.torrentId = torrentId;
            this.manifest = manifest;
        }
        
        @Override
        public Identifier getId() {
            return eventId;
        }
        
        public DownloadAdvance success(MyTorrent torrent) {
            return new DownloadAdvance(this, torrent);
        }
    }
    
    public static class DownloadAdvance implements Direct.Response,  StreamEvent {
        public final Identifier eventId;
        public final OverlayId torrentId;
        public final MyTorrent torrent;

        DownloadAdvance(DownloadedManifest req, MyTorrent torrent) {
            this.eventId = req.eventId;
            this.torrentId = req.torrentId;
            this.torrent = torrent;
        }
        
        @Override
        public Identifier getId() {
            return eventId;
        }
    }
    
     public static class DownloadStarting implements StreamEvent {
        public final Identifier eventId;
        public final OverlayId torrentId;
        public final DataReport dataReport;

        public DownloadStarting(Identifier eventId, OverlayId torrentId, DataReport dataReport) {
            this.eventId = eventId;
            this.torrentId = torrentId;
            this.dataReport = dataReport;
        }

        public DownloadStarting(OverlayId torrentId, DataReport dataReport) {
            this(BasicIdentifiers.eventId(), torrentId, dataReport);
        }

        @Override
        public Identifier getId() {
            return eventId;
        }

        @Override
        public String toString() {
            return "Download<" + torrentId + ">Starting<" + getId() + ">";
        }
    }
     
    public static class DownloadDone implements StreamEvent {

        public final Identifier eventId;
        public final OverlayId overlayId;
        public final DataReport dataReport;

        public DownloadDone(Identifier eventId, OverlayId overlayId, DataReport dataReport) {
            this.eventId = eventId;
            this.overlayId = overlayId;
            this.dataReport = dataReport;
        }

        public DownloadDone(OverlayId overlayId, DataReport dataReport) {
            this(BasicIdentifiers.eventId(), overlayId, dataReport);
        }

        @Override
        public Identifier getId() {
            return eventId;
        }

        @Override
        public String toString() {
            return "Download<" + overlayId + ">Finished<" + getId() + ">";
        }
    }
    
    public static class Indication implements KompicsEvent {
        public final DataReport dataReport;
        public final DownloadReport downloadReport;
        
        public Indication(DataReport dataReport, DownloadReport downloadReport) {
            this.dataReport = dataReport;
            this.downloadReport = downloadReport;
        }
    }
}
