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
 * GNU General Public License for more torrentDetails.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
 */
package se.sics.nstream.transfer;

import se.sics.kompics.Direct;
import se.sics.ktoolbox.util.identifiable.BasicIdentifiers;
import se.sics.ktoolbox.util.identifiable.Identifier;
import se.sics.ktoolbox.util.identifiable.overlay.OverlayId;
import se.sics.ktoolbox.util.overlays.OverlayEvent;
import se.sics.ktoolbox.util.result.Result;
import se.sics.nstream.StreamEvent;
import se.sics.nstream.transfer.MyTorrent.Manifest;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class Transfer {
    public static class DownloadRequest extends Direct.Request<DownloadResponse> implements StreamEvent, OverlayEvent {

        public final Identifier msgId;
        public final OverlayId torrentId;
        public final Result<Manifest> manifest;

        public DownloadRequest(OverlayId torrentId, Result<Manifest> manifest) {
            this.msgId = BasicIdentifiers.msgId();
            this.torrentId = torrentId;
            this.manifest = manifest;
        }

        @Override
        public Identifier getId() {
            return msgId;
        }
        
        public DownloadResponse answer(MyTorrent torrent) {
            return new DownloadResponse(this, torrent);
        }

        @Override
        public OverlayId overlayId() {
            return torrentId;
        }
    }
    
    public static class DownloadResponse implements Direct.Response, StreamEvent, OverlayEvent {
        public final DownloadRequest req;
        public final MyTorrent torrent;
        
        public DownloadResponse(DownloadRequest req, MyTorrent torrent) {
            this.req = req;
            this.torrent = torrent;
        }

        @Override
        public Identifier getId() {
            return req.getId();
        }

        @Override
        public OverlayId overlayId() {
            return req.overlayId();
        }
    }
}
