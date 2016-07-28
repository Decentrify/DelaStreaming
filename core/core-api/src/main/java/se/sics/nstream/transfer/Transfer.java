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
import se.sics.ktoolbox.util.identifiable.Identifier;
import se.sics.ktoolbox.util.identifiable.basic.UUIDIdentifier;
import se.sics.ktoolbox.util.overlays.OverlayEvent;
import se.sics.ktoolbox.util.result.Result;
import se.sics.nstream.StreamEvent;
import se.sics.nstream.util.TransferDetails;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class Transfer {
    public static class UploadIndication implements StreamEvent, OverlayEvent {
        public final Identifier eventId;
        public final Identifier torrentId;
        public final Result<Boolean> result;

        public UploadIndication(Identifier torrentId, Result<Boolean> result) {
            this.eventId = UUIDIdentifier.randomId();
            this.torrentId = torrentId;
            this.result = result;
        }

        @Override
        public Identifier getId() {
            return eventId;
        }
        
        @Override
        public Identifier overlayId() {
            return torrentId;
        }
    }

    public static class DownloadRequest extends Direct.Request<DownloadResponse> implements StreamEvent, OverlayEvent {

        public final Identifier eventId;
        public final Identifier torrentId;
        public final Result<byte[]> torrentByte;

        public DownloadRequest(Identifier torrentId, Result<byte[]> torrentByte) {
            this.eventId = UUIDIdentifier.randomId();
            this.torrentId = torrentId;
            this.torrentByte = torrentByte;
        }

        @Override
        public Identifier getId() {
            return eventId;
        }
        
        public DownloadResponse answer(TransferDetails transferDetails) {
            return new DownloadResponse(this, transferDetails);
        }

        @Override
        public Identifier overlayId() {
            return torrentId;
        }
    }
    
    public static class DownloadResponse implements Direct.Response, StreamEvent {
        public final DownloadRequest req;
        public final TransferDetails transferDetails;
        
        public DownloadResponse(DownloadRequest req, TransferDetails transferDetails) {
            this.req = req;
            this.transferDetails = transferDetails;
        }

        @Override
        public Identifier getId() {
            return req.getId();
        }
    }
}
