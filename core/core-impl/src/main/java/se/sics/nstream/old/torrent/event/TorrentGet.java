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
package se.sics.nstream.old.torrent.event;

import se.sics.ktoolbox.util.identifiable.BasicIdentifiers;
import se.sics.ktoolbox.util.identifiable.Identifier;
import se.sics.ktoolbox.util.identifiable.overlay.OverlayId;
import se.sics.ktoolbox.util.result.Result;
import se.sics.nstream.util.event.StreamMsg;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class TorrentGet {

    public static class Request implements StreamMsg.Request {

        public final Identifier msgId;
        public final OverlayId overlayId;

        public Request(Identifier msgId, OverlayId overlayId) {
            this.msgId = msgId;
            this.overlayId = overlayId;
        }

        public Request(OverlayId overlayId) {
            this(BasicIdentifiers.msgId(), overlayId);
        }

        @Override
        public Identifier getId() {
            return msgId;
        }
        
        @Override
        public OverlayId overlayId() {
            return overlayId;
        }

        public Response success(byte[] torrent) {
            return new Response(this, Result.Status.SUCCESS, torrent);
        }
        
        public Response busy() {
            return new Response(this, Result.Status.BUSY, null);
        }
    }

    public static class Response implements StreamMsg.Response {

        public final Identifier msgId;
        public final OverlayId overlayId;
        public final Result.Status status;
        public final byte[] torrent;
        
        Response(Identifier msgId, OverlayId overlayId, Result.Status status, byte[] torrent) {
            this.msgId = msgId;
            this.overlayId = overlayId;
            this.status = status;
            this.torrent = torrent;
        }
        public Response(Request req, Result.Status status, byte[] torrent) {
            this(req.msgId, req.overlayId, status, torrent);
        }
        
        @Override
        public Identifier getId() {
            return msgId;
        }
        
         @Override
        public OverlayId overlayId() {
            return overlayId;
        }

        @Override
        public Result.Status getStatus() {
            return status;
        }
    }
}
