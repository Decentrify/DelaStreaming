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
package se.sics.nstream.hops.library.event.core;

import se.sics.gvod.stream.mngr.event.VoDMngrEvent;
import se.sics.kompics.Direct;
import se.sics.ktoolbox.util.identifiable.BasicIdentifiers;
import se.sics.ktoolbox.util.identifiable.Identifier;
import se.sics.ktoolbox.util.identifiable.overlay.OverlayId;
import se.sics.ktoolbox.util.result.Result;
import se.sics.nstream.StreamEvent;
import se.sics.nstream.hops.hdfs.HDFSEndpoint;
import se.sics.nstream.hops.hdfs.HDFSResource;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class HopsTorrentUploadEvent {

    public static class Request extends Direct.Request<Response> implements VoDMngrEvent {

        public final Identifier eventId;
        public final OverlayId torrentId;
        public final String torrentName;
        public final HDFSEndpoint hdfsEndpoint;
        public final HDFSResource manifestResource;

        public Request(Identifier eventId, OverlayId torrentId, String torrentName, HDFSEndpoint hdfsEndpoint,  HDFSResource manifestResource) {
            this.eventId = eventId;
            this.torrentId = torrentId;
            this.torrentName = torrentName;
            this.hdfsEndpoint = hdfsEndpoint;
            this.manifestResource = manifestResource;
        }

        public Request(OverlayId torrentId, String torrentName, HDFSEndpoint hdfsEndpoint, HDFSResource hdfsResource) {
            this(BasicIdentifiers.eventId(), torrentId, torrentName, hdfsEndpoint, hdfsResource);
        }

        @Override
        public Identifier getId() {
            return eventId;
        }

        public Response failed(Result<Boolean> result) {
            return new Failed(this, result);
        }
        
        public Response success(Result<Boolean> result) {
            return new Success(this, result);
        }
    }

    public static abstract class Response implements Direct.Response, StreamEvent {

        public final Request req;
        public final Result<Boolean> result;

        public Response(Request req, Result<Boolean> result) {
            this.req = req;
            this.result = result;
        }

        @Override
        public Identifier getId() {
            return req.eventId;
        }
    }
    
    public static class Success extends Response {
        public Success(Request req, Result<Boolean> result) {
            super(req, result);
        }
    }
    
    public static class Failed extends Response {
        public Failed(Request req, Result<Boolean> result) {
            super(req, result);
        }
    }
}
