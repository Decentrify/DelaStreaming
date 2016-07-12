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
package se.sics.gvod.stream.mngr.hops.torrent.event;

import se.sics.gvod.mngr.util.Result;
import se.sics.gvod.stream.mngr.event.VoDMngrEvent;
import se.sics.kompics.Direct;
import se.sics.ktoolbox.hdfs.HDFSResource;
import se.sics.ktoolbox.hdfs.HopsResource;
import se.sics.ktoolbox.util.identifiable.Identifier;
import se.sics.ktoolbox.util.identifiable.basic.UUIDIdentifier;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class HopsTorrentUploadEvent {

    public static class Request extends Direct.Request<Response> implements VoDMngrEvent {

        public final Identifier eventId;

        public final HDFSResource hdfsResource;
        public final HopsResource hopsResource;
        public final Identifier torrentId;

        public Request(Identifier eventId, HDFSResource hdfsResource, HopsResource hopsResource, Identifier torrentId) {
            this.eventId = eventId;
            this.hdfsResource = hdfsResource;
            this.hopsResource = hopsResource;
            this.torrentId = torrentId;
        }

        public Request(HDFSResource hdfsResource, HopsResource hopsResource, Identifier torrentId) {
            this(UUIDIdentifier.randomId(), hdfsResource, hopsResource, torrentId);
        }

        @Override
        public Identifier getId() {
            return eventId;
        }
        
        public Response success() {
            return new Response(this, Result.success());
        }
        
        public Response badRequest(String message) {
            return new Response(this, Result.badRequest(message));
        }
        
        public Response fail(String message) {
            return new Response(this, Result.fail(message));
        }
    }

    public static class Response implements Direct.Response, VoDMngrEvent {
        public final Request req;
        public final Result result;
        
        public Response(Request req, Result result) {
            this.req = req;
            this.result = result;
        }

        @Override
        public Identifier getId() {
            return req.eventId;
        }
    }
}
