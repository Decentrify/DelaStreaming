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

import java.util.Map;
import org.javatuples.Pair;
import se.sics.gvod.stream.mngr.event.VoDMngrEvent;
import se.sics.kompics.Direct;
import se.sics.ktoolbox.util.identifiable.Identifier;
import se.sics.ktoolbox.util.identifiable.basic.UUIDIdentifier;
import se.sics.ktoolbox.util.result.Result;
import se.sics.nstream.StreamEvent;
import se.sics.nstream.hops.hdfs.HDFSEndpoint;
import se.sics.nstream.hops.hdfs.HDFSResource;
import se.sics.nstream.util.FileExtendedDetails;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class HopsTorrentUploadEvent {

    public static class Request extends Direct.Request<Response> implements VoDMngrEvent {

        public final Identifier eventId;
        public final Identifier torrentId;
        public final Pair<HDFSEndpoint, HDFSResource> manifest;
        public final Map<String, FileExtendedDetails> extendedDetails;

        public Request(Identifier eventId, Identifier torrentId, Pair<HDFSEndpoint, HDFSResource> manifest, Map<String, FileExtendedDetails> extendedDetails) {
            this.eventId = eventId;
            this.torrentId = torrentId;
            this.manifest = manifest;
            this.extendedDetails = extendedDetails;
        }

        public Request(Identifier torrentId, Pair<HDFSEndpoint, HDFSResource> manifest, Map<String, FileExtendedDetails> extendedDetails) {
            this(UUIDIdentifier.randomId(), torrentId, manifest, extendedDetails);
        }

        @Override
        public Identifier getId() {
            return eventId;
        }

        public Response alreadyExists(Result<Boolean> result) {
            return new AlreadyExists(this, result);
        }
        
        public Response uploading(Result<Boolean> result) {
            return new Uploading(this, result);
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
    
    public static class Uploading extends Response {
        public Uploading(Request req, Result<Boolean> result) {
            super(req, result);
        }
    }
    
    public static class AlreadyExists extends Response {
        public AlreadyExists(Request req, Result<Boolean> result) {
            super(req, result);
        }
    }
}
