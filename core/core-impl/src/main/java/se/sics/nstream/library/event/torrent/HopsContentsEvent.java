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
package se.sics.nstream.library.event.torrent;

import com.google.common.base.Optional;
import java.util.List;
import se.sics.gvod.mngr.util.ElementSummary;
import se.sics.gvod.stream.mngr.event.VoDMngrEvent;
import se.sics.kompics.Direct;
import se.sics.ktoolbox.util.identifiable.Identifier;
import se.sics.ktoolbox.util.identifiable.basic.UUIDIdentifier;
import se.sics.ktoolbox.util.result.Result;

/**
 *
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class HopsContentsEvent {
    public static class Request extends Direct.Request<Response> implements VoDMngrEvent {
        public final Identifier eventId;
        public final Optional<Integer> projectId;
        
        public Request(Identifier eventId, Optional<Integer> projectId) {
            this.eventId = eventId;
            this.projectId = projectId;
        }
        
        public Request(Optional<Integer> projectId) {
            this(UUIDIdentifier.randomId(), projectId);
        }
        
        public Response success(List<ElementSummary> value) {
            return new Response(this, Result.success(value));
        }
        
        @Override
        public Identifier getId() {
            return eventId;
        }
        
        @Override
        public String toString() {
            return "ContentsSummaryRequest<" + getId() + ">";
        }
    }
    
    public static class Response implements Direct.Response, VoDMngrEvent {
        public final Request req;
        public final Result<List<ElementSummary>> result;
        
        private Response(Request req, Result<List<ElementSummary>> result) {
            this.req = req;
            this.result = result;
        }
        
        @Override
        public Identifier getId() {
            return req.getId();
        }
        
         @Override
        public String toString() {
            return "ContentsSummaryResponse<" + getId() + ">";
        }
    }
}
