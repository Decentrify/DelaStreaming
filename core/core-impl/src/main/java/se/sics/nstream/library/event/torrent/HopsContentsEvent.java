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

import java.util.List;
import java.util.Map;
import se.sics.gvod.stream.mngr.event.VoDMngrEvent;
import se.sics.kompics.Direct;
import se.sics.kompics.util.Identifier;
import se.sics.ktoolbox.util.identifiable.BasicIdentifiers;
import se.sics.ktoolbox.util.result.Result;
import se.sics.nstream.mngr.util.ElementSummary;

/**
 *
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class HopsContentsEvent {
    public static class Request extends Direct.Request<Response> implements VoDMngrEvent {
        public final Identifier eventId;
        //an empty list means all projects;
        public final List<Integer> projectIds;
        
        public Request(Identifier eventId, List<Integer> projectIds) {
            this.eventId = eventId;
            this.projectIds = projectIds;
        }
        
        public Request(List<Integer> projectIds) {
            this(BasicIdentifiers.eventId(), projectIds);
        }
        
        public Response success(Map<Integer, List<ElementSummary>> value) {
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
        public final Result<Map<Integer, List<ElementSummary>>> result;
        
        private Response(Request req, Result<Map<Integer, List<ElementSummary>>> result) {
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
