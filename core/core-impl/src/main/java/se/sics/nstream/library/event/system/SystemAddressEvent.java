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
package se.sics.nstream.library.event.system;

import se.sics.gvod.stream.mngr.event.VoDMngrEvent;
import se.sics.kompics.Direct;
import se.sics.kompics.util.Identifier;
import se.sics.ktoolbox.util.identifiable.BasicIdentifiers;
import se.sics.ktoolbox.util.network.KAddress;
import se.sics.ktoolbox.util.result.Result;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class SystemAddressEvent {
    public static class Request extends Direct.Request<Response> implements VoDMngrEvent {
        public final Identifier eventId;
        
        public Request(Identifier eventId) {
            this.eventId = eventId;
        }
        
        public Request() {
            this(BasicIdentifiers.eventId());
        }
        
        public Response success(KAddress systemAdr) {
            return new Response(this, Result.success(systemAdr));
        }
        
        @Override
        public Identifier getId() {
            return eventId;
        }
    }
    
    public static class Response implements Direct.Response, VoDMngrEvent {
        public final Request req;
        public final Result<KAddress> systemAdr;
        
        private Response(Request req, Result<KAddress> systemAdr) {
            this.req = req;
            this.systemAdr = systemAdr;
        }
        
        @Override
        public Identifier getId() {
            return req.getId();
        }
    }
}
