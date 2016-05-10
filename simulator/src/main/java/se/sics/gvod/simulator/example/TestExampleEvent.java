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
package se.sics.gvod.simulator.example;

import se.sics.kompics.Direct;
import se.sics.kompics.KompicsEvent;
import se.sics.ktoolbox.util.identifiable.Identifiable;
import se.sics.ktoolbox.util.identifiable.Identifier;
import se.sics.ktoolbox.util.identifiable.basic.UUIDIdentifier;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class TestExampleEvent {
    public static class Request extends Direct.Request<Response> implements KompicsEvent, Identifiable {
        public final Identifier eventId;
        
        public Request(Identifier eventId) {
            this.eventId = eventId;
        }
        
        public Request() {
            this(UUIDIdentifier.randomId());
        }
        
        public Response answer() {
            return new Response(eventId);
        }

        @Override
        public Identifier getId() {
            return eventId;
        }
    }
    
    public static class Indication implements KompicsEvent, Identifiable {
        public final Identifier eventId;
        
        public Indication(Identifier eventId) {
            this.eventId = eventId;
        }
        
        public Indication() {
            this(UUIDIdentifier.randomId());
        }

        @Override
        public Identifier getId() {
            return eventId;
        }
    }
    
    public static class Response implements Direct.Response, KompicsEvent, Identifiable {
        public final Identifier eventId;
        
        public Response(Identifier eventId) {
            this.eventId = eventId;
        }
        
        public Response() {
            this(UUIDIdentifier.randomId());
        }

        @Override
        public Identifier getId() {
            return eventId;
        }
    }
}
