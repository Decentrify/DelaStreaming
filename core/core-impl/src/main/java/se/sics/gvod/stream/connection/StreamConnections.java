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
package se.sics.gvod.stream.connection;

import java.util.Map;
import se.sics.gvod.common.util.VodDescriptor;
import se.sics.gvod.stream.StreamEvent;
import se.sics.kompics.Direct;
import se.sics.ktoolbox.util.identifiable.Identifier;
import se.sics.ktoolbox.util.identifiable.basic.UUIDIdentifier;
import se.sics.ktoolbox.util.network.KAddress;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class StreamConnections {
    public static class Request extends Direct.Request<Response> implements StreamEvent {

        public final Identifier eventId;
        
        public Request(Identifier eventId) {
            this.eventId = eventId;
        }
        
        public Request() {
            this(UUIDIdentifier.randomId());
        }
            
        public Response answer(Map<Identifier, KAddress> connections, Map<Identifier, VodDescriptor> descriptors) {
            return new Response(eventId, connections, descriptors);
        }
        @Override
        public Identifier getId() {
            return eventId;
        }

        @Override
        public String toString() {
            return "StreamConnections.Request<" + getId() + ">";
        }
    }

    public static class Indication implements StreamEvent {

        public final Identifier eventId;
        public final Map<Identifier, KAddress> connections;
        public final Map<Identifier, VodDescriptor> descriptors;

        public Indication(Identifier eventId, Map<Identifier, KAddress> connections, Map<Identifier, VodDescriptor> descriptors) {
            this.eventId = eventId;
            this.connections = connections;
            this.descriptors = descriptors;
        }

        public Indication(Map<Identifier, KAddress> connections, Map<Identifier, VodDescriptor> descriptors) {
            this(UUIDIdentifier.randomId(), connections, descriptors);
        }

        @Override
        public Identifier getId() {
            return eventId;
        }

        @Override
        public String toString() {
            return "StreamConnections.Indication<" + getId() + ">";
        }
    }
    
    public static class Response extends Indication implements Direct.Response {
        public Response(Identifier eventId, Map<Identifier, KAddress> connections, Map<Identifier, VodDescriptor> descriptors) {
            super(eventId, connections, descriptors);
        }
        
        @Override
        public String toString() {
            return "StreamConnections.Response<" + getId() + ">";
        }
    }
}
