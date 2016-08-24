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
package se.sics.nstream.report.event;

import se.sics.kompics.Direct;
import se.sics.ktoolbox.util.identifiable.Identifier;
import se.sics.ktoolbox.util.identifiable.basic.UUIDIdentifier;
import se.sics.nstream.StreamEvent;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class DownloadStatus {

    public static class Starting implements StreamEvent {
        public final Identifier eventId;
        public final Identifier overlayId;

        public Starting(Identifier eventId, Identifier overlayId) {
            this.eventId = eventId;
            this.overlayId = overlayId;
        }

        public Starting(Identifier overlayId) {
            this(UUIDIdentifier.randomId(), overlayId);
        }

        @Override
        public Identifier getId() {
            return eventId;
        }

        @Override
        public String toString() {
            return "Download<" + overlayId + ">Starting<" + getId() + ">";
        }
    }
    
    public static class Done implements StreamEvent {

        public final Identifier eventId;
        public final Identifier overlayId;

        public Done(Identifier eventId, Identifier overlayId) {
            this.eventId = eventId;
            this.overlayId = overlayId;
        }

        public Done(Identifier overlayId) {
            this(UUIDIdentifier.randomId(), overlayId);
        }

        @Override
        public Identifier getId() {
            return eventId;
        }

        @Override
        public String toString() {
            return "Download<" + overlayId + ">Finished<" + getId() + ">";
        }
    }
    
    public static class Request extends Direct.Request<Response> implements StreamEvent {
        public final Identifier eventId;
        public final Identifier overlayId;

        public Request(Identifier eventId, Identifier overlayId) {
            this.eventId = eventId;
            this.overlayId = overlayId;
        }

        public Request(Identifier overlayId) {
            this(UUIDIdentifier.randomId(), overlayId);
        }
        
        public Response answer(double percentageCompleted) {
            return new Response(eventId, overlayId, percentageCompleted);
        }

        @Override
        public Identifier getId() {
            return eventId;
        }

        @Override
        public String toString() {
            return "Download<" + overlayId + ">Request<" + getId() + ">";
        }
    }
    
    public static class Response implements Direct.Response, StreamEvent {
        public final Identifier eventId;
        public final Identifier overlayId;
        public final double percentageCompleted;

        Response(Identifier eventId, Identifier overlayId, double percentageCompleted) {
            this.eventId = eventId;
            this.overlayId = overlayId;
            this.percentageCompleted = percentageCompleted;
        }

        Response(Identifier overlayId, double percentageCompleted) {
            this(UUIDIdentifier.randomId(), overlayId, percentageCompleted);
        }

        @Override
        public Identifier getId() {
            return eventId;
        }

        @Override
        public String toString() {
            return "Download<" + overlayId + ">Response<" + getId() + ">";
        }
    }
}
