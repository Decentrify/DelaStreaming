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
package se.sics.gvod.stream.congestion.event.external;

import se.sics.gvod.stream.congestion.PLedbatEvent;
import se.sics.kompics.Direct;
import se.sics.kompics.id.Identifier;
import se.sics.ktoolbox.util.identifiable.BasicIdentifiers;
import se.sics.ktoolbox.util.network.KAddress;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class PLedbatConnection {

    public static class TrackRequest extends Direct.Request<TrackResponse> implements PLedbatEvent {

        public final Identifier eventId;
        public final KAddress target;

        public TrackRequest(Identifier eventId, KAddress target) {
            this.eventId = eventId;
            this.target = target;
        }
        
        public TrackRequest(KAddress target) {
            this(BasicIdentifiers.eventId(), target);
        }

        @Override
        public Identifier getId() {
            return eventId;
        }

        @Override
        public String toString() {
            return "LedbatConnection.TrackRequest<" + getId() + ">";
        }

        public TrackResponse answer(TrackResponse.Status status) {
            return new TrackResponse(this, status);
        }
    }

    public static class TrackResponse implements Direct.Response, PLedbatEvent {

        public final TrackRequest req;
        public final Status status;

        TrackResponse(TrackRequest req, Status status) {
            this.req = req;
            this.status = status;
        }

        @Override
        public Identifier getId() {
            return req.getId();
        }

        @Override
        public String toString() {
            return "LedbatConnection.TrackResponse<" + getId() + ">";
        }

        public static enum Status {

            SPEED_UP, SLOW_DOWN, TIMEOUT;
        }
    }

    public static class Untrack implements PLedbatEvent {

        public final TrackRequest req;

        public Untrack(TrackRequest req) {
            this.req = req;
        }

        @Override
        public Identifier getId() {
            return req.getId();
        }

        @Override
        public String toString() {
            return "LedbatConnection.Untrack<" + getId() + ">";
        }
    }
}
