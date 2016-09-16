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
package se.sics.nstream.torrent.conn.event;

import se.sics.kompics.Direct;
import se.sics.ktoolbox.util.identifiable.Identifiable;
import se.sics.ktoolbox.util.identifiable.Identifier;
import se.sics.ktoolbox.util.identifiable.basic.UUIDIdentifier;
import se.sics.ktoolbox.util.network.KAddress;
import se.sics.nstream.torrent.FileIdentifier;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class OpenTransferDefinition {

    public static class LeecherRequest extends Direct.Request<LeecherResponse> implements Identifiable {

        public final Identifier eventId;
        public final FileIdentifier fileId;
        public final KAddress peer;

        public LeecherRequest(KAddress peer, FileIdentifier fileId) {
            this.eventId = UUIDIdentifier.randomId();
            this.fileId = fileId;
            this.peer = peer;
        }

        @Override
        public Identifier getId() {
            return eventId;
        }

        public LeecherResponse answer(boolean result) {
            return new LeecherResponse(this, result);
        }
        
        public LeecherTimeout timeout() {
            return new LeecherTimeout(this);
        }
    }

    public static abstract class LeecherIndication implements Direct.Response, Identifiable {

        public final Identifier eventId;
        public final KAddress peer;

        public LeecherIndication(LeecherRequest req) {
            this.eventId = req.eventId;
            this.peer = req.peer;
        }

        @Override
        public Identifier getId() {
            return eventId;
        }
    }

    public static class LeecherResponse extends LeecherIndication {

        public final boolean result;

        private LeecherResponse(LeecherRequest req, boolean result) {
            super(req);
            this.result = result;
        }
    }
    
    public static class LeecherTimeout extends LeecherIndication {
        private LeecherTimeout(LeecherRequest req) {
            super(req);
        }
    }

    public static class SeederRequest extends Direct.Request<SeederResponse> implements Identifiable {

        public final Identifier eventId;
        public final KAddress peer;

        public SeederRequest(KAddress peer) {
            this.eventId = UUIDIdentifier.randomId();
            this.peer = peer;
        }

        @Override
        public Identifier getId() {
            return eventId;
        }

        public SeederResponse answer(boolean result) {
            return new SeederResponse(this, result);
        }
    }

    public static class SeederResponse implements Direct.Response, Identifiable {

        public final Identifier eventId;
        public final KAddress peer;
        public final boolean result;

        private SeederResponse(SeederRequest req, boolean result) {
            this.eventId = req.eventId;
            this.peer = req.peer;
            this.result = result;
        }

        @Override
        public Identifier getId() {
            return eventId;
        }
    }
}
