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

/**
 *
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class Seeder {
    public static class Connect extends Direct.Request<Indication> implements Identifiable {
        public final Identifier eventId;
        public final KAddress peer;

        public Connect(KAddress peer) {
            this.eventId = UUIDIdentifier.randomId();
            this.peer = peer;
        }
        
        @Override
        public Identifier getId() {
            return eventId;
        }
        
        public Success success() {
            return new Success(this);
        }
        
        public Timeout timeout() {
            return new Timeout(this);
        }
        
        public Suspect suspect() {
            return new Suspect(this);
        }
    }
    
    public static abstract class Indication implements Direct.Response, Identifiable {
        public final Identifier eventId;
        public final KAddress target;
        
        public Indication(Connect req) {
            this.eventId = req.eventId;
            this.target = req.peer;
        }

        @Override
        public Identifier getId() {
            return eventId;
        }
    }
    
    public static class Success extends Indication {
        private Success(Connect req) {
            super(req);
        }
    }
    
    public static class Timeout extends Indication {
        private Timeout(Connect req) {
            super(req);
        }
    }
    
    public static class Suspect extends Indication {
        private Suspect(Connect req) {
            super(req);
        }
    }
}
