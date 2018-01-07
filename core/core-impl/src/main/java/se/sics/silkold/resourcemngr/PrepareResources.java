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
package se.sics.silkold.resourcemngr;

import java.util.Map;
import se.sics.kompics.Direct;
import se.sics.kompics.util.Identifiable;
import se.sics.kompics.util.Identifier;
import se.sics.ktoolbox.util.identifiable.BasicIdentifiers;
import se.sics.ktoolbox.util.identifiable.overlay.OverlayId;
import se.sics.nstream.StreamId;
import se.sics.nstream.transfer.MyTorrent;

/**
 *
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class PrepareResources {
    public static class Request extends Direct.Request<Success> implements Identifiable {
        public final Identifier eventId;
        public final OverlayId torrentId;
        public final MyTorrent torrent;
        
        public Request(OverlayId torrentId, MyTorrent torrent) {
            this.eventId = BasicIdentifiers.eventId();
            this.torrentId = torrentId;
            this.torrent = torrent;
        }

        @Override
        public Identifier getId() {
            return eventId;
        }
        
        public Success success(Map<StreamId, Long> streamsInfo) {
            return new Success(this, streamsInfo);
        }
    }
    
    public static class Success implements Direct.Response, Identifiable {
        public final Request req;
        public final Map<StreamId, Long> streamsInfo;
        
        public Success(Request req, Map<StreamId, Long> streamsInfo) {
            this.req = req;
            this.streamsInfo = streamsInfo;
        }
        
        @Override
        public Identifier getId() {
            return req.getId();
        }
        
        @Override
        public String toString() {
            return "PrepResSuccess<" + getId() + ">";
        }
    }
}
