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
package se.sics.nstream.old.torrent.event;

import se.sics.kompics.timer.SchedulePeriodicTimeout;
import se.sics.kompics.timer.ScheduleTimeout;
import se.sics.kompics.timer.Timeout;
import se.sics.ktoolbox.util.network.KAddress;
import se.sics.nstream.util.event.StreamMsg;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class TorrentTimeout {

    public static class Metadata extends Timeout implements StreamMsg.Timeout {

        private final KAddress target;

        public Metadata(ScheduleTimeout st, KAddress target) {
            super(st);
            this.target = target;
        }

        @Override
        public KAddress getTarget() {
            return target;
        }
    }
    
    public static class AdvanceDownload extends Timeout {

        public AdvanceDownload(SchedulePeriodicTimeout st) {
            super(st);
        }
    }

    public static class Hash extends Timeout implements StreamMsg.Timeout {

        public final HashGet.Request req;
        public final KAddress target;

        public Hash(ScheduleTimeout st, HashGet.Request req, KAddress target) {
            super(st);
            this.req = req;
            this.target = target;
        }

        @Override
        public KAddress getTarget() {
            return target;
        }
    }
    
    public static class Piece extends Timeout implements StreamMsg.Timeout {

        public final PieceGet.Request req;
        public final KAddress target;

        public Piece(ScheduleTimeout st, PieceGet.Request req, KAddress target) {
            super(st);
            this.req = req;
            this.target = target;
        }

        @Override
        public KAddress getTarget() {
            return target;
        }
    }
}
