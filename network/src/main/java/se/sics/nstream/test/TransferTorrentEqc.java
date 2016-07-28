package se.sics.nstream.test;

import java.util.Arrays;
import se.sics.ktoolbox.util.test.EqualComparator;
import se.sics.nstream.transfer.event.TransferTorrent;

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

/**
 *
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class TransferTorrentEqc {
    public static class Request implements EqualComparator<TransferTorrent.Request> {
        @Override
        public boolean isEqual(TransferTorrent.Request o1, TransferTorrent.Request o2) {
            if(!o1.eventId.equals(o2.eventId)) {
                return false;
            }
            return true;
        }
    }
    
    public static class Response implements EqualComparator<TransferTorrent.Response> {
        @Override
        public boolean isEqual(TransferTorrent.Response o1, TransferTorrent.Response o2) {
            if(!o1.eventId.equals(o2.eventId)) {
                return false;
            }
            if(!o1.status.equals(o2.status)) {
                return false;
            }
            if(!Arrays.equals(o1.torrent, o2.torrent)) {
                return false;
            }
            return true;
        }
    }
}
