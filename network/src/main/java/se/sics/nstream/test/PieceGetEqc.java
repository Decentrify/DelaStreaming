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
package se.sics.nstream.test;

import se.sics.ktoolbox.util.test.EqualComparator;
import se.sics.nstream.torrent.event.PieceGet;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class PieceGetEqc {
    public static class Request implements EqualComparator<PieceGet.Request> {
        private final StringKeyMapEqc auxEqc;
        
        public Request(StringKeyMapEqc auxEqc) {
            this.auxEqc = auxEqc;
        }
        
        @Override
        public boolean isEqual(PieceGet.Request o1, PieceGet.Request o2) {
            if(!o1.eventId.equals(o2.eventId)) {
                return false;
            }
            if(!o1.overlayId.equals(o2.overlayId)) {
                return false;
            }
            if (!o1.fileName.equals(o2.fileName)) {
                return false;
            }
            if(!o1.pieceNr.equals(o2.pieceNr)) {
                return false;
            }
            if(!auxEqc.isEqual(o1.cacheHints, o2.cacheHints)) {
                return false;
            }
            return true;
        }
    }
    
    public static class RangeRequest implements EqualComparator<PieceGet.RangeRequest> {
        private final StringKeyMapEqc auxEqc;
        
        public RangeRequest(StringKeyMapEqc auxEqc) {
            this.auxEqc = auxEqc;
        }
        
        @Override
        public boolean isEqual(PieceGet.RangeRequest o1, PieceGet.RangeRequest o2) {
            if(!o1.eventId.equals(o2.eventId)) {
                return false;
            }
            if(!o1.overlayId.equals(o2.overlayId)) {
                return false;
            }
            if (!o1.fileName.equals(o2.fileName)) {
                return false;
            }
            if(o1.blockNr != o2.blockNr) {
                return false;
            }
            if(o1.from != o2.from) {
                return false;
            }
            if(o1.to != o2.to) {
                return false;
            }
            if(!auxEqc.isEqual(o1.cacheHints, o2.cacheHints)) {
                return false;
            }
            return true;
        }
    }
    
    public static class Response implements EqualComparator<PieceGet.Response> {
        @Override
        public boolean isEqual(PieceGet.Response o1, PieceGet.Response o2) {
            if(!o1.eventId.equals(o2.eventId)) {
                return false;
            }
            if(!o1.overlayId.equals(o2.overlayId)) {
                return false;
            }
            if(!o1.status.equals(o2.status)) {
                return false;
            }
            if (!o1.fileName.equals(o2.fileName)) {
                return false;
            }
            if(!o1.pieceNr.equals(o2.pieceNr)) {
                return false;
            }
            if(!o1.piece.equals(o2.piece)) {
                return false;
            }
            return true;
        }
    }
}
