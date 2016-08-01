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
import se.sics.nstream.torrent.event.HashGet;

/**
 *
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class HashGetEqc {

    public static class Request implements EqualComparator<HashGet.Request> {

        private final StringKeyMapEqc auxEqc;

        public Request(StringKeyMapEqc auxEqc) {
            this.auxEqc = auxEqc;
        }

        @Override
        public boolean isEqual(HashGet.Request o1, HashGet.Request o2) {
            if (!o1.eventId.equals(o2.eventId)) {
                return false;
            }
            if (!o1.overlayId.equals(o2.overlayId)) {
                return false;
            }
            if (!o1.fileName.equals(o2.fileName)) {
                return false;
            }
            if (o1.targetPos != o2.targetPos) {
                return false;
            }
            if (!o1.hashes.equals(o2.hashes)) {
                return false;
            }
            if(!auxEqc.isEqual(o1.cacheHints, o2.cacheHints)) {
                return false;
            }
            return true;
        }
    }

    public static class Response implements EqualComparator<HashGet.Response> {

        @Override
        public boolean isEqual(HashGet.Response o1, HashGet.Response o2) {
            if (!o1.eventId.equals(o2.eventId)) {
                return false;
            }
            if (!o1.overlayId.equals(o2.overlayId)) {
                return false;
            }
            if (!o1.status.equals(o2.status)) {
                return false;
            }
            if (o1.targetPos != o2.targetPos) {
                return false;
            }
            if (!o1.hashes.equals(o2.hashes)) {
                return false;
            }
            if (!o1.missingHashes.equals(o2.missingHashes)) {
                return false;
            }
            return true;
        }
    }
}
