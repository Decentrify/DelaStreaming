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
package se.sics.gvod.cc.event;

import se.sics.gvod.common.event.GVoDEvent;
import se.sics.gvod.common.event.ReqStatus;
import se.sics.gvod.common.util.FileMetadata;
import se.sics.ktoolbox.util.identifiable.Identifier;
import se.sics.ktoolbox.util.identifiable.basic.UUIDIdentifier;

/**
 * @author Alex Ormenisan <aaor@sics.se>
 */
public class CCJoinOverlay {

    public static class Request implements GVoDEvent {

        public final Identifier id;
        public final Identifier overlayId;

        public Request(Identifier id, Identifier overlayId) {
            this.id = id;
            this.overlayId = overlayId;
        }
        
        public Request(Identifier overlayId) {
            this(UUIDIdentifier.randomId(), overlayId);
        }

        public Response success(FileMetadata fileMeta) {
            return new Response(id, ReqStatus.SUCCESS, overlayId, fileMeta);
        }

        public Response missing() {
            return new Response(id, ReqStatus.MISSING, overlayId, null);
        }

        public Response fail() {
            return new Response(id, ReqStatus.FAIL, overlayId, null);
        }

        public Response timeout() {
            return new Response(id, ReqStatus.TIMEOUT, overlayId, null);
        }

        @Override
        public Identifier getId() {
            return id;
        }

        @Override
        public String toString() {
            return "JoinOverlayRequest " + id.toString();
        }
    }

    public static class Response implements GVoDEvent {

        public final Identifier id;
        public final Identifier overlayId;
        public final ReqStatus status;
        public final FileMetadata fileMeta;

        public Response(Identifier id, ReqStatus status, Identifier overlayId, FileMetadata fileMeta) {
            this.id = id;
            this.status = status;
            this.overlayId = overlayId;
            this.fileMeta = fileMeta;
        }

        @Override
        public Identifier getId() {
            return id;
        }

        @Override
        public String toString() {
            return "JoinOverlayMsgResponse<" + status.toString() + "> " + id.toString();
        }
    }
}
