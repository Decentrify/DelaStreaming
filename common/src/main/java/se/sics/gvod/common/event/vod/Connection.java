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
package se.sics.gvod.common.event.vod;

import se.sics.gvod.common.event.GVoDEvent;
import se.sics.gvod.common.event.ReqStatus;
import se.sics.gvod.common.util.VodDescriptor;
import se.sics.kompics.util.Identifier;
import se.sics.ktoolbox.util.identifiable.overlay.OverlayId;
import se.sics.ktoolbox.util.overlays.OverlayEvent;

/**
 * @author Alex Ormenisan <aaor@sics.se>
 */
public class Connection {

    public static class Request implements GVoDEvent, OverlayEvent {

        public final Identifier msgId;
        public final OverlayId overlayId;
        public final VodDescriptor desc;

        public Request(Identifier msgId, OverlayId overlayId, VodDescriptor desc) {
            this.msgId = msgId;
            this.overlayId = overlayId;
            this.desc = desc;
        }
        
        @Override
        public String toString() {
            return "Connect.Request<" + overlayId + ", " + msgId + ">";
        }

        public Response accept(VodDescriptor desc) {
            return new Response(msgId, overlayId, ReqStatus.SUCCESS, desc);
        }

        @Override
        public Identifier getId() {
            return msgId;
        }

        @Override
        public OverlayId overlayId() {
            return overlayId;
        }
    }

    public static class Response implements GVoDEvent, OverlayEvent {

        public final Identifier msgId;
        public final OverlayId overlayId;
        public final ReqStatus status;
        public final VodDescriptor desc;

        public Response(Identifier msgId, OverlayId overlayId, ReqStatus status, VodDescriptor desc) {
            this.msgId = msgId;
            this.overlayId = overlayId;
            this.status = status;
            this.desc = desc;
        }

        @Override
        public String toString() {
            return "Connect.Response<" + overlayId + ", " + msgId + "> " + status;
        }

        @Override
        public Identifier getId() {
            return msgId;
        }

        @Override
        public OverlayId overlayId() {
            return overlayId;
        }
    }

    public static class Update implements GVoDEvent, OverlayEvent {

        public final Identifier msgId;
        public final OverlayId overlayId;
        public final VodDescriptor desc;
        public final boolean downloadConnection;

        public Update(Identifier msgId, OverlayId overlayId, VodDescriptor desc, boolean downloadConnection) {
            this.msgId = msgId;
            this.overlayId = overlayId;
            this.desc = desc;
            this.downloadConnection = downloadConnection;
        }
        
        @Override
        public String toString() {
            return "Connect.Update<" + overlayId + ", " + msgId + ">";
        }

        @Override
        public Identifier getId() {
            return msgId;
        }

        @Override
        public OverlayId overlayId() {
            return overlayId;
        }
    }

    public static class Close implements GVoDEvent, OverlayEvent {

        public final Identifier eventId;
        public final OverlayId overlayId;
        public final boolean downloadConnection;

        public Close(Identifier msgId, OverlayId overlayId, boolean downloadConnection) {
            this.eventId = msgId;
            this.overlayId = overlayId;
            this.downloadConnection = downloadConnection;
        }

        @Override
        public String toString() {
            return "Connect.Close<" + overlayId + ", " + eventId + ">";
        }

        @Override
        public Identifier getId() {
            return eventId;
        }

        @Override
        public OverlayId overlayId() {
            return overlayId;
        }
    }
}
