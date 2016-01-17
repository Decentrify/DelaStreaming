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
import se.sics.ktoolbox.util.identifiable.Identifier;

/**
 * @author Alex Ormenisan <aaor@sics.se>
 */
public class Connection {

    public static class Request implements GVoDEvent {

        public final Identifier id;
        public final VodDescriptor desc;

        public Request(Identifier id, VodDescriptor desc) {
            this.id = id;
            this.desc = desc;
        }

        @Override
        public String toString() {
            return "Connect.Request<" + id + ">";
        }

//        public Response fail() {
//            return new Response(id, ReqStatus.FAIL);
//        }

        public Response accept(VodDescriptor desc) {
            return new Response(id, ReqStatus.SUCCESS, desc);
        }

        @Override
        public Identifier getId() {
            return id;
        }
    }

    public static class Response implements GVoDEvent {
        public final Identifier id;
        public final ReqStatus status;
        public final VodDescriptor desc;
        
        public Response(Identifier id, ReqStatus status, VodDescriptor desc) {
            this.id = id;
            this.status = status;
            this.desc = desc;
        }

        @Override
        public String toString() {
            return "Connect.Response<" + id + "> " + status;
        }

        @Override
        public Identifier getId() {
            return id;
        }
    }

    public static class Update implements GVoDEvent {
        public final Identifier id;
        public final VodDescriptor desc;
        public final boolean downloadConnection;

        public Update(Identifier id, VodDescriptor desc, boolean downloadConnection) {
            this.id = id;
            this.desc = desc;
            this.downloadConnection = downloadConnection;
        }

        @Override
        public String toString() {
            return "Connect.Update<" + id + ">";
        }

        @Override
        public Identifier getId() {
            return id;
        }
    }

    public static class Close implements GVoDEvent {
        public final Identifier id;
        public final boolean downloadConnection;
        
        public Close(Identifier id, boolean downloadConnection) {
            this.id = id;
            this.downloadConnection = downloadConnection;
        }

        @Override
        public String toString() {
            return "Connect.Close<" + id + ">";
        }

        @Override
        public Identifier getId() {
            return id;
        }
    }
}
