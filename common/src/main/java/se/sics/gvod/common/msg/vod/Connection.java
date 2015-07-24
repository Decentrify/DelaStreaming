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
package se.sics.gvod.common.msg.vod;

import java.util.UUID;
import se.sics.gvod.common.msg.GvodMsg;
import se.sics.gvod.common.msg.ReqStatus;
import se.sics.gvod.common.util.VodDescriptor;

/**
 * @author Alex Ormenisan <aaor@sics.se>
 */
public class Connection {

    public static class Request extends GvodMsg.Request {

        public final VodDescriptor desc;

        public Request(UUID id, VodDescriptor desc) {
            super(id);
            this.desc = desc;
        }

        @Override
        public Request copy() {
            return new Request(id, desc);
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

    }

    public static class Response extends GvodMsg.Response {
        public final VodDescriptor desc;
        
        public Response(UUID id, ReqStatus status, VodDescriptor desc) {
            super(id, status);
            this.desc = desc;
        }

        @Override
        public Response copy() {
            return new Response(id, status, desc);
        }

        @Override
        public String toString() {
            return "Connect.Response<" + id + "> " + status;
        }

    }

    public static class Update extends GvodMsg.OneWay {
        public final VodDescriptor desc;
        public final boolean downloadConnection;

        public Update(UUID id, VodDescriptor desc, boolean downloadConnection) {
            super(id);
            this.desc = desc;
            this.downloadConnection = downloadConnection;
        }

        @Override
        public Update copy() {
            return new Update(id, desc, downloadConnection);
        }

        @Override
        public String toString() {
            return "Connect.Update<" + id + ">";
        }
        
    }

    public static class Close extends GvodMsg.OneWay {
        public boolean downloadConnection;
        
        public Close(UUID id, boolean downloadConnection) {
            super(id);
            this.downloadConnection = downloadConnection;
        }

        @Override
        public Close copy() {
            return new Close(id, downloadConnection);
        }

        @Override
        public String toString() {
            return "Connect.Close<" + id + ">";
        }
    }
}
