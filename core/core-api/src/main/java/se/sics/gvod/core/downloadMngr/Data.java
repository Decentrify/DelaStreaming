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

package se.sics.gvod.core.downloadMngr;

import java.util.UUID;
import se.sics.gvod.common.msg.ReqStatus;
import se.sics.kompics.Request;
import se.sics.kompics.Response;

/**
 * @author Alex Ormenisan <aaor@sics.se>
 */
public class Data {
     public static class Req extends Request {

        public final UUID id;
        public final int overlayId;
        public final long readPos;
        public final int readBlockSize;

        public Req(UUID id, int overlayId, long readPos, int readBlockSize) {
            super();
            this.id = id;
            this.overlayId = overlayId;
            this.readPos = readPos;
            this.readBlockSize = readBlockSize;
        }
    }

    public static class Resp extends Response {
        
        public final UUID id;
        public final ReqStatus status;
        public final int overlayId;
        public final long readPos;
        public final byte[] block;
        
        public Resp(Req req, ReqStatus status, byte[] block) {
            super(req);
            this.id = req.id;
            this.status = status;
            this.overlayId = req.overlayId;
            this.readPos = req.readPos;
            this.block = block;
        }
    }
}
