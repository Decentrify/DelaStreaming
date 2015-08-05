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
package se.sics.gvod.cc.op;

import com.google.common.base.Optional;
import io.netty.buffer.Unpooled;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import se.sics.caracaldb.Key;
import se.sics.caracaldb.operations.GetRequest;
import se.sics.caracaldb.operations.GetResponse;
import se.sics.gvod.cc.msg.CCJoinOverlay;
import se.sics.gvod.cc.opMngr.Operation;
import se.sics.gvod.cc.util.CaracalKeyFactory;
import se.sics.gvod.common.util.FileMetadata;
import se.sics.kompics.KompicsEvent;
import se.sics.kompics.network.netty.serialization.Serializers;
import se.sics.ktoolbox.cc.common.op.CCOpEvent;

/**
 * @author Alex Ormenisan <aaor@sics.se>
 */
public class CCJoinOverlayOp implements Operation {

    private final UUID opId;
    private final CCJoinOverlay.Request req;
    private final byte[] schemaId;

    private CCJoinOverlay.Response result;
    private CCOpEvent.Request pendingMsg;
    private UUID pendingMsgId;

    public CCJoinOverlayOp(byte[] schemaId, CCJoinOverlay.Request req) {
        this.opId = UUID.randomUUID();
        this.schemaId = schemaId;
        this.req = req;
        this.result = null;
    }

    @Override
    public void start() {
        Key target = CaracalKeyFactory.getFileMetadataKey(schemaId, req.overlayId);
        pendingMsgId = UUID.randomUUID();
        pendingMsg = new CCOpEvent.Request(new GetRequest(pendingMsgId, target), target);
    }

    @Override
    public HandleStatus handleEvent(CCOpEvent.Response event) {
        if (event.opResp instanceof GetResponse) {
            GetResponse resp = (GetResponse) event.opResp;
            if (resp.id.equals(pendingMsgId)) {
                if (resp.data == null) {
                    result = req.missing();
                } else {
                    FileMetadata fileMetadata = (FileMetadata) Serializers.lookupSerializer(FileMetadata.class).fromBinary(Unpooled.wrappedBuffer(resp.data), Optional.absent());
                    if (fileMetadata == null) {
                        result = req.missing();
                    } else {
                        result = req.success(fileMetadata);
                    }
                }
                return Operation.HandleStatus.HANDLED;
            }
        }
        return Operation.HandleStatus.NOT_HANDLED;
    }

    @Override
    public void timeout(UUID msgId) {
        result = req.timeout();
    }
    
    @Override
    public Map<CCOpEvent.Request, Boolean> sendingQueue() {
        Map<CCOpEvent.Request, Boolean> toSend = new HashMap<CCOpEvent.Request, Boolean>();
        if (pendingMsg != null) {
            toSend.put(pendingMsg, true);
            pendingMsg = null;
        }
        return toSend;
    }

    @Override
    public UUID getId() {
        return opId;
    }

    @Override
    public OpStatus getStatus() {
        if (result == null) {
            return OpStatus.ONGOING;
        } else {
            return OpStatus.DONE;
        }
    }

    @Override
    public KompicsEvent getResult() {
        return result;
    }
}
