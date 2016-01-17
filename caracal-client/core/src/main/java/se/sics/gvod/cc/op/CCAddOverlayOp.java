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
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import se.sics.caracaldb.Key;
import se.sics.caracaldb.operations.GetRequest;
import se.sics.caracaldb.operations.GetResponse;
import se.sics.caracaldb.operations.PutRequest;
import se.sics.caracaldb.operations.PutResponse;
import se.sics.caracaldb.operations.ResponseCode;
import se.sics.gvod.cc.event.CCAddOverlay;
import se.sics.gvod.cc.opMngr.Operation;
import se.sics.gvod.cc.util.CaracalKeyFactory;
import se.sics.gvod.common.util.FileMetadata;
import se.sics.kompics.KompicsEvent;
import se.sics.kompics.network.netty.serialization.Serializers;
import se.sics.ktoolbox.cc.operation.event.CCOpRequest;
import se.sics.ktoolbox.cc.operation.event.CCOpResponse;
import se.sics.ktoolbox.util.identifiable.Identifier;
import se.sics.ktoolbox.util.identifiable.basic.UUIDIdentifier;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class CCAddOverlayOp implements Operation {

    public static enum Phase {

        GET, ADD
    }
    private final Identifier opId;
    private final CCAddOverlay.Request req;
    private final byte[] schemaId;

    private Phase phase;
    private CCAddOverlay.Response result;
    private CCOpRequest pendingMsg;
    private UUID pendingMsgId;

    public CCAddOverlayOp(byte[] schemaId, CCAddOverlay.Request req) {
        this.opId = UUIDIdentifier.randomId();
        this.schemaId = schemaId;
        this.req = req;
        this.result = null;
    }

    @Override
    public void start() {
        phase = Phase.GET;
        Key target = CaracalKeyFactory.getFileMetadataKey(schemaId, req.overlayId);
        pendingMsgId = UUID.randomUUID();
        pendingMsg = new CCOpRequest(new GetRequest(pendingMsgId, target), target);
    }

    @Override
    public HandleStatus handleEvent(CCOpResponse event) {
        if (phase.equals(Phase.GET) && event.opResp instanceof GetResponse) {
            GetResponse resp = (GetResponse) event.opResp;
            if (!resp.id.equals(pendingMsgId)) {
                return Operation.HandleStatus.NOT_HANDLED;
            }
            if (resp.code.equals(ResponseCode.SUCCESS)) {
                if (resp.data != null) {
                    FileMetadata fileMetadata = (FileMetadata) Serializers.lookupSerializer(FileMetadata.class).fromBinary(Unpooled.wrappedBuffer(resp.data), Optional.absent());
                    if (req.fileMeta.equals(fileMetadata)) {
                        result = req.success();
                    } else {
                        result = req.fail();
                    }
                } else {
                    phase = Phase.ADD;
                    pendingMsgId = UUID.randomUUID();
                    ByteBuf buf = Unpooled.buffer();
                    Serializers.lookupSerializer(FileMetadata.class).toBinary(req.fileMeta, buf);
                    //TODO Alex optimization - do I need to cut short the backing array?
                    pendingMsg = new CCOpRequest(new PutRequest(pendingMsgId, resp.key, buf.array()), resp.key);
                }
            } else {
                result = req.timeout();
            }
            return Operation.HandleStatus.HANDLED;
        } else if (phase.equals(Phase.ADD) && event.opResp instanceof PutResponse) {
            PutResponse resp = (PutResponse) event.opResp;
            if (!resp.id.equals(pendingMsgId)) {
                return Operation.HandleStatus.NOT_HANDLED;
            }
            if (resp.code.equals(ResponseCode.SUCCESS)) {
                result = req.success();
            } else {
                result = req.timeout();
            }
            return Operation.HandleStatus.HANDLED;
        }
        return Operation.HandleStatus.NOT_HANDLED;
    }
    
    @Override
    public void timeout(Identifier reqId) {
        result = req.timeout();
    }

    @Override
    public Map<CCOpRequest, Boolean> sendingQueue() {
        Map<CCOpRequest, Boolean> toSend = new HashMap<CCOpRequest, Boolean>();
        if (pendingMsg != null) {
            toSend.put(pendingMsg, true);
            pendingMsg = null;
        }
        return toSend;
    }

    @Override
    public Identifier getId() {
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