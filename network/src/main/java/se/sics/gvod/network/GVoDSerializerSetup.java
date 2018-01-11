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

package se.sics.gvod.network;

import se.sics.kompics.network.netty.serialization.Serializers;
import se.sics.ktoolbox.util.setup.BasicSerializerSetup;
import se.sics.nstream.FileId;
import se.sics.nstream.storage.cache.KHint;
import se.sics.nstream.storage.cache.KHintSummarySerializer;
import se.sics.nstream.torrent.FileIdentifierSerializer;
import se.sics.nstream.torrent.conn.msg.NetCloseTransfer;
import se.sics.nstream.torrent.conn.msg.NetCloseTransferSerializer;
import se.sics.nstream.torrent.conn.msg.NetConnect;
import se.sics.nstream.torrent.conn.msg.NetConnectSerializer;
import se.sics.nstream.torrent.conn.msg.NetDetailedState;
import se.sics.nstream.torrent.conn.msg.NetDetailedStateSerializer;
import se.sics.nstream.torrent.conn.msg.NetOpenTransfer;
import se.sics.nstream.torrent.conn.msg.NetOpenTransferSerializer;
import se.sics.nstream.torrent.transfer.msg.CacheHint;
import se.sics.nstream.torrent.transfer.msg.CacheHintSerializer;
import se.sics.nstream.torrent.transfer.msg.DownloadHash;
import se.sics.nstream.torrent.transfer.msg.DownloadHashSerializer;
import se.sics.nstream.torrent.transfer.msg.DownloadPiece;
import se.sics.nstream.torrent.transfer.msg.DownloadPieceSerializer;
import se.sics.nstream.util.BlockDetails;
import se.sics.nstream.util.BlockDetailsSerializer;
import se.sics.silk.r2torrent.conn.msg.R2NodeConnMsgs;
import se.sics.silk.r2torrent.conn.msg.ConnMsgsSerializers;

/**
 * @author Alex Ormenisan <aaor@sics.se>
 */
public class GVoDSerializerSetup {
    //You may add up to max serializers without the need to recompile all the projects that use the serializer space after gvod
    public static int maxSerializers = 25;
    public static int serializerIds = 25;
    
    public static enum GVoDSerializers {
        FileIdentifier(FileId.class, "nStreamFileIdentifier"),
        NetConnectRequest(NetConnect.Request.class, "nStreamNetConnRequest"),
        NetConnectResponse(NetConnect.Response.class, "nStreamNetConnResponse"),
        BlockDetails(BlockDetails.class, "nstreamBlockDetails"),
        NetDetailedStateRequest(NetDetailedState.Request.class, "nStreamNetDetailedStateRequest"),
        NetDetailedStateResponse(NetDetailedState.Response.class, "nStreamNetDetailedStateResponse"),
        NetOpenTransferRequest(NetOpenTransfer.Request.class, "nStreamNetOpenTransferRequest"),
        NetOpenTransferResponse(NetOpenTransfer.Response.class, "nStreamNetOpenTransferResponse"),
        NetCloseTransfer(NetCloseTransfer.class, "nStreamNetCloseTransfer"),
        KHintSummary(KHint.Summary.class, "nStreamKHintSummary"),
        CacheHintRequest(CacheHint.Request.class, "nstreamCacheHintRequest"),
        CacheHintResponse(CacheHint.Response.class, "nstreamCacheHintResponse"),
        DownloadPieceRequest(DownloadPiece.Request.class, "nstreamDownloadPieceRequest"),
        DownloadPieceSuccess(DownloadPiece.Success.class, "nstreamDownloadPieceSuccess"),
        DownloadPieceBadReq(DownloadPiece.BadRequest.class, "nstreamDownloadPieceBadReq"),
        DownloadHashRequest(DownloadHash.Request.class, "nstreamDownloadHashRequest"),
        DownloadHashSuccess(DownloadHash.Success.class, "nstreamDownloadHashSuccess"),
        DownloadHashBadReq(DownloadHash.BadRequest.class, "nstreamDownloadHashBadReq"),
        
        ConnMsgsConnectReq(R2NodeConnMsgs.ConnectReq.class, "silkConnMsgsConnect"),
        ConnMsgsConnectAcc(R2NodeConnMsgs.ConnectAcc.class, "silkConnMsgsConnectAcc"),
        ConnMsgsConnectRej(R2NodeConnMsgs.ConnectRej.class, "silkConnMsgsConnectRej"),
        ConnMsgsDisconnect(R2NodeConnMsgs.Disconnect.class, "silkConnMsgsDisconnect"),
        ConnMsgsDisconnectAck(R2NodeConnMsgs.DisconnectAck.class, "silkConnMsgsDisconnectAck"),
        ConnMsgsPing(R2NodeConnMsgs.Ping.class, "silkConnMsgsPing"),
        ConnMsgsPong(R2NodeConnMsgs.Pong.class, "silkConnMsgsPong")
        ;
        
        
        public final Class serializedClass;
        public final String serializerName;

        private GVoDSerializers(Class serializedClass, String serializerName) {
            this.serializedClass = serializedClass;
            this.serializerName = serializerName;
        }
    }
    
    public static boolean checkSetup() {
        for (GVoDSerializers gs : GVoDSerializers.values()) {
            if (Serializers.lookupSerializer(gs.serializedClass) == null) {
                return false;
            }
        }
        if(!BasicSerializerSetup.checkSetup()) {
            return false;
        }
        return true;
    }
    
    public static int registerSerializers(int startingId) {
        int currentId = startingId;
        
        FileIdentifierSerializer fileIdentifierSerializer = new FileIdentifierSerializer(currentId++);
        Serializers.register(fileIdentifierSerializer, GVoDSerializers.FileIdentifier.serializerName);
        Serializers.register(GVoDSerializers.FileIdentifier.serializedClass, GVoDSerializers.FileIdentifier.serializerName);
       
        NetConnectSerializer.Request connRequestSerializer = new NetConnectSerializer.Request(currentId++);
        Serializers.register(connRequestSerializer, GVoDSerializers.NetConnectRequest.serializerName);
        Serializers.register(GVoDSerializers.NetConnectRequest.serializedClass, GVoDSerializers.NetConnectRequest.serializerName);
        
        NetConnectSerializer.Response connResponseSerializer = new NetConnectSerializer.Response(currentId++);
        Serializers.register(connResponseSerializer, GVoDSerializers.NetConnectResponse.serializerName);
        Serializers.register(GVoDSerializers.NetConnectResponse.serializedClass, GVoDSerializers.NetConnectResponse.serializerName);
        
        BlockDetailsSerializer blockDetailsSerializer = new BlockDetailsSerializer(currentId++);
        Serializers.register(blockDetailsSerializer, GVoDSerializers.BlockDetails.serializerName);
        Serializers.register(GVoDSerializers.BlockDetails.serializedClass, GVoDSerializers.BlockDetails.serializerName);
        
        NetDetailedStateSerializer.Request torrentDefinitionRequestSerializer = new NetDetailedStateSerializer.Request(currentId++);
        Serializers.register(torrentDefinitionRequestSerializer, GVoDSerializers.NetDetailedStateRequest.serializerName);
        Serializers.register(GVoDSerializers.NetDetailedStateRequest.serializedClass, GVoDSerializers.NetDetailedStateRequest.serializerName);
        
        NetDetailedStateSerializer.Response torrentDefinitionResponseSerializer = new NetDetailedStateSerializer.Response(currentId++);
        Serializers.register(torrentDefinitionResponseSerializer, GVoDSerializers.NetDetailedStateResponse.serializerName);
        Serializers.register(GVoDSerializers.NetDetailedStateResponse.serializedClass, GVoDSerializers.NetDetailedStateResponse.serializerName);
        
        NetOpenTransferSerializer.DefinitionRequest transferDefinitionRequest = new NetOpenTransferSerializer.DefinitionRequest(currentId++);
        Serializers.register(transferDefinitionRequest, GVoDSerializers.NetOpenTransferRequest.serializerName);
        Serializers.register(GVoDSerializers.NetOpenTransferRequest.serializedClass, GVoDSerializers.NetOpenTransferRequest.serializerName);
        
        NetOpenTransferSerializer.DefinitionResponse transferDefinitionResponse = new NetOpenTransferSerializer.DefinitionResponse(currentId++);
        Serializers.register(transferDefinitionResponse, GVoDSerializers.NetOpenTransferResponse.serializerName);
        Serializers.register(GVoDSerializers.NetOpenTransferResponse.serializedClass, GVoDSerializers.NetOpenTransferResponse.serializerName);

        NetCloseTransferSerializer closeTransfer = new NetCloseTransferSerializer(currentId++);
        Serializers.register(closeTransfer, GVoDSerializers.NetCloseTransfer.serializerName);
        Serializers.register(GVoDSerializers.NetCloseTransfer.serializedClass, GVoDSerializers.NetCloseTransfer.serializerName);
        
        KHintSummarySerializer kHintSummarySerializer = new KHintSummarySerializer(currentId++);
        Serializers.register(kHintSummarySerializer, GVoDSerializers.KHintSummary.serializerName);
        Serializers.register(GVoDSerializers.KHintSummary.serializedClass, GVoDSerializers.KHintSummary.serializerName);
        
        CacheHintSerializer.Request cacheHintRequestSerializer = new CacheHintSerializer.Request(currentId++);
        Serializers.register(cacheHintRequestSerializer, GVoDSerializers.CacheHintRequest.serializerName);
        Serializers.register(GVoDSerializers.CacheHintRequest.serializedClass, GVoDSerializers.CacheHintRequest.serializerName);
        
        CacheHintSerializer.Response cacheHintResponseSerializer = new CacheHintSerializer.Response(currentId++);
        Serializers.register(cacheHintResponseSerializer, GVoDSerializers.CacheHintResponse.serializerName);
        Serializers.register(GVoDSerializers.CacheHintResponse.serializedClass, GVoDSerializers.CacheHintResponse.serializerName);
        
        DownloadPieceSerializer.Request downloadPieceRequestSerializer = new DownloadPieceSerializer.Request(currentId++);
        Serializers.register(downloadPieceRequestSerializer, GVoDSerializers.DownloadPieceRequest.serializerName);
        Serializers.register(GVoDSerializers.DownloadPieceRequest.serializedClass, GVoDSerializers.DownloadPieceRequest.serializerName);
        
        DownloadPieceSerializer.Success downloadPieceSuccessSerializer = new DownloadPieceSerializer.Success(currentId++);
        Serializers.register(downloadPieceSuccessSerializer, GVoDSerializers.DownloadPieceSuccess.serializerName);
        Serializers.register(GVoDSerializers.DownloadPieceSuccess.serializedClass, GVoDSerializers.DownloadPieceSuccess.serializerName);
        
        DownloadPieceSerializer.BadRequest downloadPieceBadRequestSerializer = new DownloadPieceSerializer.BadRequest(currentId++);
        Serializers.register(downloadPieceBadRequestSerializer, GVoDSerializers.DownloadPieceBadReq.serializerName);
        Serializers.register(GVoDSerializers.DownloadPieceBadReq.serializedClass, GVoDSerializers.DownloadPieceBadReq.serializerName);
        
        DownloadHashSerializer.Request downloadHashRequestSerializer = new DownloadHashSerializer.Request(currentId++);
        Serializers.register(downloadHashRequestSerializer, GVoDSerializers.DownloadHashRequest.serializerName);
        Serializers.register(GVoDSerializers.DownloadHashRequest.serializedClass, GVoDSerializers.DownloadHashRequest.serializerName);
        
        DownloadHashSerializer.Success downloadHashSuccessSerializer = new DownloadHashSerializer.Success(currentId++);
        Serializers.register(downloadHashSuccessSerializer, GVoDSerializers.DownloadHashSuccess.serializerName);
        Serializers.register(GVoDSerializers.DownloadHashSuccess.serializedClass, GVoDSerializers.DownloadHashSuccess.serializerName);
        
        DownloadHashSerializer.BadRequest downloadHashBadRequestuestSerializer = new DownloadHashSerializer.BadRequest(currentId++);
        Serializers.register(downloadHashBadRequestuestSerializer, GVoDSerializers.DownloadHashBadReq.serializerName);
        Serializers.register(GVoDSerializers.DownloadHashBadReq.serializedClass, GVoDSerializers.DownloadHashBadReq.serializerName);
        
        ConnMsgsSerializers.ConnectReq connMsgsConnReqSerializer = new ConnMsgsSerializers.ConnectReq(currentId++);
        Serializers.register(connMsgsConnReqSerializer, GVoDSerializers.ConnMsgsConnectReq.serializerName);
        Serializers.register(GVoDSerializers.ConnMsgsConnectReq.serializedClass, GVoDSerializers.ConnMsgsConnectReq.serializerName);
        
        ConnMsgsSerializers.ConnectAcc connMsgsConnAccSerializer = new ConnMsgsSerializers.ConnectAcc(currentId++);
        Serializers.register(connMsgsConnAccSerializer, GVoDSerializers.ConnMsgsConnectAcc.serializerName);
        Serializers.register(GVoDSerializers.ConnMsgsConnectAcc.serializedClass, GVoDSerializers.ConnMsgsConnectAcc.serializerName);
        
        ConnMsgsSerializers.ConnectRej connMsgsConnRejSerializer = new ConnMsgsSerializers.ConnectRej(currentId++);
        Serializers.register(connMsgsConnRejSerializer, GVoDSerializers.ConnMsgsConnectRej.serializerName);
        Serializers.register(GVoDSerializers.ConnMsgsConnectRej.serializedClass, GVoDSerializers.ConnMsgsConnectRej.serializerName);
        
        ConnMsgsSerializers.Disconnect connMsgsDiscSerializer = new ConnMsgsSerializers.Disconnect(currentId++);
        Serializers.register(connMsgsDiscSerializer, GVoDSerializers.ConnMsgsDisconnect.serializerName);
        Serializers.register(GVoDSerializers.ConnMsgsDisconnect.serializedClass, GVoDSerializers.ConnMsgsDisconnect.serializerName);
        
        ConnMsgsSerializers.DisconnectAck connMsgsDiscAckSerializer = new ConnMsgsSerializers.DisconnectAck(currentId++);
        Serializers.register(connMsgsDiscAckSerializer, GVoDSerializers.ConnMsgsDisconnectAck.serializerName);
        Serializers.register(GVoDSerializers.ConnMsgsDisconnectAck.serializedClass, GVoDSerializers.ConnMsgsDisconnectAck.serializerName);
        
        ConnMsgsSerializers.Ping connMsgsPingSerializer = new ConnMsgsSerializers.Ping(currentId++);
        Serializers.register(connMsgsPingSerializer, GVoDSerializers.ConnMsgsPing.serializerName);
        Serializers.register(GVoDSerializers.ConnMsgsPing.serializedClass, GVoDSerializers.ConnMsgsPing.serializerName);
        
        ConnMsgsSerializers.Pong connMsgsPongSerializer = new ConnMsgsSerializers.Pong(currentId++);
        Serializers.register(connMsgsPongSerializer, GVoDSerializers.ConnMsgsPong.serializerName);
        Serializers.register(GVoDSerializers.ConnMsgsPong.serializedClass, GVoDSerializers.ConnMsgsPong.serializerName);
        assert startingId + serializerIds == currentId;
        assert serializerIds <= maxSerializers;
        return startingId + maxSerializers;
    }
}
