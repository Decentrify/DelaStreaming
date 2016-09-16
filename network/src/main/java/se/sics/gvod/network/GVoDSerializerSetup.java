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
import se.sics.ktoolbox.croupier.CroupierSerializerSetup;
import se.sics.ktoolbox.util.setup.BasicSerializerSetup;
import se.sics.nstream.storage.cache.KHint;
import se.sics.nstream.storage.cache.KHintSummarySerializer;
import se.sics.nstream.torrent.FileIdentifier;
import se.sics.nstream.torrent.FileIdentifierSerializer;
import se.sics.nstream.torrent.conn.msg.NetCloseTransfer;
import se.sics.nstream.torrent.conn.msg.NetCloseTransferSerializer;
import se.sics.nstream.torrent.conn.msg.NetConnect;
import se.sics.nstream.torrent.conn.msg.NetConnectSerializer;
import se.sics.nstream.torrent.conn.msg.NetDetailedState;
import se.sics.nstream.torrent.conn.msg.NetDetailedStateSerializer;
import se.sics.nstream.torrent.conn.msg.NetOpenTransfer;
import se.sics.nstream.torrent.conn.msg.NetOpenTransferSerializer;
import se.sics.nstream.torrent.event.HashGet;
import se.sics.nstream.torrent.event.HashGetSerializer;
import se.sics.nstream.torrent.event.PieceGet;
import se.sics.nstream.torrent.event.PieceGetSerializer;
import se.sics.nstream.torrent.event.TorrentGet;
import se.sics.nstream.torrent.event.TorrentGetSerializer;
import se.sics.nstream.torrent.transfer.msg.CacheHint;
import se.sics.nstream.torrent.transfer.msg.CacheHintSerializer;
import se.sics.nstream.torrent.transfer.msg.DownloadHash;
import se.sics.nstream.torrent.transfer.msg.DownloadHashSerializer;
import se.sics.nstream.torrent.transfer.msg.DownloadPiece;
import se.sics.nstream.torrent.transfer.msg.DownloadPieceSerializer;
import se.sics.nstream.util.BlockDetails;
import se.sics.nstream.util.BlockDetailsSerializer;
import se.sics.nstream.util.FileBaseDetails;
import se.sics.nstream.util.FileBaseDetailsSerializer;

/**
 * @author Alex Ormenisan <aaor@sics.se>
 */
public class GVoDSerializerSetup {
    public static int serializerIds = 24;
    
    public static enum GVoDSerializers {
        FileIdentifier(FileIdentifier.class, "nStreamFileIdentifier"),
        NetConnectRequest(NetConnect.Request.class, "nStreamNetConnRequest"),
        NetConnectResponse(NetConnect.Response.class, "nStreamNetConnResponse"),
        NetDetailedStateRequest(NetDetailedState.Request.class, "nStreamNetDetailedStateRequest"),
        NetDetailedStateResponse(NetDetailedState.Response.class, "nStreamNetDetailedStateResponse"),
        NetOpenTransferRequest(NetOpenTransfer.Request.class, "nStreamNetOpenTransferRequest"),
        NetOpenTransferResponse(NetOpenTransfer.Response.class, "nStreamNetOpenTransferResponse"),
        NetCloseTransfer(NetCloseTransfer.class, "nStreamNetCloseTransfer"),
        KHintSummary(KHint.Summary.class, "nStreamKHintSummary"),
        CacheHintRequest(CacheHint.Request.class, "nstreamCacheHintRequest"),
        CacheHintResponse(CacheHint.Response.class, "nstreamCacheHintResponse"),
        DownloadPieceRequest(DownloadPiece.Request.class, "nstreamDownloadPieceRequest"),
        DownloadPieceResponse(DownloadPiece.Response.class, "nstreamDownloadPieceResponse"),
        DownloadHashRequest(DownloadHash.Request.class, "nstreamDownloadHashRequest"),
        DownloadHashResponse(DownloadHash.Response.class, "nstreamDownloadHashResponse"),

        BlockDetails(BlockDetails.class, "nstreamBlockDetails"),
        FileBaseDetails(FileBaseDetails.class, "nstreamFileBaseDetails"),
        TorrentGetRequest(TorrentGet.Request.class, "nstreamTorrentGetRequest"),
        TorrentGetResponse(TorrentGet.Response.class, "nstreamTorrentGetResponse"),
        
        HashGetRequest(HashGet.Request.class, "nstreamHashGetRequest"),
        HashGetResponse(HashGet.Response.class, "nstreamHashGetResopnse"),
        PieceGetRequest(PieceGet.Request.class, "nstreamPieceGetRequest"),
        PieceGetRangeRequest(PieceGet.RangeRequest.class, "nstreamPieceGetRangeRequest"),
        PieceGetResponse(PieceGet.Response.class, "nstreamPieceGetResopnse");
        
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
        if(!CroupierSerializerSetup.checkSetup()) {
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
        
        DownloadPieceSerializer.Response downloadPieceResponseSerializer = new DownloadPieceSerializer.Response(currentId++);
        Serializers.register(downloadPieceResponseSerializer, GVoDSerializers.DownloadPieceResponse.serializerName);
        Serializers.register(GVoDSerializers.DownloadPieceResponse.serializedClass, GVoDSerializers.DownloadPieceResponse.serializerName);
        
        DownloadHashSerializer.Request downloadHashRequestSerializer = new DownloadHashSerializer.Request(currentId++);
        Serializers.register(downloadHashRequestSerializer, GVoDSerializers.DownloadHashRequest.serializerName);
        Serializers.register(GVoDSerializers.DownloadHashRequest.serializedClass, GVoDSerializers.DownloadHashRequest.serializerName);
        
        DownloadHashSerializer.Response downloadHashResponseSerializer = new DownloadHashSerializer.Response(currentId++);
        Serializers.register(downloadHashResponseSerializer, GVoDSerializers.DownloadHashResponse.serializerName);
        Serializers.register(GVoDSerializers.DownloadHashResponse.serializedClass, GVoDSerializers.DownloadHashResponse.serializerName);
        
        BlockDetailsSerializer blockDetailsSerializer = new BlockDetailsSerializer(currentId++);
        Serializers.register(blockDetailsSerializer, GVoDSerializers.BlockDetails.serializerName);
        Serializers.register(GVoDSerializers.BlockDetails.serializedClass, GVoDSerializers.BlockDetails.serializerName);
        
        FileBaseDetailsSerializer fileBaseDetailsSerializer = new FileBaseDetailsSerializer(currentId++);
        Serializers.register(fileBaseDetailsSerializer, GVoDSerializers.FileBaseDetails.serializerName);
        Serializers.register(GVoDSerializers.FileBaseDetails.serializedClass, GVoDSerializers.FileBaseDetails.serializerName);
        
        TorrentGetSerializer.Request torrentGetRequestSerializer = new TorrentGetSerializer.Request(currentId++);
        Serializers.register(torrentGetRequestSerializer, GVoDSerializers.TorrentGetRequest.serializerName);
        Serializers.register(GVoDSerializers.TorrentGetRequest.serializedClass, GVoDSerializers.TorrentGetRequest.serializerName);
        
        TorrentGetSerializer.Response torrentGetResponseSerializer = new TorrentGetSerializer.Response(currentId++);
        Serializers.register(torrentGetResponseSerializer, GVoDSerializers.TorrentGetResponse.serializerName);
        Serializers.register(GVoDSerializers.TorrentGetResponse.serializedClass, GVoDSerializers.TorrentGetResponse.serializerName);
        
        HashGetSerializer.Request hashGetRequestSerializer = new HashGetSerializer.Request(currentId++);
        Serializers.register(hashGetRequestSerializer, GVoDSerializers.HashGetRequest.serializerName);
        Serializers.register(GVoDSerializers.HashGetRequest.serializedClass, GVoDSerializers.HashGetRequest.serializerName);
        
        HashGetSerializer.Response hashGetResponseSerializer = new HashGetSerializer.Response(currentId++);
        Serializers.register(hashGetResponseSerializer, GVoDSerializers.HashGetResponse.serializerName);
        Serializers.register(GVoDSerializers.HashGetResponse.serializedClass, GVoDSerializers.HashGetResponse.serializerName);
        
        PieceGetSerializer.Request pieceGetRequestSerializer = new PieceGetSerializer.Request(currentId++);
        Serializers.register(pieceGetRequestSerializer, GVoDSerializers.PieceGetRequest.serializerName);
        Serializers.register(GVoDSerializers.PieceGetRequest.serializedClass, GVoDSerializers.PieceGetRequest.serializerName);
         
        PieceGetSerializer.RangeRequest pieceGetRangeRequestSerializer = new PieceGetSerializer.RangeRequest(currentId++);
        Serializers.register(pieceGetRangeRequestSerializer, GVoDSerializers.PieceGetRangeRequest.serializerName);
        Serializers.register(GVoDSerializers.PieceGetRangeRequest.serializedClass, GVoDSerializers.PieceGetRangeRequest.serializerName);
        
        PieceGetSerializer.Response pieceGetResponseSerializer = new PieceGetSerializer.Response(currentId++);
        Serializers.register(pieceGetResponseSerializer, GVoDSerializers.PieceGetResponse.serializerName);
        Serializers.register(GVoDSerializers.PieceGetResponse.serializedClass, GVoDSerializers.PieceGetResponse.serializerName);
        
        assert startingId + serializerIds == currentId;
        return currentId;
    }
}
