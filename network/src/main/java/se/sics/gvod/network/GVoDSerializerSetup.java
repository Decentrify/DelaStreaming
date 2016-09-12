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
import se.sics.nstream.torrent.conn.msg.CacheHint;
import se.sics.nstream.torrent.conn.msg.CacheHintSerializer;
import se.sics.nstream.torrent.conn.msg.DownloadPiece;
import se.sics.nstream.torrent.conn.msg.DownloadPieceSerializer;
import se.sics.nstream.torrent.event.HashGet;
import se.sics.nstream.torrent.event.HashGetSerializer;
import se.sics.nstream.torrent.event.PieceGet;
import se.sics.nstream.torrent.event.PieceGetSerializer;
import se.sics.nstream.torrent.event.TorrentGet;
import se.sics.nstream.torrent.event.TorrentGetSerializer;
import se.sics.nstream.util.BlockDetails;
import se.sics.nstream.util.BlockDetailsSerializer;
import se.sics.nstream.util.FileBaseDetails;
import se.sics.nstream.util.FileBaseDetailsSerializer;

/**
 * @author Alex Ormenisan <aaor@sics.se>
 */
public class GVoDSerializerSetup {
    public static int serializerIds = 15;
    
    public static enum GVoDSerializers {
        FileIdentifier(FileIdentifier.class, "nStreamFileIdentifier"),
        KHintSummary(KHint.Summary.class, "nStreamKHintSummary"),
        CacheHintRequest(CacheHint.Request.class, "nstreamCacheHintRequest"),
        CacheHintResponse(CacheHint.Response.class, "nstreamCacheHintResponse"),
        DownloadPieceRequest(DownloadPiece.Request.class, "nstreamDownloadPieceRequest"),
        DownloadPieceResponse(DownloadPiece.Response.class, "nstreamDownloadPieceResponse"),
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
