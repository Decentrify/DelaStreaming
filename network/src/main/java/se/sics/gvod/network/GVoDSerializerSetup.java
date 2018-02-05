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
import se.sics.silk.r2torrent.transfer.msgs.R1TransferConnMsgs;
import se.sics.silk.r2torrent.transfer.msgs.R1TransferConnSerializers;
import se.sics.silk.r2torrent.transfer.msgs.R1TransferMsgs;
import se.sics.silk.r2torrent.transfer.msgs.R1TransferSerializers;

/**
 * @author Alex Ormenisan <aaor@sics.se>
 */
public class GVoDSerializerSetup {

  //You may add up to max serializers without the need to recompile all the projects that use the serializer space after gvod
  public static int maxSerializers = 35;
  public static int serializerIds = 30;

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

    R1CacheHintReq(R1TransferMsgs.CacheHintReq.class, "silkCacheHintReq"),
    R1CacheHintAcc(R1TransferMsgs.CacheHintAcc.class, "silkCacheHintAcc"),
    R1HashReq(R1TransferMsgs.HashReq.class, "silkHashReq"),
    R1HashResp(R1TransferMsgs.HashResp.class, "silkHashReq"),
    R1BlockReq(R1TransferMsgs.BlockReq.class, "silkBlockReq"),
    R1PieceReq(R1TransferMsgs.PieceReq.class, "silkPieceReq"),
    R1PieceResp(R1TransferMsgs.PieceResp.class, "silkPieceResp"),
    R1TransferConnect(R1TransferConnMsgs.Connect.class, "silkTransferConnect"),
    R1TransferConnectAcc(R1TransferConnMsgs.ConnectAcc.class, "silkTransferConnectAcc"),
    R1TransferDisconnect(R1TransferConnMsgs.Disconnect.class, "silkTransferDisconnect"),
    R1TransferPing(R1TransferConnMsgs.Ping.class, "silkTransferPing"),
    R1TransferPong(R1TransferConnMsgs.Pong.class, "silkTransferPong");

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
    if (!BasicSerializerSetup.checkSetup()) {
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
    Serializers.register(GVoDSerializers.NetConnectRequest.serializedClass,
      GVoDSerializers.NetConnectRequest.serializerName);

    NetConnectSerializer.Response connResponseSerializer = new NetConnectSerializer.Response(currentId++);
    Serializers.register(connResponseSerializer, GVoDSerializers.NetConnectResponse.serializerName);
    Serializers.register(GVoDSerializers.NetConnectResponse.serializedClass,
      GVoDSerializers.NetConnectResponse.serializerName);

    BlockDetailsSerializer blockDetailsSerializer = new BlockDetailsSerializer(currentId++);
    Serializers.register(blockDetailsSerializer, GVoDSerializers.BlockDetails.serializerName);
    Serializers.register(GVoDSerializers.BlockDetails.serializedClass, GVoDSerializers.BlockDetails.serializerName);

    NetDetailedStateSerializer.Request torrentDefinitionRequestSerializer = new NetDetailedStateSerializer.Request(
      currentId++);
    Serializers.register(torrentDefinitionRequestSerializer, GVoDSerializers.NetDetailedStateRequest.serializerName);
    Serializers.register(GVoDSerializers.NetDetailedStateRequest.serializedClass,
      GVoDSerializers.NetDetailedStateRequest.serializerName);

    NetDetailedStateSerializer.Response torrentDefinitionResponseSerializer = new NetDetailedStateSerializer.Response(
      currentId++);
    Serializers.register(torrentDefinitionResponseSerializer, GVoDSerializers.NetDetailedStateResponse.serializerName);
    Serializers.register(GVoDSerializers.NetDetailedStateResponse.serializedClass,
      GVoDSerializers.NetDetailedStateResponse.serializerName);

    NetOpenTransferSerializer.DefinitionRequest transferDefinitionRequest
      = new NetOpenTransferSerializer.DefinitionRequest(currentId++);
    Serializers.register(transferDefinitionRequest, GVoDSerializers.NetOpenTransferRequest.serializerName);
    Serializers.register(GVoDSerializers.NetOpenTransferRequest.serializedClass,
      GVoDSerializers.NetOpenTransferRequest.serializerName);

    NetOpenTransferSerializer.DefinitionResponse transferDefinitionResponse
      = new NetOpenTransferSerializer.DefinitionResponse(currentId++);
    Serializers.register(transferDefinitionResponse, GVoDSerializers.NetOpenTransferResponse.serializerName);
    Serializers.register(GVoDSerializers.NetOpenTransferResponse.serializedClass,
      GVoDSerializers.NetOpenTransferResponse.serializerName);

    NetCloseTransferSerializer closeTransfer = new NetCloseTransferSerializer(currentId++);
    Serializers.register(closeTransfer, GVoDSerializers.NetCloseTransfer.serializerName);
    Serializers.register(GVoDSerializers.NetCloseTransfer.serializedClass,
      GVoDSerializers.NetCloseTransfer.serializerName);

    KHintSummarySerializer kHintSummarySerializer = new KHintSummarySerializer(currentId++);
    Serializers.register(kHintSummarySerializer, GVoDSerializers.KHintSummary.serializerName);
    Serializers.register(GVoDSerializers.KHintSummary.serializedClass, GVoDSerializers.KHintSummary.serializerName);

    CacheHintSerializer.Request cacheHintRequestSerializer = new CacheHintSerializer.Request(currentId++);
    Serializers.register(cacheHintRequestSerializer, GVoDSerializers.CacheHintRequest.serializerName);
    Serializers.register(GVoDSerializers.CacheHintRequest.serializedClass,
      GVoDSerializers.CacheHintRequest.serializerName);

    CacheHintSerializer.Response cacheHintResponseSerializer = new CacheHintSerializer.Response(currentId++);
    Serializers.register(cacheHintResponseSerializer, GVoDSerializers.CacheHintResponse.serializerName);
    Serializers.register(GVoDSerializers.CacheHintResponse.serializedClass,
      GVoDSerializers.CacheHintResponse.serializerName);

    DownloadPieceSerializer.Request downloadPieceRequestSerializer = new DownloadPieceSerializer.Request(currentId++);
    Serializers.register(downloadPieceRequestSerializer, GVoDSerializers.DownloadPieceRequest.serializerName);
    Serializers.register(GVoDSerializers.DownloadPieceRequest.serializedClass,
      GVoDSerializers.DownloadPieceRequest.serializerName);

    DownloadPieceSerializer.Success downloadPieceSuccessSerializer = new DownloadPieceSerializer.Success(currentId++);
    Serializers.register(downloadPieceSuccessSerializer, GVoDSerializers.DownloadPieceSuccess.serializerName);
    Serializers.register(GVoDSerializers.DownloadPieceSuccess.serializedClass,
      GVoDSerializers.DownloadPieceSuccess.serializerName);

    DownloadPieceSerializer.BadRequest downloadPieceBadRequestSerializer = new DownloadPieceSerializer.BadRequest(
      currentId++);
    Serializers.register(downloadPieceBadRequestSerializer, GVoDSerializers.DownloadPieceBadReq.serializerName);
    Serializers.register(GVoDSerializers.DownloadPieceBadReq.serializedClass,
      GVoDSerializers.DownloadPieceBadReq.serializerName);

    DownloadHashSerializer.Request downloadHashRequestSerializer = new DownloadHashSerializer.Request(currentId++);
    Serializers.register(downloadHashRequestSerializer, GVoDSerializers.DownloadHashRequest.serializerName);
    Serializers.register(GVoDSerializers.DownloadHashRequest.serializedClass,
      GVoDSerializers.DownloadHashRequest.serializerName);

    DownloadHashSerializer.Success downloadHashSuccessSerializer = new DownloadHashSerializer.Success(currentId++);
    Serializers.register(downloadHashSuccessSerializer, GVoDSerializers.DownloadHashSuccess.serializerName);
    Serializers.register(GVoDSerializers.DownloadHashSuccess.serializedClass,
      GVoDSerializers.DownloadHashSuccess.serializerName);

    DownloadHashSerializer.BadRequest downloadHashBadRequestuestSerializer = new DownloadHashSerializer.BadRequest(
      currentId++);
    Serializers.register(downloadHashBadRequestuestSerializer, GVoDSerializers.DownloadHashBadReq.serializerName);
    Serializers.register(GVoDSerializers.DownloadHashBadReq.serializedClass,
      GVoDSerializers.DownloadHashBadReq.serializerName);

    R1TransferSerializers.CacheHintReq cacheHintReq = new R1TransferSerializers.CacheHintReq(currentId++);
    Serializers.register(cacheHintReq, GVoDSerializers.R1CacheHintReq.serializerName);
    Serializers.register(GVoDSerializers.R1CacheHintReq.serializedClass, GVoDSerializers.R1CacheHintReq.serializerName);

    R1TransferSerializers.CacheHintAcc cacheHintAcc = new R1TransferSerializers.CacheHintAcc(currentId++);
    Serializers.register(cacheHintAcc, GVoDSerializers.R1CacheHintAcc.serializerName);
    Serializers.register(GVoDSerializers.R1CacheHintAcc.serializedClass, GVoDSerializers.R1CacheHintAcc.serializerName);

    R1TransferSerializers.HashReq hashReq = new R1TransferSerializers.HashReq(currentId++);
    Serializers.register(hashReq, GVoDSerializers.R1HashReq.serializerName);
    Serializers.register(GVoDSerializers.R1HashReq.serializedClass, GVoDSerializers.R1HashReq.serializerName);

    R1TransferSerializers.HashResp hashResp = new R1TransferSerializers.HashResp(currentId++);
    Serializers.register(hashResp, GVoDSerializers.R1HashResp.serializerName);
    Serializers.register(GVoDSerializers.R1HashResp.serializedClass, GVoDSerializers.R1HashResp.serializerName);

    R1TransferSerializers.BlockReq blockReq = new R1TransferSerializers.BlockReq(currentId++);
    Serializers.register(blockReq, GVoDSerializers.R1BlockReq.serializerName);
    Serializers.register(GVoDSerializers.R1BlockReq.serializedClass, GVoDSerializers.R1BlockReq.serializerName);

    R1TransferSerializers.PieceReq pieceReq = new R1TransferSerializers.PieceReq(currentId++);
    Serializers.register(pieceReq, GVoDSerializers.R1PieceReq.serializerName);
    Serializers.register(GVoDSerializers.R1PieceReq.serializedClass, GVoDSerializers.R1PieceReq.serializerName);

    R1TransferSerializers.PieceResp pieceResp = new R1TransferSerializers.PieceResp(currentId++);
    Serializers.register(pieceResp, GVoDSerializers.R1PieceResp.serializerName);
    Serializers.register(GVoDSerializers.R1PieceResp.serializedClass, GVoDSerializers.R1PieceResp.serializerName);

    R1TransferConnSerializers.Connect transferConnect = new R1TransferConnSerializers.Connect(currentId++);
    Serializers.register(transferConnect, GVoDSerializers.R1TransferConnect.serializerName);
    Serializers.register(GVoDSerializers.R1TransferConnect.serializedClass,
      GVoDSerializers.R1TransferConnect.serializerName);

    R1TransferConnSerializers.ConnectAcc transferConnectAcc = new R1TransferConnSerializers.ConnectAcc(currentId++);
    Serializers.register(transferConnectAcc, GVoDSerializers.R1TransferConnectAcc.serializerName);
    Serializers.register(GVoDSerializers.R1TransferConnectAcc.serializedClass,
      GVoDSerializers.R1TransferConnectAcc.serializerName);
    
    R1TransferConnSerializers.Ping transferPing = new R1TransferConnSerializers.Ping(currentId++);
    Serializers.register(transferPing, GVoDSerializers.R1TransferPing.serializerName);
    Serializers.register(GVoDSerializers.R1TransferPing.serializedClass, 
      GVoDSerializers.R1TransferPing.serializerName);
    
    R1TransferConnSerializers.Pong transferPong = new R1TransferConnSerializers.Pong(currentId++);
    Serializers.register(transferPong, GVoDSerializers.R1TransferPong.serializerName);
    Serializers.register(GVoDSerializers.R1TransferPong.serializedClass, 
      GVoDSerializers.R1TransferPong.serializerName);
    
    R1TransferConnSerializers.Disconnect transferDisconnect = new R1TransferConnSerializers.Disconnect(currentId++);
    Serializers.register(transferDisconnect, GVoDSerializers.R1TransferDisconnect.serializerName);
    Serializers.register(GVoDSerializers.R1TransferDisconnect.serializedClass, 
      GVoDSerializers.R1TransferDisconnect.serializerName);

    assert startingId + serializerIds == currentId;
    assert serializerIds <= maxSerializers;
    return startingId + maxSerializers;
  }
}
