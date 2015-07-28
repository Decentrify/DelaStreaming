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

import se.sics.gvod.common.msg.ReqStatus;
import se.sics.gvod.common.msg.peerMngr.AddOverlay;
import se.sics.gvod.common.msg.peerMngr.BootstrapGlobal;
import se.sics.gvod.common.msg.peerMngr.Heartbeat;
import se.sics.gvod.common.msg.peerMngr.JoinOverlay;
import se.sics.gvod.common.msg.peerMngr.OverlaySample;
import se.sics.gvod.common.msg.vod.Connection;
import se.sics.gvod.common.msg.vod.Download;
import se.sics.gvod.common.util.FileMetadata;
import se.sics.gvod.common.util.VodDescriptor;
import se.sics.gvod.network.serializers.bootstrap.AddOverlaySerializer;
import se.sics.gvod.network.serializers.bootstrap.BootstrapGlobalSerializer;
import se.sics.gvod.network.serializers.bootstrap.HeartbeatSerializer;
import se.sics.gvod.network.serializers.bootstrap.JoinOverlaySerializer;
import se.sics.gvod.network.serializers.bootstrap.OverlaySampleSerializer;
import se.sics.gvod.network.serializers.util.FileMetadataSerializer;
import se.sics.gvod.network.serializers.util.ReqStatusSerializer;
import se.sics.gvod.network.serializers.util.VodDescriptorSerializer;
import se.sics.gvod.network.serializers.vod.ConnectionSerializer;
import se.sics.gvod.network.serializers.vod.DownloadSerializer;
import se.sics.kompics.network.netty.serialization.Serializers;
import se.sics.p2ptoolbox.util.serializer.BasicSerializerSetup;

/**
 * @author Alex Ormenisan <aaor@sics.se>
 */
public class GVoDSerializerSetup {
    public static int serializerIds = 20;
    
    public static enum GVoDSerializers {
        FileMetadata(FileMetadata.class, "gvodFileMetadataSerializer"),
        ReqStatus(ReqStatus.class, "gvodReqStatusSerializer"),
        VodDescriptor(VodDescriptor.class, "gvodVodDescriptorSerializer"),
        AddOverlayRequest(AddOverlay.Request.class, "gvodAddOverlayRequestSerializer"),
        AddOverlayResponse(AddOverlay.Response.class, "gvodAddOverlayResponseSerializer"),
        BootstrapGlobalRequest(BootstrapGlobal.Request.class, "gvodBootstrapGlobalRequestSerializer"),
        BootstrapGlobalResponse(BootstrapGlobal.Response.class, "gvodBootstrapGlobalResponseSerializer"),
        HeartbeatOneWay(Heartbeat.OneWay.class, "gvodHeartbeatOneWaySerializer"),
        JoinOverlayRequest(JoinOverlay.Request.class, "gvodJoinOverlayRequestSerializer"),
        JoinOverlayResponse(JoinOverlay.Response.class, "gvodJoinOverlayResponseSerializer"),
        OverlaySampleRequest(OverlaySample.Request.class, "gvodOverlaySampleRequestSerializer"),
        OverlaySampleResponse(OverlaySample.Response.class, "gvodOverlaySampleResponseSerializer"),
        ConnectionRequest(Connection.Request.class, "gvodConnectionRequestSerializer"),
        ConnectionResponse(Connection.Response.class, "gvodConnectionResponseSerializer"),
        ConnectionClose(Connection.Close.class, "gvodConnectionCloseSerializer"),
        ConnectionUpdate(Connection.Update.class, "gvodConnectionUpdateSerializer"),
        DownloadDataRequest(Download.DataRequest.class, "gvodDownloadDataRequestSerializer"),
        DownloadDataResponse(Download.DataResponse.class, "gvodDownloadDataResponseSerializer"),
        DownloadHashRequest(Download.HashRequest.class, "gvodDownloadHashRequestSerializer"),
        DownloadHashResponse(Download.HashResponse.class, "gvodDownloadHashResponseSerializer");
        
        public final Class serializedClass;
        public final String serializerName;

        private GVoDSerializers(Class serializedClass, String serializerName) {
            this.serializedClass = serializedClass;
            this.serializerName = serializerName;
        }
    }
    
    public static void checkSetup() {
        for (GVoDSerializers gs : GVoDSerializers.values()) {
            if (Serializers.lookupSerializer(gs.serializedClass) == null) {
                throw new RuntimeException("No serializer for " + gs.serializedClass);
            }
        }
        BasicSerializerSetup.checkSetup();
    }
    
    public static int registerSerializers(int startingId) {
        int currentId = startingId;
        
        FileMetadataSerializer fileMetadataSerializer = new FileMetadataSerializer(currentId++);
        Serializers.register(fileMetadataSerializer, GVoDSerializers.FileMetadata.serializerName);
        Serializers.register(GVoDSerializers.FileMetadata.serializedClass, GVoDSerializers.FileMetadata.serializerName);
        
        ReqStatusSerializer reqStatusSerializer = new ReqStatusSerializer(currentId++);
        Serializers.register(reqStatusSerializer, GVoDSerializers.ReqStatus.serializerName);
        Serializers.register(GVoDSerializers.ReqStatus.serializedClass, GVoDSerializers.ReqStatus.serializerName);
        
        VodDescriptorSerializer vodDescriptorSerializer = new VodDescriptorSerializer(currentId++);
        Serializers.register(vodDescriptorSerializer, GVoDSerializers.VodDescriptor.serializerName);
        Serializers.register(GVoDSerializers.VodDescriptor.serializedClass, GVoDSerializers.VodDescriptor.serializerName);
        
        AddOverlaySerializer.Request addOverlayRequestSerializer = new AddOverlaySerializer.Request(currentId++);
        Serializers.register(addOverlayRequestSerializer, GVoDSerializers.AddOverlayRequest.serializerName);
        Serializers.register(GVoDSerializers.AddOverlayRequest.serializedClass, GVoDSerializers.AddOverlayRequest.serializerName);
        
        AddOverlaySerializer.Response addOverlayResponseSerializer = new AddOverlaySerializer.Response(currentId++);
        Serializers.register(addOverlayResponseSerializer, GVoDSerializers.AddOverlayResponse.serializerName);
        Serializers.register(GVoDSerializers.AddOverlayResponse.serializedClass, GVoDSerializers.AddOverlayResponse.serializerName);
        
        BootstrapGlobalSerializer.Request bootstrapGlobalRequestSerializer = new BootstrapGlobalSerializer.Request(currentId++);
        Serializers.register(bootstrapGlobalRequestSerializer, GVoDSerializers.BootstrapGlobalRequest.serializerName);
        Serializers.register(GVoDSerializers.BootstrapGlobalRequest.serializedClass, GVoDSerializers.BootstrapGlobalRequest.serializerName);
        
        BootstrapGlobalSerializer.Response bootstrapGlobalResponseSerializer = new BootstrapGlobalSerializer.Response(currentId++);
        Serializers.register(bootstrapGlobalResponseSerializer, GVoDSerializers.BootstrapGlobalResponse.serializerName);
        Serializers.register(GVoDSerializers.BootstrapGlobalResponse.serializedClass, GVoDSerializers.BootstrapGlobalResponse.serializerName);
        
        HeartbeatSerializer.OneWay heartbeatOneWaySerializer = new HeartbeatSerializer.OneWay(currentId++);
        Serializers.register(heartbeatOneWaySerializer, GVoDSerializers.HeartbeatOneWay.serializerName);
        Serializers.register(GVoDSerializers.HeartbeatOneWay.serializedClass, GVoDSerializers.HeartbeatOneWay.serializerName);
        
        JoinOverlaySerializer.Request joinOverlayRequestSerializer = new JoinOverlaySerializer.Request(currentId++);
        Serializers.register(joinOverlayRequestSerializer, GVoDSerializers.JoinOverlayRequest.serializerName);
        Serializers.register(GVoDSerializers.JoinOverlayRequest.serializedClass, GVoDSerializers.JoinOverlayRequest.serializerName);
        
        JoinOverlaySerializer.Response joinOverlayResponseSerializer = new JoinOverlaySerializer.Response(currentId++);
        Serializers.register(joinOverlayResponseSerializer, GVoDSerializers.JoinOverlayResponse.serializerName);
        Serializers.register(GVoDSerializers.JoinOverlayResponse.serializedClass, GVoDSerializers.JoinOverlayResponse.serializerName);
        
        OverlaySampleSerializer.Request overlaySampleRequestSerializer = new OverlaySampleSerializer.Request(currentId++);
        Serializers.register(overlaySampleRequestSerializer, GVoDSerializers.OverlaySampleRequest.serializerName);
        Serializers.register(GVoDSerializers.OverlaySampleRequest.serializedClass, GVoDSerializers.OverlaySampleRequest.serializerName);
        
        OverlaySampleSerializer.Response overlaySampleResponseSerializer = new OverlaySampleSerializer.Response(currentId++);
        Serializers.register(overlaySampleResponseSerializer, GVoDSerializers.OverlaySampleResponse.serializerName);
        Serializers.register(GVoDSerializers.OverlaySampleResponse.serializedClass, GVoDSerializers.OverlaySampleResponse.serializerName);
        
        ConnectionSerializer.Request connectionRequestSerializer = new ConnectionSerializer.Request(currentId++);
        Serializers.register(connectionRequestSerializer, GVoDSerializers.ConnectionRequest.serializerName);
        Serializers.register(GVoDSerializers.ConnectionRequest.serializedClass, GVoDSerializers.ConnectionRequest.serializerName);
        
        ConnectionSerializer.Response connectionResponseSerializer = new ConnectionSerializer.Response(currentId++);
        Serializers.register(connectionResponseSerializer, GVoDSerializers.ConnectionResponse.serializerName);
        Serializers.register(GVoDSerializers.ConnectionResponse.serializedClass, GVoDSerializers.ConnectionResponse.serializerName);
        
        ConnectionSerializer.Close connectionCloseSerializer = new ConnectionSerializer.Close(currentId++);
        Serializers.register(connectionCloseSerializer, GVoDSerializers.ConnectionClose.serializerName);
        Serializers.register(GVoDSerializers.ConnectionClose.serializedClass, GVoDSerializers.ConnectionClose.serializerName);
        
        ConnectionSerializer.Update connectionUpdateSerializer = new ConnectionSerializer.Update(currentId++);
        Serializers.register(connectionUpdateSerializer, GVoDSerializers.ConnectionUpdate.serializerName);
        Serializers.register(GVoDSerializers.ConnectionUpdate.serializedClass, GVoDSerializers.ConnectionUpdate.serializerName);
        
        DownloadSerializer.DataRequest downloadDataRequestSerializer = new DownloadSerializer.DataRequest(currentId++);
        Serializers.register(downloadDataRequestSerializer, GVoDSerializers.DownloadDataRequest.serializerName);
        Serializers.register(GVoDSerializers.DownloadDataRequest.serializedClass, GVoDSerializers.DownloadDataRequest.serializerName);
        
        DownloadSerializer.DataResponse downloadDataResponseSerializer = new DownloadSerializer.DataResponse(currentId++);
        Serializers.register(downloadDataResponseSerializer, GVoDSerializers.DownloadDataResponse.serializerName);
        Serializers.register(GVoDSerializers.DownloadDataResponse.serializedClass, GVoDSerializers.DownloadDataResponse.serializerName);
        
        DownloadSerializer.HashRequest downloadHashRequestSerializer = new DownloadSerializer.HashRequest(currentId++);
        Serializers.register(downloadHashRequestSerializer, GVoDSerializers.DownloadHashRequest.serializerName);
        Serializers.register(GVoDSerializers.DownloadHashRequest.serializedClass, GVoDSerializers.DownloadHashRequest.serializerName);
        
        DownloadSerializer.HashResponse downloadHashResponseSerializer = new DownloadSerializer.HashResponse(currentId++);
        Serializers.register(downloadHashResponseSerializer, GVoDSerializers.DownloadHashResponse.serializerName);
        Serializers.register(GVoDSerializers.DownloadHashResponse.serializedClass, GVoDSerializers.DownloadHashResponse.serializerName);
        
        assert startingId + serializerIds == currentId;
        return currentId;
    }
}
