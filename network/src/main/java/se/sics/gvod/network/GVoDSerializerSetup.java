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

import se.sics.gvod.stream.congestion.PLedbatState;
import se.sics.gvod.stream.congestion.PLedbatStateImplSerializer;
import se.sics.kompics.network.netty.serialization.Serializers;
import se.sics.ktoolbox.croupier.CroupierSerializerSetup;
import se.sics.ktoolbox.util.setup.BasicSerializerSetup;
import se.sics.nstream.transfer.event.TransferTorrent;
import se.sics.nstream.transfer.event.TransferTorrentSerializer;
import se.sics.nstream.util.BlockDetails;
import se.sics.nstream.util.BlockDetailsSerializer;
import se.sics.nstream.util.FileBaseDetails;
import se.sics.nstream.util.FileBaseDetailsSerializer;

/**
 * @author Alex Ormenisan <aaor@sics.se>
 */
public class GVoDSerializerSetup {
    public static int serializerIds = 5;
    
    public static enum GVoDSerializers {
        BlockDetails(BlockDetails.class, "nstreamBlockDetails"),
        FileBaseDetails(FileBaseDetails.class, "nstreamFileBaseDetails"),
        PLedbatStateImplSerializer(PLedbatState.Impl.class, "gvodPLedbatStateImplSerializer"),
        TransferTorrentRequest(TransferTorrent.Request.class, "nstreamTransferTorrentRequest"),
        TransferTorrentResponse(TransferTorrent.Response.class, "nstreamTransferTorrentResponse");
        
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
        
        BlockDetailsSerializer blockDetailsSerializer = new BlockDetailsSerializer(currentId++);
        Serializers.register(blockDetailsSerializer, GVoDSerializers.BlockDetails.serializerName);
        Serializers.register(GVoDSerializers.BlockDetails.serializedClass, GVoDSerializers.BlockDetails.serializerName);
        
        FileBaseDetailsSerializer fileBaseDetailsSerializer = new FileBaseDetailsSerializer(currentId++);
        Serializers.register(fileBaseDetailsSerializer, GVoDSerializers.FileBaseDetails.serializerName);
        Serializers.register(GVoDSerializers.FileBaseDetails.serializedClass, GVoDSerializers.FileBaseDetails.serializerName);
        
        PLedbatStateImplSerializer pLedbatStateImplSerializer = new PLedbatStateImplSerializer(currentId++);
        Serializers.register(pLedbatStateImplSerializer, GVoDSerializers.PLedbatStateImplSerializer.serializerName);
        Serializers.register(GVoDSerializers.PLedbatStateImplSerializer.serializedClass, GVoDSerializers.PLedbatStateImplSerializer.serializerName);
        
        TransferTorrentSerializer.Request transferTorrentRequestSerializer = new TransferTorrentSerializer.Request(currentId++);
        Serializers.register(transferTorrentRequestSerializer, GVoDSerializers.TransferTorrentRequest.serializerName);
        Serializers.register(GVoDSerializers.TransferTorrentRequest.serializedClass, GVoDSerializers.TransferTorrentRequest.serializerName);
        
        TransferTorrentSerializer.Response transferTorrentResponseSerializer = new TransferTorrentSerializer.Response(currentId++);
        Serializers.register(transferTorrentResponseSerializer, GVoDSerializers.TransferTorrentResponse.serializerName);
        Serializers.register(GVoDSerializers.TransferTorrentResponse.serializedClass, GVoDSerializers.TransferTorrentResponse.serializerName);
        
        assert startingId + serializerIds == currentId;
        return currentId;
    }
}
