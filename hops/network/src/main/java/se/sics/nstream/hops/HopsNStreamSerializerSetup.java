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
package se.sics.nstream.hops;

import se.sics.gvod.network.GVoDSerializerSetup;
import se.sics.kompics.network.netty.serialization.Serializers;
import se.sics.ktoolbox.util.setup.BasicSerializerSetup;

/**
 *
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class HopsNStreamSerializerSetup {
    public static int serializerIds = 1;
    
    public static enum MySerializers {
        HopsFES(HopsFES.class, "nstreamHopsFES");
        
        public final Class serializedClass;
        public final String serializerName;

        private MySerializers(Class serializedClass, String serializerName) {
            this.serializedClass = serializedClass;
            this.serializerName = serializerName;
        }
    }
    
    public static boolean checkSetup() {
        for (HopsNStreamSerializerSetup.MySerializers gs : HopsNStreamSerializerSetup.MySerializers.values()) {
            if (Serializers.lookupSerializer(gs.serializedClass) == null) {
                return false;
            }
        }
        if(!BasicSerializerSetup.checkSetup()) {
            return false;
        }
        if(!GVoDSerializerSetup.checkSetup()) {
            return false;
        }
        return true;
    }
    
    public static int registerSerializers(int startingId) {
        int currentId = startingId;
        
        HopsFESSerializer hopsFESSerializer = new HopsFESSerializer(currentId++);
        Serializers.register(hopsFESSerializer, HopsNStreamSerializerSetup.MySerializers.HopsFES.serializerName);
        Serializers.register(HopsNStreamSerializerSetup.MySerializers.HopsFES.serializedClass, HopsNStreamSerializerSetup.MySerializers.HopsFES.serializerName);
        
        assert startingId + serializerIds == currentId;
        return currentId;
    }
}
