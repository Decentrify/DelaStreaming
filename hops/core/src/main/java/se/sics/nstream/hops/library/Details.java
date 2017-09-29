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
package se.sics.nstream.hops.library;

import java.util.HashMap;
import java.util.Map;
import org.javatuples.Pair;
import se.sics.kompics.id.Identifier;
import se.sics.nstream.hops.kafka.KafkaEndpoint;
import se.sics.nstream.hops.kafka.KafkaResource;
import se.sics.nstream.hops.storage.disk.DiskEndpoint;
import se.sics.nstream.hops.storage.disk.DiskResource;
import se.sics.nstream.hops.storage.hdfs.HDFSEndpoint;
import se.sics.nstream.hops.storage.hdfs.HDFSResource;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class Details {
    public static enum Types {
        HDFS, DISK;
    }
    
    public static class Endpoints {
        public final Pair<Identifier, DiskEndpoint> diskEndpoint;
        public final Pair<Identifier, HDFSEndpoint> hdfsEndpoint;
        public final Pair<Identifier, KafkaEndpoint> kafkaEndpoint;
        
        public Endpoints(Pair<Identifier, DiskEndpoint> diskEndpoint, Pair<Identifier, HDFSEndpoint> hdfsEndpoint, 
                Pair<Identifier, KafkaEndpoint> kafkaEndpoint) {
            this.diskEndpoint = diskEndpoint;
            this.hdfsEndpoint = hdfsEndpoint;
            this.kafkaEndpoint = kafkaEndpoint;
        }
    }
    
    public static class ExtendedDetails {
        public final Map<String, DiskResource> diskDetails;
        public final Map<String, HDFSResource> hdfsDetails;
        public final Map<String, KafkaResource> kafkaDetails;
        
        public ExtendedDetails(Map<String, DiskResource> diskDetails, Map<String, HDFSResource> hdfsDetails, 
                Map<String, KafkaResource> kafkaDetails) {
            this.diskDetails = diskDetails;
            this.hdfsDetails = hdfsDetails;
            this.kafkaDetails = kafkaDetails;
        }
        
        public static ExtendedDetails getDisk(Map<String, HDFSResource> diskResources) {
            Map<String, DiskResource> converted = new HashMap<>();
            for(Map.Entry<String, HDFSResource> r : diskResources.entrySet()) {
                converted.put(r.getKey(), new DiskResource(r.getValue().dirPath, r.getValue().fileName));
            }
            return new ExtendedDetails(converted, null, null);
        }
        
        public static ExtendedDetails getHDFS(Map<String, HDFSResource> hdfsResources, Map<String, KafkaResource> kafkaResource) {
            return new ExtendedDetails(null, hdfsResources, kafkaResource);
        }
     }
}
