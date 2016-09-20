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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import se.sics.ktoolbox.util.identifiable.Identifier;
import se.sics.ktoolbox.util.identifiable.basic.IntIdentifier;
import se.sics.nstream.hops.hdfs.HDFSControlPort;
import se.sics.nstream.hops.hdfs.HDFSPort;
import se.sics.nstream.hops.kafka.KafkaControlPort;
import se.sics.nstream.hops.kafka.KafkaPort;
import se.sics.nstream.storage.StorageControlPort;
import se.sics.nstream.storage.StoragePort;
import se.sics.nstream.torrent.StorageProvider;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class HopsStorageProvider implements StorageProvider {
    public static final Identifier hdfsIdentifier = new IntIdentifier(1);
    public static final Identifier kafkaIdentifier = new IntIdentifier(2);
    
    @Override
    public List<Class<? extends StoragePort>> requiredStoragePorts() {
        List ports = new ArrayList();
        ports.add(HDFSPort.class);
        ports.add(KafkaPort.class);
        return ports;
    }

    @Override
    public Map<Identifier, Class<? extends StorageControlPort>> requiredStorageControlPorts() {
        Map<Identifier, Class<? extends StorageControlPort>> ctrlPorts = new HashMap<>();
        ctrlPorts.put(hdfsIdentifier, HDFSControlPort.class);
        ctrlPorts.put(kafkaIdentifier, KafkaControlPort.class);
        return ctrlPorts;
    }
}
