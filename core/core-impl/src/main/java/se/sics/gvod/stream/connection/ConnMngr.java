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
package se.sics.gvod.stream.connection;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.javatuples.Pair;
import se.sics.gvod.common.util.VodDescriptor;
import se.sics.ktoolbox.util.identifiable.Identifier;
import se.sics.ktoolbox.util.network.KAddress;

/**
 *
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class ConnMngr {
    private final Map<Identifier, KAddress> connections = new HashMap<>();
    private final Map<Identifier, VodDescriptor> descriptors = new HashMap<>();
    
    public void addConn(List<KAddress> conn) {
        for(KAddress partner : conn) {
            connections.put(partner.getId(), partner);
            descriptors.put(partner.getId(), new VodDescriptor(-1));
        }
    }
    
    public Pair<Map, Map> publish() {
        return Pair.with((Map)new HashMap(connections), (Map)new HashMap(descriptors));
    }
}
