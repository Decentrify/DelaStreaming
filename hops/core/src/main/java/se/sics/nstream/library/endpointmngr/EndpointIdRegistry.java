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
package se.sics.nstream.library.endpointmngr;

import java.util.HashMap;
import java.util.Map;
import se.sics.ktoolbox.util.identifiable.Identifier;
import se.sics.ktoolbox.util.identifiable.basic.IntIdFactory;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class EndpointIdRegistry {

    private final IntIdFactory storageIdFactory;
    private final Map<String, Identifier> nameToId = new HashMap<>();
    private int id = 0;
    
    public EndpointIdRegistry() {
        storageIdFactory = new IntIdFactory(null); //no random ids
    }
    
    public boolean registered(String endpointName) {
        return nameToId.containsKey(endpointName);
    }
    
    public Identifier register(String endpointName) {
        if(registered(endpointName)) {
            throw new RuntimeException("already registered - logic exception");
        }
        Identifier endpointId = storageIdFactory.rawId(id++);
        nameToId.put(endpointName, endpointId);
        return endpointId;
    }
    
    public Identifier lookup(String endpointName) {
        return nameToId.get(endpointName);
    }
}
