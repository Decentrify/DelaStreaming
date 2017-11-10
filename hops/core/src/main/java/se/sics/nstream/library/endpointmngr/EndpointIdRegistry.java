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

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import se.sics.kompics.util.Identifier;
import se.sics.ktoolbox.util.identifiable.basic.IntIdFactory;

/**
 * TODO Alex - nameToId is memory leak, move to EndpointManager
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class EndpointIdRegistry {

  private final IntIdFactory storageIdFactory;
  private final BiMap<String, Identifier> nameToId = HashBiMap.create();
  private int id = 0;
  private final Map<Identifier, AtomicInteger> idRef = new HashMap<>();
  
  public EndpointIdRegistry() {
    storageIdFactory = new IntIdFactory(null); //no random ids
  }

  public boolean registered(String endpointName) {
    return nameToId.containsKey(endpointName);
  }

  public Identifier register(String endpointName) {
    if (registered(endpointName)) {
      throw new RuntimeException("already registered - logic exception");
    }
    Identifier endpointId = storageIdFactory.rawId(id++);
    nameToId.put(endpointName, endpointId);
    idRef.put(endpointId, new AtomicInteger(0));
    return endpointId;
  }

  public Identifier lookup(String endpointName) {
    return nameToId.get(endpointName);
  }
  
  public void use(Identifier endpointId) {
    idRef.get(endpointId).incrementAndGet();
  }
  
  public void release(Identifier endpointId) {
    int refCount = idRef.get(endpointId).decrementAndGet();
    if(refCount == 0) {
      nameToId.inverse().remove(endpointId);
      idRef.remove(endpointId);
    }
  }
}
