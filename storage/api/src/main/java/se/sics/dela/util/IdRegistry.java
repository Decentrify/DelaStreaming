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
package se.sics.dela.util;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import se.sics.kompics.util.Identifier;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class IdRegistry {

  private final MyIdentifierFactory idFactory;
  private final BiMap<String, Identifier> nameToId = HashBiMap.create();
  
  
  public IdRegistry(MyIdentifierFactory idFactory) {
    this.idFactory = idFactory;
  }

  public boolean registered(String endpointName) {
    return nameToId.containsKey(endpointName);
  }

  public Identifier register(String endpointName) {
    Identifier id = nameToId.get(endpointName);
    if (id != null) {
      return id;
    }
    Identifier endpointId = idFactory.next();
    nameToId.put(endpointName, endpointId);
    return endpointId;
  }
  
  public Identifier lookup(String endpointName) {
    return nameToId.get(endpointName);
  }
}
