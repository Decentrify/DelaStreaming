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
package se.sics.nstream.hops.libmngr;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.javatuples.Pair;
import se.sics.ktoolbox.util.identifiable.Identifier;
import se.sics.nstream.storage.durable.util.StreamEndpoint;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class EndpointRegistration {

  private final Map<String, Identifier> nameToId = new HashMap<>();
  private final Map<Identifier, StreamEndpoint> endpoints = new HashMap<>();
  private final Set<Identifier> connecting = new HashSet<>();
  
  private final Set<Identifier> selfCleaning = new HashSet<>();

  public void addWaiting(String endpointName, Identifier endpointId, StreamEndpoint endpoint) {
    nameToId.put(endpointName, endpointId);
    endpoints.put(endpointId, endpoint);
    connecting.add(endpointId);
  }

  public boolean isComplete() {
    return connecting.isEmpty();
  }
  
  public Pair<Map<String, Identifier>, Map<Identifier, StreamEndpoint>> getSetup() {
    return Pair.with(nameToId, endpoints);
  }

  public void connected(Identifier endpointId) {
    connecting.remove(endpointId);
  }
  
  public Identifier nameToId(String endpointName) {
    return nameToId.get(endpointName);
  }
  
  public Set<Identifier> selfCleaning() {
    selfCleaning.addAll(endpoints.keySet());
    selfCleaning.addAll(connecting);
    return selfCleaning;
  }
  
  public void cleaned(Identifier endpointId) {
    selfCleaning.remove(endpointId);
  }
  
  public boolean cleaningComplete() {
    return selfCleaning.isEmpty();
  }
}
