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
import se.sics.ktoolbox.util.identifiable.Identifier;
import se.sics.ktoolbox.util.identifiable.basic.IntIdFactory;
import se.sics.nstream.storage.durable.util.StreamEndpoint;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class StorageRegistry {

    private final IntIdFactory storageIdFactory;
    private final Map<String, Identifier> nameToId = new HashMap<>();
    private final Map<Identifier, Op> endpoints = new HashMap<>();
    private int id = 0;

    public StorageRegistry() {
        storageIdFactory = new IntIdFactory(null); //no random ids
    }

    public Result preRegister(StreamEndpoint endpoint) {
        Identifier endpointId = nameToId.get(endpoint.getEndpointName());
        if (endpointId == null) {
            endpointId = storageIdFactory.rawId(id++);
            nameToId.put(endpoint.getEndpointName(), endpointId);
            endpoints.put(endpointId, new Op(endpointId, endpoint));
            return new Result(endpointId, endpoint, false);
        }
        return new Result(endpointId, endpoint, true);
    }
    
    public void register(Identifier endpointId, ConnectedCallback callback) {
        Op op = endpoints.get(endpointId);
        op.setCallback(callback);
    }

    public void connected(Identifier endpointId) {
        Op op = endpoints.get(endpointId);
        if(op == null || op.connected()) {
            throw new RuntimeException("logic error");
        }
        op.connect();
    }
    
    public Pair<Identifier, StreamEndpoint> get(String endpointName) {
        Identifier endpointId = nameToId.get(endpointName);
        if(endpointId == null) {
            return null;
        }
        Op op = endpoints.get(endpointId);
        return Pair.with(endpointId, op.endpoint);
    }

    public static class Op {

        public final Identifier endpointId;
        public final StreamEndpoint endpoint;
        private ConnectedCallback callback;
        private boolean connected;

        public Op(Identifier endpointId, StreamEndpoint endpoint) {
            this.endpointId = endpointId;
            this.endpoint = endpoint;
            connected = false;
        }
        
        public void setCallback(ConnectedCallback callback) {
            this.callback = callback;
        }

        public void connect() {
            connected = true;
            callback.success(endpointId);
        }

        public boolean connected() {
            return connected;
        }
    }

    public static interface ConnectedCallback {

        public void success(Identifier endpointId);
    }
    
    public static class Result {
        public final Identifier endpointId;
        public final StreamEndpoint endpoint;
        public final boolean alreadyRegistered;
        
        public Result(Identifier endpointId, StreamEndpoint endpoint, boolean alreadyRegistered) {
            this.endpointId = endpointId;
            this.endpoint = endpoint;
            this.alreadyRegistered = alreadyRegistered;
        }
    }
}
