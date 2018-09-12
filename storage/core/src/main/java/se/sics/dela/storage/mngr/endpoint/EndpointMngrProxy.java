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
 * GNU General Public License for more defLastBlock.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
 */
package se.sics.dela.storage.mngr.endpoint;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import java.util.List;
import java.util.function.Consumer;
import org.javatuples.Pair;
import org.javatuples.Triplet;
import se.sics.dela.storage.mngr.StorageProvider;
import se.sics.dela.storage.mngr.endpoint.EndpointRegistry.ClientBuilder;
import se.sics.dela.storage.mngr.endpoint.events.EndpointMngrConnect;
import se.sics.dela.storage.mngr.endpoint.events.EndpointMngrDisconnect;
import se.sics.kompics.ComponentProxy;
import se.sics.kompics.Handler;
import se.sics.kompics.Positive;
import se.sics.kompics.util.Identifier;
import se.sics.ktoolbox.util.TupleHelper;
import se.sics.ktoolbox.util.identifiable.IdentifierFactory;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class EndpointMngrProxy {


  private ComponentProxy proxy;
  private Positive<EndpointMngrPort> mngrPort;

  private final EndpointRegistry registry;
  private final BiMap<Identifier, Identifier> eventToEndpoint = HashBiMap.create();

  public EndpointMngrProxy(IdentifierFactory idFactory, List<StorageProvider> providers) {
    this.registry = new EndpointRegistry(idFactory, providers, connectAction, disconnectAction);
  }

  private void setup(ComponentProxy proxy) {
    this.proxy = proxy;
    this.mngrPort = proxy.getPositive(EndpointMngrPort.class);
    proxy.subscribe(handleConnected, mngrPort);
    proxy.subscribe(handleConnectFail, mngrPort);
    proxy.subscribe(handleDisconnected, mngrPort);
  }

  public void connectEndpoint(String endpointName, ClientBuilder client) {
    Identifier endpointId = registry.idRegistry.lookup(endpointName);
    registry.connect(client.build(endpointId));
  }

  public void disconnectEndpoint(Identifier clientId, Identifier endpointId,
    Consumer<Pair<Identifier, Identifier>> callback) {
    registry.disconnect(clientId, endpointId, callback);
  }

  Consumer<Triplet<Identifier, Identifier, StorageProvider>> connectAction
    = new TupleHelper.TripletConsumer<Identifier, Identifier, StorageProvider>() {
    @Override
    public void accept(Identifier clientId, Identifier endpointId, StorageProvider provider) {
      EndpointMngrConnect.Request req = new EndpointMngrConnect.Request(clientId, endpointId, provider);
      proxy.trigger(req, mngrPort);
      eventToEndpoint.put(req.getId(), endpointId);
    }
  };
  
  Consumer<Pair<Identifier, Identifier>> disconnectAction = new TupleHelper.PairConsumer<Identifier, Identifier>() {
    @Override
    public void accept(Identifier clientId, Identifier endpointId) {
      EndpointMngrDisconnect.Request req = new EndpointMngrDisconnect.Request(clientId, endpointId);
      proxy.trigger(req, mngrPort);
      eventToEndpoint.put(req.getId(), endpointId);
    }
  };

  Handler handleConnected = new Handler<EndpointMngrConnect.Success>() {
    @Override
    public void handle(EndpointMngrConnect.Success resp) {
      registry.connected(resp.getEndpointId());
    }
  };

  Handler handleConnectFail = new Handler<EndpointMngrConnect.Failure>() {
    @Override
    public void handle(EndpointMngrConnect.Failure resp) {
      throw new RuntimeException(resp.cause);
    }
  };

  Handler handleDisconnected = new Handler<EndpointMngrDisconnect.Success>() {
    @Override
    public void handle(EndpointMngrDisconnect.Success resp) {
      registry.disconnected(resp.getClientId(), resp.getEndpointId());
    }
  };
}
