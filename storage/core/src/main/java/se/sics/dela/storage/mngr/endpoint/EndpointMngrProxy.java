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
import org.slf4j.Logger;
import se.sics.dela.storage.mngr.StorageProvider;
import se.sics.dela.storage.mngr.endpoint.EndpointRegistry.ClientBuilder;
import se.sics.dela.storage.mngr.endpoint.events.EndpointMngrConnect;
import se.sics.dela.storage.mngr.endpoint.events.EndpointMngrDisconnect;
import se.sics.dela.util.MyIdentifierFactory;
import se.sics.kompics.ComponentProxy;
import se.sics.kompics.Handler;
import se.sics.kompics.Positive;
import se.sics.kompics.util.Identifier;
import se.sics.ktoolbox.util.TupleHelper;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class EndpointMngrProxy {

  private ComponentProxy proxy;
  private Logger logger;
  private Positive<EndpointMngrPort> mngrPort;

  public final EndpointRegistry registry;
  private final BiMap<Identifier, Identifier> eventToEndpoint = HashBiMap.create();

  public EndpointMngrProxy(MyIdentifierFactory idFactory, List<StorageProvider> providers) {
    this.registry = new EndpointRegistry(idFactory, providers, connectAction, disconnectAction);
  }

  public EndpointMngrProxy setup(ComponentProxy proxy, Logger logger) {
    this.proxy = proxy;
    this.logger = logger;
    this.mngrPort = proxy.getNegative(EndpointMngrPort.class).getPair();
    proxy.subscribe(handleConnected, mngrPort);
    proxy.subscribe(handleConnectFail, mngrPort);
    proxy.subscribe(handleDisconnected, mngrPort);
    return this;
  }

  public Identifier connectEndpoint(String endpointName, ClientBuilder client) {
    Identifier endpointId = registry.idRegistry.lookup(endpointName);
    registry.connect(client.build(endpointId));
    return endpointId;
  }

  public void disconnectEndpoint(Identifier clientId, Identifier endpointId) {
    registry.disconnect(clientId, endpointId);
  }

  TupleHelper.TripletConsumer<Identifier, Identifier, StorageProvider> connectAction
    = new TupleHelper.TripletConsumer<Identifier, Identifier, StorageProvider>() {
    @Override
    public void accept(Identifier clientId, Identifier endpointId, StorageProvider provider) {
      logger.debug("mngr proxy - endpoint:{} client:{} - connecting provider:{}", 
        new Object[]{endpointId, clientId, provider.getName()});
      EndpointMngrConnect.Request req = new EndpointMngrConnect.Request(clientId, endpointId, provider);
      proxy.trigger(req, mngrPort);
      eventToEndpoint.put(req.getId(), endpointId);
    }
  };

  TupleHelper.PairConsumer<Identifier, Identifier> disconnectAction
    = new TupleHelper.PairConsumer<Identifier, Identifier>() {
    @Override
    public void accept(Identifier clientId, Identifier endpointId) {
      logger.debug("mngr proxy - endpoint:{} client:{} - disconnecting", new Object[]{endpointId, clientId});
      EndpointMngrDisconnect.Request req = new EndpointMngrDisconnect.Request(clientId, endpointId);
      proxy.trigger(req, mngrPort);
      eventToEndpoint.put(req.getId(), endpointId);
    }
  };

  Handler handleConnected = new Handler<EndpointMngrConnect.Success>() {
    @Override
    public void handle(EndpointMngrConnect.Success resp) {
      logger.debug("mngr proxy - endpoint:{} client:{} - connected", 
        new Object[]{resp.req.endpointId, resp.req.clientId});
      registry.connected(resp.getEndpointId());
      eventToEndpoint.remove(resp.getId());
    }
  };

  Handler handleConnectFail = new Handler<EndpointMngrConnect.Failure>() {
    @Override
    public void handle(EndpointMngrConnect.Failure resp) {
      logger.debug("mngr proxy - endpoint:{} client:{} - connect failed", 
        new Object[]{resp.req.endpointId, resp.req.clientId});
      eventToEndpoint.remove(resp.getId());
      throw new RuntimeException(resp.cause);
    }
  };

  Handler handleDisconnected = new Handler<EndpointMngrDisconnect.Success>() {
    @Override
    public void handle(EndpointMngrDisconnect.Success resp) {
      logger.debug("mngr proxy - endpoint:{} client:{} - disconnected", 
        new Object[]{resp.req.endpointId, resp.req.clientId});
      registry.disconnected(resp.getEndpointId());
      eventToEndpoint.remove(resp.getId());
    }
  };
}
