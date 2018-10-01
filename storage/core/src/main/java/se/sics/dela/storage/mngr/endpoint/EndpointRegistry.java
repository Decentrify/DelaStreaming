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
package se.sics.dela.storage.mngr.endpoint;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.Table;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import se.sics.dela.storage.mngr.StorageProvider;
import se.sics.dela.util.IdRegistry;
import se.sics.dela.util.MyIdentifierFactory;
import se.sics.kompics.util.Identifier;
import se.sics.ktoolbox.util.TupleHelper;
import se.sics.ktoolbox.util.identifiable.BasicIdentifiers;

/**
 * It is expected that the requester will do one request at a time:
 * 1. a connect request and then wait for connected indication
 * 2. a disconnect request and then wait for disconnected indication
 * This condition ensures we never have connecting followed by disconnecting (ensured by original connect requester)
 * We can only have disconnected -> connecting -> connected -> disconnecting
 * However while disconnecting someone could ask to connect.
 * Connect/disconnect can be complex actions involving multiple external messages.
 * We choose not to overlap a disconnecting with a connecting and instead wait for disconnected and postpone the connect
 * Enforced flow:
 * disconnected -> connecting -> connected -> disconnecting -> disconnected
 *
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class EndpointRegistry {

  public final IdRegistry idRegistry;
  final TupleHelper.TripletConsumer<Identifier, Identifier, StorageProvider> connectAction;
  final TupleHelper.PairConsumer<Identifier, Identifier> disconnectAction;
  final Map<Identifier, StorageProvider> providers = new HashMap<>();

  final Set<Identifier> connecting = new HashSet<>();
  final Set<Identifier> disconnecting = new HashSet<>();

  //<endpointId, clientId, client>
  final Table<Identifier, Identifier, Client> appClients = HashBasedTable.create();
  final Map<Identifier, Client> slowDiscClients = new HashMap<>();
  //<endpointId, clientProxyId>
  final BiMap<Identifier, Identifier> clientProxies = HashBiMap.create();
  final Set<Identifier> postponedConnect = new HashSet<>();

  public EndpointRegistry(MyIdentifierFactory idFactory, List<StorageProvider> providers,
    TupleHelper.TripletConsumer<Identifier, Identifier, StorageProvider> connectAction,
    TupleHelper.PairConsumer<Identifier, Identifier> disconnectAction) {
    this.connectAction = connectAction;
    this.disconnectAction = disconnectAction;
    this.idRegistry = new IdRegistry(idFactory);
    providers.forEach((provider) -> {
      Identifier endpointId = idRegistry.register(provider.getName());
      this.providers.put(endpointId, provider);
    });
  }

  public void connect(Client client) {
    /**
     * endpoint references get removed on disconnected when we do a full cleanup
     * a null endpoint reference means we can assume full cleanup for this endpoint
     */
    if (appClients.row(client.endpointId).isEmpty()) {
      proxyConnect(client.endpointId);
    } else {
      if (disconnecting.contains(client.endpointId)) {
        postponedConnect.add(client.endpointId);
      } else if (connecting.contains(client.endpointId)) {
        //nothing
      } else {
        client.connected();
      }
    }
    appClients.put(client.endpointId, client.clientId, client);
  }

  private void proxyConnect(Identifier endpointId) {
    connecting.add(endpointId);
    Identifier proxyId = BasicIdentifiers.eventId();
    StorageProvider provider = providers.get(endpointId);
    connectAction.accept(proxyId, endpointId, provider);
    clientProxies.put(endpointId, proxyId);
  }

  public void connected(Identifier endpointId) {
    connecting.remove(endpointId);
    appClients.row(endpointId).values().forEach((client) -> client.connected());
  }

  /**
   * If no reference counter - do nothing.
   * Decrement reference counter
   * If reference counter is 0 - initiate disconnect action
   * Otherwise - do nothing - someone else is still using the endpoint
   * return false - nothing to disconnect - true - disconnecting
   */
  public boolean disconnect(Identifier clientId, Identifier endpointId) {
    Client client = appClients.remove(endpointId, clientId);
    if (client != null) {
      if (appClients.row(endpointId).isEmpty()) {
        Identifier clientProxyId = clientProxies.get(endpointId);
        disconnectAction.accept(clientProxyId, endpointId);
        disconnecting.add(endpointId);
        slowDiscClients.put(endpointId, client);
        return true;
      } else {
        client.disconnected();
      }
      return true;
    } else {
      return false;
    }
  }

  public void disconnected(Identifier endpointId) {
    disconnecting.remove(endpointId);
    clientProxies.remove(endpointId);
    Client disconnectedCallback = slowDiscClients.remove(endpointId);
    if (disconnectedCallback != null) {
      disconnectedCallback.disconnected();
    }
    if (postponedConnect.remove(endpointId)) {
      proxyConnect(endpointId);
    }
  }

  public static class ClientBuilder {

    public final Identifier clientId;
    public final TupleHelper.PairConsumer<Identifier, Identifier> connectedCallback;
    public final TupleHelper.TripletConsumer<Identifier, Identifier, Throwable> connectionFailedCallback;
    public final TupleHelper.PairConsumer<Identifier, Identifier> disconnectedCallback;

    public ClientBuilder(Identifier clientId,
      TupleHelper.PairConsumer<Identifier, Identifier> connectedCallback,
      TupleHelper.TripletConsumer<Identifier, Identifier, Throwable> connectionFailedCallback,
      TupleHelper.PairConsumer<Identifier, Identifier> disconnectedCallback) {
      this.clientId = clientId;
      this.connectedCallback = connectedCallback;
      this.connectionFailedCallback = connectionFailedCallback;
      this.disconnectedCallback = disconnectedCallback;
    }

    public Client build(Identifier endpointId) {
      return new Client(clientId, endpointId, connectedCallback, connectionFailedCallback, disconnectedCallback);
    }
  }

  public static class Client {

    public final Identifier clientId;
    public final Identifier endpointId;
    public final TupleHelper.PairConsumer<Identifier, Identifier> connectedCallback;
    public final TupleHelper.TripletConsumer<Identifier, Identifier, Throwable> connectionFailedCallback;
    public final TupleHelper.PairConsumer<Identifier, Identifier> disconnectedCallback;

    public Client(Identifier clientId, Identifier endpointId,
      TupleHelper.PairConsumer<Identifier, Identifier> connectedCallback,
      TupleHelper.TripletConsumer<Identifier, Identifier, Throwable> connectionFailedCallback,
      TupleHelper.PairConsumer<Identifier, Identifier> disconnectedCallback) {
      this.clientId = clientId;
      this.endpointId = endpointId;
      this.connectedCallback = connectedCallback;
      this.connectionFailedCallback = connectionFailedCallback;
      this.disconnectedCallback = disconnectedCallback;
    }

    public void connected() {
      connectedCallback.accept(clientId, endpointId);
    }

    public void connectionFailed(Throwable cause) {
      connectionFailedCallback.accept(clientId, endpointId, cause);
    }

    public void disconnected() {
      disconnectedCallback.accept(clientId, endpointId);
    }
  }
}
