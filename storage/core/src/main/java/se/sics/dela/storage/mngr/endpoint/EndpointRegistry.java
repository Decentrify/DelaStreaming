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
import java.util.function.Consumer;
import org.javatuples.Pair;
import org.javatuples.Triplet;
import se.sics.dela.storage.mngr.StorageProvider;
import se.sics.dela.util.IdRegistry;
import se.sics.dela.util.MyIntIdFactory;
import se.sics.kompics.util.Identifier;
import se.sics.ktoolbox.util.identifiable.IdentifierFactory;
import se.sics.ktoolbox.util.identifiable.basic.IntIdFactory;

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
  final Consumer<Triplet<Identifier, Identifier, StorageProvider>> connectAction;
  final Consumer<Pair<Identifier, Identifier>> disconnectAction;
  final Map<Identifier, StorageProvider> providers = new HashMap<>();
  final IdentifierFactory idFactory;
  
  final Set<Identifier> connecting = new HashSet<>();
  final Set<Identifier> disconnecting = new HashSet<>();
  
  //<endpointId, clientId, client>
  final Table<Identifier, Identifier, Client> appClients = HashBasedTable.create();
  //<endpointId,<clientId, callback>>
  final Map<Identifier, Pair<Identifier, Consumer<Pair<Identifier, Identifier>>>> disconnectedCallbacks
    = new HashMap<>();
  //<endpointId, proxyId>
  final BiMap<Identifier, Identifier> proxyClients = HashBiMap.create();
  final Set<Identifier> postponedConnect = new HashSet<>();

  public EndpointRegistry(IdentifierFactory idFactory, List<StorageProvider> providers,
    Consumer<Triplet<Identifier, Identifier, StorageProvider>> connectAction,
    Consumer<Pair<Identifier, Identifier>> disconnectAction) {
    this.idFactory = idFactory;
    this.connectAction = connectAction;
    this.disconnectAction = disconnectAction;
    this.idRegistry = new IdRegistry(new MyIntIdFactory(new IntIdFactory(null), 0));
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
    Identifier proxyId = idFactory.randomId();
    StorageProvider provider = providers.get(endpointId);
    connectAction.accept(Triplet.with(proxyId, endpointId, provider));
    proxyClients.put(endpointId, proxyId);
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
   */
  public void disconnect(Identifier clientId, Identifier endpointId,
    Consumer<Pair<Identifier, Identifier>> disconnectedCallback) {
    Client client = appClients.remove(endpointId, clientId);
    if (client != null && appClients.row(endpointId).isEmpty()) {
      Identifier proxyId = proxyClients.get(endpointId);
      disconnectAction.accept(Pair.with(proxyId, endpointId));
      disconnecting.add(endpointId);
      disconnectedCallbacks.put(endpointId, Pair.with(clientId, disconnectedCallback));
    } else {
      disconnectedCallback.accept(Pair.with(clientId, endpointId));
    }
  }

  public void disconnected(Identifier proxyId, Identifier endpointId) {
    disconnecting.remove(endpointId);
    proxyClients.remove(endpointId);
    Pair<Identifier, Consumer<Pair<Identifier, Identifier>>> disconnectedCallback
      = disconnectedCallbacks.remove(endpointId);
    if (disconnectedCallback != null) {
      Identifier clientId = disconnectedCallback.getValue0();
      disconnectedCallback.getValue1().accept(Pair.with(clientId, endpointId));
    }
    if (postponedConnect.remove(endpointId)) {
      proxyConnect(endpointId);
    }
  }

  public static class ClientBuilder {

    public final Identifier clientId;
    public final Consumer<Pair<Identifier, Identifier>> connectedCallback;
    public final Consumer<Triplet<Identifier, Identifier, Throwable>> connectionFailedCallback;

    public ClientBuilder(Identifier clientId,
      Consumer<Pair<Identifier, Identifier>> connectedCallback,
      Consumer<Triplet<Identifier, Identifier, Throwable>> connectionFailedCallback) {
      this.clientId = clientId;
      this.connectedCallback = connectedCallback;
      this.connectionFailedCallback = connectionFailedCallback;
    }

    public Client build(Identifier endpointId) {
      return new Client(clientId, endpointId, connectedCallback, connectionFailedCallback);
    }
  }
  
  public static class Client {
    public final Identifier clientId;
    public final Identifier endpointId;
    public final Consumer<Pair<Identifier, Identifier>> connectedCallback;
    public final Consumer<Triplet<Identifier, Identifier, Throwable>> connectionFailedCallback;

    public Client(Identifier clientId, Identifier endpointId,
      Consumer<Pair<Identifier, Identifier>> connectedCallback,
      Consumer<Triplet<Identifier, Identifier, Throwable>> connectionFailedCallback) {
      this.clientId = clientId;
      this.endpointId = endpointId;
      this.connectedCallback = connectedCallback;
      this.connectionFailedCallback = connectionFailedCallback;
    }

    public void connected() {
      connectedCallback.accept(Pair.with(clientId, endpointId));
    }

    public void connectionFailed(Throwable cause) {
      connectionFailedCallback.accept(Triplet.with(clientId, endpointId, cause));
    }
  }
}
