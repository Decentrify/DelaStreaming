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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import junit.framework.Assert;
import org.javatuples.Pair;
import org.junit.After;
import org.junit.Test;
import org.slf4j.Logger;
import se.sics.dela.storage.StorageEndpoint;
import se.sics.dela.storage.StorageResource;
import se.sics.dela.storage.mngr.StorageProvider;
import se.sics.kompics.util.Identifier;
import se.sics.ktoolbox.util.TupleHelper;
import se.sics.ktoolbox.util.identifiable.BasicBuilders;
import se.sics.ktoolbox.util.identifiable.basic.IntIdFactory;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class EndpointRegistryTest {

  IntIdFactory idFactory = new IntIdFactory(new Random(123));
  List<Pair<States, Identifier>> timeline = new ArrayList<>();
  Map<Identifier, Identifier> endpointProxyClients = new HashMap<>();
  TupleHelper.TripletConsumer<Identifier, Identifier, StorageProvider> connectAction
    = new TupleHelper.TripletConsumer<Identifier, Identifier, StorageProvider>() {
    @Override
    public void accept(Identifier clientId, Identifier endpointId, StorageProvider provider) {
      timeline.add(Pair.with(States.CONNECT, endpointId));
      endpointProxyClients.put(endpointId, clientId);
    }
  };
  TupleHelper.PairConsumer<Identifier, Identifier> disconnectAction
    = new TupleHelper.PairConsumer<Identifier, Identifier>() {
    @Override
    public void accept(Identifier clientId, Identifier endpointId) {
      timeline.add(Pair.with(States.DISCONNECT, endpointId));
    }
  };

  IntIdFactory intIdFactory = new IntIdFactory(null);
  Identifier client1Id = intIdFactory.id(new BasicBuilders.IntBuilder(1));
  Identifier client2Id = intIdFactory.id(new BasicBuilders.IntBuilder(2));
  Identifier client3Id = intIdFactory.id(new BasicBuilders.IntBuilder(3));

  String endpoint1Name = "provider1";
  List<StorageProvider> providers = new ArrayList<>();

  {
    providers.add(storageProvider(endpoint1Name));
  }

  @After
  public void afterTest() {
    timeline.clear();
  }

  @Test
  public void test1() {
    EndpointRegistry registry = new EndpointRegistry(idFactory, providers, connectAction, disconnectAction);
    Identifier endpoint1Id = registry.idRegistry.lookup(endpoint1Name);

    ResultCallback connected1 = new ResultCallback();
    ResultCallback disconnected1 = new ResultCallback();
    EndpointRegistry.Client client1_1 = newClient(client1Id, endpoint1Id, connected1, disconnected1);
    ResultCallback connected2 = new ResultCallback();
    ResultCallback disconnected2 = new ResultCallback();
    EndpointRegistry.Client client2_1 = newClient(client2Id, endpoint1Id, connected2, disconnected2);
    ResultCallback connected3 = new ResultCallback();
    ResultCallback disconnected3 = new ResultCallback();
    EndpointRegistry.Client client3_1 = newClient(client3Id, endpoint1Id, connected3, disconnected3);
    
    Pair[] expectedTimeline;

    delayedConnect(registry, client1_1, connected1);
    connected(registry, endpoint1Id, connected1);
    autoConnect(registry, client2_1, connected2);
    autoDisconnect(registry, client1_1, disconnected1);
    //timeline check
    expectedTimeline = new Pair[]{
      Pair.with(States.CONNECT, endpoint1Id)
    };
    sameTimeline(expectedTimeline, timeline);
    autoConnect(registry, client3_1, connected3);
    autoDisconnect(registry, client2_1, disconnected2);
    delayedDisconnect(registry, client3_1, disconnected3);
    disconnected(registry, endpoint1Id, disconnected3);
    expectedTimeline = new Pair[]{
      Pair.with(States.CONNECT, endpoint1Id),
      Pair.with(States.DISCONNECT, endpoint1Id)
    };
    sameTimeline(expectedTimeline, timeline);
    checkCleanRegistry(registry);
  }

  @Test
  public void test2() {
    EndpointRegistry registry = new EndpointRegistry(idFactory, providers, connectAction, disconnectAction);
    Identifier endpoint1Id = registry.idRegistry.lookup(endpoint1Name);

    ResultCallback connected1 = new ResultCallback();
    ResultCallback disconnected1 = new ResultCallback();
    EndpointRegistry.Client client1_1 = newClient(client1Id, endpoint1Id, connected1, disconnected1);
    ResultCallback connected2 = new ResultCallback();
    ResultCallback disconnected2 = new ResultCallback();
    EndpointRegistry.Client client2_1 = newClient(client2Id, endpoint1Id, connected2, disconnected2);
    ResultCallback connected3 = new ResultCallback();
    ResultCallback disconnected3 = new ResultCallback();
    EndpointRegistry.Client client3_1 = newClient(client3Id, endpoint1Id, connected3, disconnected3);
    
    Pair[] expectedTimeline;

    delayedConnect(registry, client1_1, connected1);
    delayedConnect(registry, client2_1, connected2);
    connected(registry, endpoint1Id, connected1, connected2);
    autoDisconnect(registry, client1_1, disconnected1);
    delayedDisconnect(registry, client2_1, disconnected2);
    disconnected(registry, endpoint1Id, disconnected2);
    expectedTimeline = new Pair[]{
      Pair.with(States.CONNECT, endpoint1Id),
      Pair.with(States.DISCONNECT, endpoint1Id),};
    sameTimeline(expectedTimeline, timeline);
    checkCleanRegistry(registry);
  }

  private void checkCleanRegistry(EndpointRegistry registry) {
    Assert.assertTrue(registry.appClients.isEmpty());
    Assert.assertTrue(registry.connecting.isEmpty());
    Assert.assertTrue(registry.disconnecting.isEmpty());
    Assert.assertTrue(registry.slowDiscClients.isEmpty());
    Assert.assertTrue(registry.postponedConnect.isEmpty());
    Assert.assertTrue(registry.clientProxies.isEmpty());
  }

  private void delayedConnect(EndpointRegistry registry, EndpointRegistry.Client client, ResultCallback connected) {
    Assert.assertFalse(connected.completed);
    registry.connect(client);
    Assert.assertFalse(connected.completed);
  }

  private void autoConnect(EndpointRegistry registry, EndpointRegistry.Client client, ResultCallback connected) {
    Assert.assertFalse(connected.completed);
    registry.connect(client);
    Assert.assertTrue(connected.completed);
  }

  private void connected(EndpointRegistry registry, Identifier endpointId, ResultCallback... result) {
    registry.connected(endpointId);
    Arrays.asList(result).forEach((connected) -> Assert.assertTrue(connected.completed));
  }

  private void delayedDisconnect(EndpointRegistry registry, EndpointRegistry.Client client,
    ResultCallback disconnected) {
    Assert.assertFalse(disconnected.completed);
    registry.disconnect(client.clientId, client.endpointId);
    Assert.assertFalse(disconnected.completed);
  }

  private void autoDisconnect(EndpointRegistry registry, EndpointRegistry.Client client, ResultCallback disconnected) {
    Assert.assertFalse(disconnected.completed);
    registry.disconnect(client.clientId, client.endpointId);
    Assert.assertTrue(disconnected.completed);
  }

  private void disconnected(EndpointRegistry registry, Identifier endpointId, ResultCallback disconnected) {
    endpointProxyClients.remove(endpointId);
    registry.disconnected(endpointId);
    Assert.assertTrue(disconnected.completed);
  }

  private void sameTimeline(Pair<States, Identifier>[] expectedTimeline, List<Pair<States, Identifier>> timeline) {
    Assert.assertEquals(expectedTimeline.length, timeline.size());
    Iterator<Pair<States, Identifier>> etIt = Arrays.asList(expectedTimeline).iterator();
    Iterator<Pair<States, Identifier>> tIt = timeline.iterator();
    while (etIt.hasNext()) {
      Assert.assertEquals(etIt.next(), tIt.next());
    }
  }

  private EndpointRegistry.Client newClient(Identifier clientId, Identifier endpointId,
    ResultCallback connectedCallback, ResultCallback disconnectedCallback) {
    EndpointRegistry.Client client
      = new EndpointRegistry.ClientBuilder(clientId, connectedCallback, null, disconnectedCallback)
        .build(endpointId);
    return client;
  }

  private StorageProvider storageProvider(String endpointName) {
    StorageEndpoint endpoint = new StorageEndpoint() {
      @Override
      public String getEndpointName() {
        return endpointName;
      }
    };
    return new StorageProvider() {
      @Override
      public String getName() {
        return endpoint.getEndpointName();
      }

      @Override
      public StorageEndpoint getEndpoint() {
        return endpoint;
      }

      @Override
      public Class getStorageDefinition() {
        throw new UnsupportedOperationException("Not supported yet.");
      }

      @Override
      public Pair initiate(StorageResource resource, Logger logger) {
        throw new UnsupportedOperationException("Not supported yet.");
      }
    };

  }

  public static class ResultCallback extends TupleHelper.PairConsumer<Identifier, Identifier> {

    private boolean completed = false;

    @Override
    public void accept(Identifier clientId, Identifier endpointId) {
      completed = true;
    }

    public void reset() {
      completed = false;
    }
  }

  public static enum States {
    CONNECT,
    DISCONNECT
  }
}
