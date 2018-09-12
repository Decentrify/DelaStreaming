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

import se.sics.dela.storage.mngr.stream.StreamMngrPort;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import se.sics.dela.storage.mngr.StorageProvider;
import se.sics.dela.storage.mngr.endpoint.events.EndpointMngrConnect;
import se.sics.dela.storage.mngr.endpoint.events.EndpointMngrDisconnect;
import se.sics.dela.storage.mngr.stream.StreamMngrComp;
import se.sics.dela.storage.operation.StreamOpPort;
import se.sics.kompics.Channel;
import se.sics.kompics.Component;
import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.Fault;
import se.sics.kompics.Handler;
import se.sics.kompics.Kill;
import se.sics.kompics.Killed;
import se.sics.kompics.Negative;
import se.sics.kompics.Positive;
import se.sics.kompics.Start;
import se.sics.kompics.timer.Timer;
import se.sics.kompics.util.Identifier;
import se.sics.ktoolbox.util.network.ports.One2NChannel;
import se.sics.ktoolbox.util.result.Result;

/**
 *
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class StorageMngrComp extends ComponentDefinition {

  private final String logPrefix;

  private final Positive<Timer> timerPort = requires(Timer.class);
  private final Negative<StreamOpPort> streamOpPort = provides(StreamOpPort.class);
  private final Negative<StreamMngrPort> streamMngrPort = provides(StreamMngrPort.class);
  private final Negative<EndpointMngrPort> endpointMngrPort = provides(EndpointMngrPort.class);
  private final One2NChannel streamMngrChannel;
  private final One2NChannel streamOpChannel;
  //**************************************************************************
  private final Identifier self;
  //**************************************************************************
  private final Map<UUID, Identifier> compIdToEndpointId = new HashMap<>();
  private final Map<Identifier, Map<Identifier, EndpointMngrConnect.Request>> clients = new HashMap<>();
  private final Map<Identifier, Component> endpoints = new HashMap<>();

  public StorageMngrComp(Init init) {
    self = init.self;
    logPrefix = "<nid:" + self + ">";
    logger.info("{}initiating...", logPrefix);

    streamMngrChannel = One2NChannel.getChannel(logPrefix + ":dstream_control", streamMngrPort,
      new EndpointIdExtractor());
    streamOpChannel = One2NChannel.getChannel(logPrefix + ":dstorage", streamOpPort, new EndpointIdExtractor());

    subscribe(handleStart, control);
    subscribe(handleKilled, control);
    subscribe(handleConnect, endpointMngrPort);
    subscribe(handleDisconnect, endpointMngrPort);
  }

  Handler handleStart = new Handler<Start>() {
    @Override
    public void handle(Start event) {
      logger.info("{}starting", logPrefix);
    }
  };

  Handler handleKilled = new Handler<Killed>() {
    @Override
    public void handle(Killed event) {
      logger.info("{}killed endpoint component:{}", logPrefix, event.component.id());
    }
  };

  @Override
  public Fault.ResolveAction handleFault(Fault fault) {
    Identifier endpointId = compIdToEndpointId.remove(fault.getSource().id());
    Component endpointComp = endpoints.remove(endpointId);
    if (endpointComp != null) {
      reportFaulty(endpointId, fault.getCause(), endpointComp);
      trigger(Kill.event, endpointComp.control());
    }
    return Fault.ResolveAction.RESOLVED;
  }

  Handler handleConnect = new Handler<EndpointMngrConnect.Request>() {
    @Override
    public void handle(EndpointMngrConnect.Request req) {
      logger.info("{}connecting endpoint:{}", logPrefix, req.endpointId);
      if (!endpoints.containsKey(req.endpointId)) {
        Component endpoint = setupEndpoint(req.endpointId, req.endpointProvider);
        trigger(Start.event, endpoint.control());
      }
      setupClient(req);
    }
  };

  Handler handleDisconnect = new Handler<EndpointMngrDisconnect.Request>() {
    @Override
    public void handle(EndpointMngrDisconnect.Request req) {
      logger.info("{}disconnecting endpoint:{}", logPrefix, req.endpointId);

      Component endpoint = endpoints.get(req.endpointId);
      if (endpoint != null) {
        cleanupClient(endpoint, req);
        if (clients.get(req.getEndpointId()).isEmpty()) {
          cleanEndpoint(req.endpointId, endpoint);
          trigger(Kill.event, endpoint.control());
        }
      }
      answer(req, req.success());
    }
  };

  private void reportFaulty(Identifier endpointId, Throwable cause, Component endpoint) {
    Map<Identifier, EndpointMngrConnect.Request> endpointClients = clients.remove(endpointId);
    if (endpointClients != null) {
      endpointClients.values().forEach((req) -> {
        answer(req, req.fail(Result.internalFailure((Exception) cause)));
        cleanupClient(endpointId, endpoint);
      });
    }
  }

  private void cleanupClient(Component endpoint, EndpointMngrDisconnect.Request req) {
    Map<Identifier, EndpointMngrConnect.Request> endpointClients = clients.get(req.endpointId);
    if (endpointClients == null) {
      return;
    }
    endpointClients.remove(req.getClientId());
    streamMngrChannel.removeChannel(req.endpointId, endpoint.getPositive(StreamMngrPort.class));
    streamOpChannel.removeChannel(req.endpointId, endpoint.getPositive(StreamOpPort.class));
  }

  private void setupClient(EndpointMngrConnect.Request req) {
    Component endpoint = endpoints.get(req.endpointId);
    streamMngrChannel.addChannel(req.clientId, endpoint.getPositive(StreamMngrPort.class));
    streamOpChannel.addChannel(req.clientId, endpoint.getPositive(StreamOpPort.class));
    Map<Identifier, EndpointMngrConnect.Request> endpointClients = clients.get(req.endpointId);
    endpointClients.put(req.getClientId(), req);
    answer(req, req.success(Result.success(true)));
  }

  private void cleanupClient(Identifier endpointId, Component endpoint) {
    streamMngrChannel.removeChannel(endpointId, endpoint.getPositive(StreamMngrPort.class));
    streamOpChannel.removeChannel(endpointId, endpoint.getPositive(StreamOpPort.class));
  }

  private Component setupEndpoint(Identifier endpointId, StorageProvider provider) {
    Component endpoint = create(StreamMngrComp.class, new StreamMngrComp.Init(self, provider));
    connect(timerPort, endpoint.getNegative(Timer.class), Channel.TWO_WAY);

    endpoints.put(endpointId, endpoint);
    compIdToEndpointId.put(endpoint.id(), endpointId);
    clients.put(endpointId, new HashMap<>());
    return endpoint;
  }

  private void cleanEndpoint(Identifier endpointId, Component endpoint) {
    disconnect(timerPort, endpoint.getNegative(Timer.class));

    endpoints.remove(endpointId);
    compIdToEndpointId.remove(endpoint.id());
    clients.remove(endpointId);
  }

  public static class Init extends se.sics.kompics.Init<StorageMngrComp> {

    public final Identifier self;

    public Init(Identifier self) {
      this.self = self;
    }
  }
}
