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
package se.sics.nstream.storage.durable;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.kompics.Component;
import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.Fault;
import se.sics.kompics.Handler;
import se.sics.kompics.Kill;
import se.sics.kompics.Killed;
import se.sics.kompics.Negative;
import se.sics.kompics.Start;
import se.sics.ktoolbox.util.identifiable.Identifier;
import se.sics.ktoolbox.util.identifiable.overlay.OverlayId;
import se.sics.ktoolbox.util.network.ports.One2NChannel;
import se.sics.ktoolbox.util.result.Result;
import se.sics.nstream.storage.durable.events.DEndpoint;

/**
 *
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class DStorageMngrComp extends ComponentDefinition {

  private static final Logger LOG = LoggerFactory.getLogger(DStorageMngrComp.class);
  private final String logPrefix;

  private final Negative<DStoragePort> storagePort = provides(DStoragePort.class);
  private final Negative<DStreamControlPort> streamControlPort = provides(DStreamControlPort.class);
  private final Negative<DEndpointCtrlPort> endpointControlPort = provides(DEndpointCtrlPort.class);
  private final One2NChannel streamControlChannel;
  private final One2NChannel storageChannel;
  //**************************************************************************
  private final Identifier self;
  //**************************************************************************
  private final Map<UUID, Identifier> compIdToEndpointId = new HashMap<>();
  private final Map<Identifier, Map<OverlayId, DEndpoint.Connect>> clients = new HashMap<>();
  private final Map<Identifier, Component> storageEndpoints = new HashMap<>();
  private final Map<Identifier, Throwable> reportedFaulty = new HashMap<>();

  public DStorageMngrComp(Init init) {
    self = init.self;
    logPrefix = "<nid:" + self + ">";
    LOG.info("{}initiating...", logPrefix);

    streamControlChannel = One2NChannel.getChannel(logPrefix + ":dstream_control", streamControlPort,
      new DEndpointIdExtractor());
    storageChannel = One2NChannel.getChannel(logPrefix + ":dstorage", storagePort, new DEndpointIdExtractor());

    subscribe(handleStart, control);
    subscribe(handleKilled, control);
    subscribe(handleConnect, endpointControlPort);
    subscribe(handleDisconnect, endpointControlPort);
  }

  Handler handleStart = new Handler<Start>() {
    @Override
    public void handle(Start event) {
      LOG.info("{}starting", logPrefix);
    }
  };

  Handler handleKilled = new Handler<Killed>() {
    @Override
    public void handle(Killed event) {
      LOG.info("{}killed endpoint component:{}", logPrefix, event.component.id());
    }
  };

  @Override
  public Fault.ResolveAction handleFault(Fault fault) {
    UUID faultyComponent = fault.getSource().id();
    Identifier endpointId = compIdToEndpointId.get(faultyComponent);
    Throwable cause = fault.getCause();
    reportedFaulty.put(endpointId, cause);
    for (DEndpoint.Connect c : clients.get(endpointId).values()) {
      answer(c, c.fail(Result.internalFailure((Exception)cause)));
    }

    Component endpointComp = storageEndpoints.get(endpointId);
    //TODO Alex - talk to Lars - it is possible a disconnect occured immediately before the exception
    if (endpointComp != null) {
      for (DEndpoint.Connect c : clients.get(endpointId).values()) {
        streamControlChannel.removeChannel(endpointId, endpointComp.getPositive(DStreamControlPort.class));
        storageChannel.removeChannel(endpointId, endpointComp.getPositive(DStoragePort.class));
      }
      clients.remove(endpointId);
      storageEndpoints.remove(endpointId);
      compIdToEndpointId.remove(faultyComponent);
      trigger(Kill.event, endpointComp.control());
    }

    return Fault.ResolveAction.RESOLVED;
  }

  Handler handleConnect = new Handler<DEndpoint.Connect>() {
    @Override
    public void handle(DEndpoint.Connect req) {
      LOG.info("{}connecting endpoint:{}", logPrefix, req.endpointId);
      if(reportedFaulty.containsKey(req.endpointId)) {
        answer(req, req.fail(Result.internalFailure((Exception)reportedFaulty.get(req.endpointId))));
      }
      Map<OverlayId, DEndpoint.Connect> endpointClients;
      if (!storageEndpoints.containsKey(req.endpointId)) {
        Component endpointComp = create(DStreamMngrComp.class, new DStreamMngrComp.Init(self, req.endpointProvider));
        streamControlChannel.addChannel(req.endpointId, endpointComp.getPositive(DStreamControlPort.class));
        storageChannel.addChannel(req.endpointId, endpointComp.getPositive(DStoragePort.class));

        compIdToEndpointId.put(endpointComp.id(), req.endpointId);
        storageEndpoints.put(req.endpointId, endpointComp);

        trigger(Start.event, endpointComp.control());

        endpointClients = new HashMap<>();
        clients.put(req.endpointId, endpointClients);
      } else {
        endpointClients = clients.get(req.endpointId);
      }
      endpointClients.put(req.torrentId, req);
      answer(req, req.success(Result.success(true)));
    }
  };

  Handler handleDisconnect = new Handler<DEndpoint.Disconnect>() {
    @Override
    public void handle(DEndpoint.Disconnect req) {
      LOG.info("{}disconnecting endpoint:{}", logPrefix, req.endpointId);
      Map<OverlayId, DEndpoint.Connect> endpointClients = clients.get(req.endpointId);
      if (endpointClients == null) {
        //it is possible to get here on component fault + cleanup
        answer(req, req.success());
        return;
      }
      endpointClients.remove(req.torrentId);

      Component endpointComp = storageEndpoints.get(req.endpointId);
      if (endpointComp == null) {
        throw new RuntimeException("weird behaviour - someone is not keeping track of things right");
      }
      streamControlChannel.removeChannel(req.endpointId, endpointComp.getPositive(DStreamControlPort.class));
      storageChannel.removeChannel(req.endpointId, endpointComp.getPositive(DStoragePort.class));

      if (endpointClients.isEmpty()) {
        clients.remove(req.endpointId);
        storageEndpoints.remove(req.endpointId);
        compIdToEndpointId.remove(endpointComp.id());
        trigger(Kill.event, endpointComp.control());
      }

      answer(req, req.success());
    }
  };

  public static class Init extends se.sics.kompics.Init<DStorageMngrComp> {

    public final Identifier self;

    public Init(Identifier self) {
      this.self = self;
    }
  }
}
