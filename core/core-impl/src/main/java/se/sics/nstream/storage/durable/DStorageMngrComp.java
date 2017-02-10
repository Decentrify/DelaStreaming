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
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.kompics.Component;
import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.Handler;
import se.sics.kompics.Kill;
import se.sics.kompics.Killed;
import se.sics.kompics.Negative;
import se.sics.kompics.Start;
import se.sics.ktoolbox.nutil.fsm.ids.FSMId;
import se.sics.ktoolbox.util.identifiable.Identifier;
import se.sics.ktoolbox.util.network.ports.One2NChannel;
import se.sics.nstream.storage.durable.events.DEndpointConnect;
import se.sics.nstream.storage.durable.events.DEndpointDisconnect;

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
  private final Map<Identifier, Component> storageEndpoints = new HashMap<>();
  private final Map<Identifier, List<FSMId>> clients = new HashMap<>();

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

  Handler handleConnect = new Handler<DEndpointConnect.Request>() {
    @Override
    public void handle(DEndpointConnect.Request req) {
      LOG.info("{}connecting endpoint:{}", logPrefix, req.endpointId);
      List<FSMId> endpointClients;
      if (!storageEndpoints.containsKey(req.endpointId)) {
        Component endpointComp = create(DStreamMngrComp.class, new DStreamMngrComp.Init(self, req.endpointProvider));
        streamControlChannel.addChannel(req.endpointId, endpointComp.getPositive(DStreamControlPort.class));
        storageChannel.addChannel(req.endpointId, endpointComp.getPositive(DStoragePort.class));

        compIdToEndpointId.put(endpointComp.id(), req.endpointId);
        storageEndpoints.put(req.endpointId, endpointComp);

        trigger(Start.event, endpointComp.control());

        endpointClients = new LinkedList<>();
        clients.put(req.endpointId, endpointClients);
      } else {
        endpointClients = clients.get(req.endpointId);
      }
      endpointClients.add(req.fsmId);
      answer(req, req.success());
    }
  };

  Handler handleDisconnect = new Handler<DEndpointDisconnect.Request>() {
    @Override
    public void handle(DEndpointDisconnect.Request req) {
      LOG.info("{}disconnecting endpoint:{}", logPrefix, req.endpointId);
      List<FSMId> endpointClients = clients.get(req.endpointId);
      if (endpointClients == null) {
        throw new RuntimeException("weird behaviour - someone is not keeping track of things right");
      }
      endpointClients.remove(req.fsmId);

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
