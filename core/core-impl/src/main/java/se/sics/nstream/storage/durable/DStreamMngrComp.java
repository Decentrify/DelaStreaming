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
import org.javatuples.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.kompics.Component;
import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.Handler;
import se.sics.kompics.Kill;
import se.sics.kompics.Killed;
import se.sics.kompics.Negative;
import se.sics.kompics.Start;
import se.sics.kompics.util.Identifier;
import se.sics.ktoolbox.util.network.ports.One2NChannel;
import se.sics.nstream.StreamId;
import se.sics.nstream.storage.durable.events.DStreamConnect;
import se.sics.nstream.storage.durable.events.DStreamDisconnect;

/**
 *
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class DStreamMngrComp extends ComponentDefinition {

  private static final Logger LOG = LoggerFactory.getLogger(DStreamMngrComp.class);
  private final String logPrefix;

  private final Negative<DStoragePort> storagePort = provides(DStoragePort.class);
  private final Negative<DStreamControlPort> streamControlPort = provides(DStreamControlPort.class);
  private final One2NChannel storageChannel;
  //**************************************************************************
  private final Identifier self;
  private final DurableStorageProvider storageProvider;
  //**************************************************************************
  private final Map<UUID, StreamId> compIdToStreamId = new HashMap<>();
  private final Map<StreamId, Component> streamStorage = new HashMap<>();

  public DStreamMngrComp(Init init) {
    self = init.self;
    logPrefix = "<nid:" + self + ">";
    LOG.info("{}initiating...", logPrefix);

    storageProvider = init.storageProvider;

    storageChannel = One2NChannel.getChannel(logPrefix + ":dstream_storage", storagePort, new DStreamIdExtractor());

    subscribe(handleStart, control);
    subscribe(handleKilled, control);
    subscribe(handleConnect, streamControlPort);
    subscribe(handleDisconnect, streamControlPort);
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
      LOG.info("{}killed stream comp:{}", logPrefix, event.component.id());
    }
  };

  Handler handleConnect = new Handler<DStreamConnect.Request>() {
    @Override
    public void handle(DStreamConnect.Request req) {
      Pair<Init, Long> init = storageProvider.initiate(req.stream.getValue1().resource);
      LOG.info("{}connecting stream:{} pos:{}", logPrefix, req.stream.getValue0(), init.getValue1());
      Component streamStorageComp = create(storageProvider.getStorageDefinition(), init.getValue0());

      storageChannel.addChannel(req.stream.getValue0(), streamStorageComp.getPositive(DStoragePort.class));

      compIdToStreamId.put(streamStorageComp.id(), req.stream.getValue0());
      streamStorage.put(req.stream.getValue0(), streamStorageComp);

      trigger(Start.event, streamStorageComp.control());
      answer(req, req.success(init.getValue1()));
    }
  };

  Handler handleDisconnect = new Handler<DStreamDisconnect.Request>() {
    @Override
    public void handle(DStreamDisconnect.Request req) {
      LOG.info("{}disconnecting stream:{}", logPrefix, req.streamId);
      Component streamStorageComp = streamStorage.remove(req.streamId);
      if (streamStorageComp == null) {
        throw new RuntimeException("TODO Alex - probbably mismatch between things keeping track of resources");
      }
      compIdToStreamId.remove(streamStorageComp.id());
      
      storageChannel.removeChannel(req.streamId, streamStorageComp.getPositive(DStoragePort.class));
      
      trigger(Kill.event, streamStorageComp.control());
      answer(req, req.success());
    }
  };

  public static class Init extends se.sics.kompics.Init<DStreamMngrComp> {

    public final Identifier self;
    public final DurableStorageProvider storageProvider;

    public Init(Identifier self, DurableStorageProvider storageProvider) {
      this.self = self;
      this.storageProvider = storageProvider;
    }
  }
}
