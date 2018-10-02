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
package se.sics.dela.storage.mngr.stream;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.javatuples.Pair;
import se.sics.dela.storage.mngr.StorageProvider;
import se.sics.dela.storage.mngr.stream.events.StreamMngrConnect;
import se.sics.dela.storage.mngr.stream.events.StreamMngrDisconnect;
import se.sics.dela.storage.operation.StreamStorageOpPort;
import se.sics.kompics.Channel;
import se.sics.kompics.Component;
import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.Handler;
import se.sics.kompics.Kill;
import se.sics.kompics.Killed;
import se.sics.kompics.Negative;
import se.sics.kompics.Positive;
import se.sics.kompics.Start;
import se.sics.kompics.timer.Timer;
import se.sics.kompics.util.Identifier;
import se.sics.ktoolbox.util.network.ports.One2NChannel;
import se.sics.nstream.StreamId;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class StreamMngrComp extends ComponentDefinition {

  private final Positive<Timer> timerPort = requires(Timer.class);
  private final Negative<StreamStorageOpPort> storagePort = provides(StreamStorageOpPort.class);
  private final Negative<StreamMngrPort> streamControlPort = provides(StreamMngrPort.class);
  private final One2NChannel storageChannel;
  //**************************************************************************
  private final Identifier self;
  private final StorageProvider storageProvider;
  //**************************************************************************
  private final Map<UUID, StreamId> compIdToStreamId = new HashMap<>();
  private final Map<StreamId, Component> streamStorage = new HashMap<>();

  public StreamMngrComp(Init init) {
    self = init.self;
    loggingCtxPutAlways("nid", self.toString());

    storageProvider = init.storageProvider;

    String channelName = "<nid:" + self + ">:dstream_storage";
    storageChannel = One2NChannel.getChannel(channelName, storagePort, new StreamIdExtractor());

    subscribe(handleStart, control);
    subscribe(handleKilled, control);
    subscribe(handleConnect, streamControlPort);
    subscribe(handleDisconnect, streamControlPort);
  }

  Handler handleStart = new Handler<Start>() {
    @Override
    public void handle(Start event) {
    }
  };

  Handler handleKilled = new Handler<Killed>() {
    @Override
    public void handle(Killed event) {
      logger.info("killed stream comp:{}", event.component.id());
    }
  };

  Handler handleConnect = new Handler<StreamMngrConnect.Request>() {
    @Override
    public void handle(StreamMngrConnect.Request req) {
      Pair<Init, Long> init = storageProvider.initiate(req.stream.resource, logger);
      logger.info("stream:{} pos:{} - connecting", req.streamId, init.getValue1());
      Component streamStorageComp = create(storageProvider.getStorageDefinition(), init.getValue0());

      storageChannel.addChannel(req.streamId, streamStorageComp.getPositive(StreamStorageOpPort.class));
      connect(timerPort, streamStorageComp.getNegative(Timer.class), Channel.TWO_WAY);
      
      compIdToStreamId.put(streamStorageComp.id(), req.streamId);
      streamStorage.put(req.streamId, streamStorageComp);

      trigger(Start.event, streamStorageComp.control());
      answer(req, req.success(init.getValue1()));
    }
  };

  Handler handleDisconnect = new Handler<StreamMngrDisconnect.Request>() {
    @Override
    public void handle(StreamMngrDisconnect.Request req) {
      logger.info("stream:{} - disconnecting", req.streamId);
      Component streamStorageComp = streamStorage.remove(req.streamId);
      if (streamStorageComp == null) {
        throw new RuntimeException("TODO Alex - probbably mismatch between things keeping track of resources");
      }
      compIdToStreamId.remove(streamStorageComp.id());
      
      storageChannel.removeChannel(req.streamId, streamStorageComp.getPositive(StreamStorageOpPort.class));
      disconnect(timerPort, streamStorageComp.getNegative(Timer.class));
      
      trigger(Kill.event, streamStorageComp.control());
      answer(req, req.success());
    }
  };

  public static class Init extends se.sics.kompics.Init<StreamMngrComp> {

    public final Identifier self;
    public final StorageProvider storageProvider;

    public Init(Identifier self, StorageProvider storageProvider) {
      this.self = self;
      this.storageProvider = storageProvider;
    }
  }
}
