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
package se.sics.dela.storage;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import se.sics.dela.storage.disk.DiskComp;
import se.sics.dela.storage.disk.DiskEndpoint;
import se.sics.dela.storage.disk.DiskResource;
import se.sics.dela.storage.mngr.StorageProvider;
import se.sics.dela.storage.mngr.endpoint.EndpointMngrPort;
import se.sics.dela.storage.mngr.endpoint.EndpointMngrProxy;
import se.sics.dela.storage.mngr.endpoint.EndpointRegistry;
import se.sics.dela.storage.mngr.stream.StreamMngrPort;
import se.sics.dela.storage.mngr.stream.impl.BaseStreamMngrProxy;
import se.sics.dela.storage.operation.StreamOpPort;
import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.Handler;
import se.sics.kompics.Positive;
import se.sics.kompics.Start;
import se.sics.kompics.util.Identifier;
import se.sics.ktoolbox.util.TupleHelper;
import se.sics.ktoolbox.util.identifiable.IdentifierFactory;
import se.sics.ktoolbox.util.identifiable.overlay.OverlayId;
import se.sics.nstream.FileId;
import se.sics.nstream.StreamId;
import se.sics.nstream.TorrentIds;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class DriverComp extends ComponentDefinition {

  private final Positive<StreamOpPort> streamOpPort = requires(StreamOpPort.class);
  private final Positive<EndpointMngrPort> endpointMngrPort = requires(EndpointMngrPort.class);
  private final Positive<StreamMngrPort> streamMngrPort = requires(StreamMngrPort.class);
  private final EndpointMngrProxy endpointMngrProxy;
  private final BaseStreamMngrProxy streamMngrProxy;
  private final Identifier selfId;
  private final String testPath;
  private final OverlayId torrent1Id;
  private final OverlayId torrent2Id;
  
  public DriverComp(Init init) {
    selfId = init.selfId;
    testPath = init.testPath;
    torrent1Id = TorrentIds.torrentIdFactory().randomId();
    torrent2Id = TorrentIds.torrentIdFactory().randomId();

    endpointMngrProxy = new EndpointMngrProxy(init.endpointIdFactory, init.storageProviders);
    streamMngrProxy = new BaseStreamMngrProxy();
    subscribe(handleStart, control);
  }

  Handler<Start> handleStart = new Handler<Start>() {
    @Override
    public void handle(Start event) {
      logger.info("started");
      endpointMngrProxy.setup(proxy);
      streamMngrProxy.setup(proxy);
      connectEndpoints();
      connectStreams();
    }
  };

  private void connectEndpoints() {
    endpointMngrProxy.connectEndpoint(DiskEndpoint.DISK_ENDPOINT_NAME, endpointClient(torrent1Id));
    endpointMngrProxy.connectEndpoint(DiskEndpoint.DISK_ENDPOINT_NAME, endpointClient(torrent2Id));
  }

  private void connectStreams() {
    String torrent1Path = testPath + File.separator + "torrent1";
    prepareFile(torrent1Path, 0);
  }
  
  private void prepareFile(String torrentPath, int fileNr) {
    FileId fileId = TorrentIds.fileId(torrent1Id, fileNr);
    Identifier endpointId = endpointMngrProxy.registry.idRegistry.lookup(DiskEndpoint.DISK_ENDPOINT_NAME);
    Map<StreamId, StreamStorage> readWrite = new HashMap<>();
    StreamId stream1Id = TorrentIds.streamId(endpointId, fileId);
    StorageEndpoint storageEndpoint1 = new DiskEndpoint();
    StorageResource storageResource1 = new DiskResource(torrentPath, "file" + fileNr);
    StreamStorage streamStorage1 = new StreamStorage(storageEndpoint1, storageResource1);
    readWrite.put(stream1Id, streamStorage1);
    Map<StreamId, StreamStorage> writeOnly = new HashMap<>();
    streamMngrProxy.prepareFile(torrent1Id, fileId, readWrite, writeOnly);
  }
  
  private EndpointRegistry.ClientBuilder endpointClient(Identifier clientId) {
    return new EndpointRegistry.ClientBuilder(clientId,
      endpointConnected(),
      endpointConnectFailed(),
      endpointDisconnected());
  }

  TupleHelper.PairConsumer<Identifier, Identifier> endpointConnected() {
    return new TupleHelper.PairConsumer<Identifier, Identifier>() {
      @Override
      public void accept(Identifier clientId, Identifier endpointId) {
        logger.info("client:{}, endpoint:{} connected", new Object[]{clientId, endpointId});
      }
    };
  }

  TupleHelper.TripletConsumer<Identifier, Identifier, Throwable> endpointConnectFailed() {
    return new TupleHelper.TripletConsumer<Identifier, Identifier, Throwable>() {
      @Override
      public void accept(Identifier clientId, Identifier endpointId, Throwable cause) {
        logger.info("client:{}, endpoint:{} connect fail", new Object[]{clientId, endpointId});
      }
    };
  }

  TupleHelper.PairConsumer<Identifier, Identifier> endpointDisconnected() {
    return new TupleHelper.PairConsumer<Identifier, Identifier>() {
      @Override
      public void accept(Identifier clientId, Identifier endpointId) {
        logger.info("client:{}, endpoint:{} disconnected", new Object[]{clientId, endpointId});
      }
    };
  }

  public static class Init extends se.sics.kompics.Init<DriverComp> {

    public final Identifier selfId;
    public final List<StorageProvider> storageProviders;
    public final IdentifierFactory endpointIdFactory;
    public final String testPath;

    public Init(Identifier selfId, IdentifierFactory endpointIdFactory, List<StorageProvider> storageProviders, 
      String testPath) {
      this.selfId = selfId;
      this.storageProviders = storageProviders;
      this.endpointIdFactory = endpointIdFactory;
      this.testPath = testPath;
    }
  }
}
