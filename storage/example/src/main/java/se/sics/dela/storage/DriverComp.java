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

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Consumer;
import org.slf4j.Logger;
import se.sics.dela.storage.disk.DiskEndpoint;
import se.sics.dela.storage.mngr.StorageProvider;
import se.sics.dela.storage.mngr.endpoint.EndpointMngrPort;
import se.sics.dela.storage.mngr.endpoint.EndpointMngrProxy;
import se.sics.dela.storage.mngr.endpoint.EndpointRegistry;
import se.sics.dela.storage.mngr.stream.StreamMngrPort;
import se.sics.dela.storage.mngr.stream.impl.StreamMngrProxy;
import se.sics.dela.storage.op.TorrentHandlerMngr;
import se.sics.dela.storage.op.TorrentHandlerMngr.FileReady;
import se.sics.dela.storage.op.TorrentHandlerMngr.TorrentHandler;
import se.sics.dela.storage.operation.StreamStorageOpPort;
import se.sics.dela.util.MyIdentifierFactory;
import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.Handler;
import se.sics.kompics.Positive;
import se.sics.kompics.Start;
import se.sics.kompics.util.Identifier;
import se.sics.ktoolbox.util.TupleHelper;
import se.sics.ktoolbox.util.identifiable.BasicIdentifiers;
import se.sics.ktoolbox.util.identifiable.IdentifierFactory;
import se.sics.ktoolbox.util.identifiable.overlay.OverlayId;
import se.sics.nstream.FileId;
import se.sics.nstream.transfer.MyTorrent;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class DriverComp extends ComponentDefinition {

  private final Positive<StreamStorageOpPort> streamOpPort = requires(StreamStorageOpPort.class);
  private final Positive<EndpointMngrPort> endpointMngrPort = requires(EndpointMngrPort.class);
  private final Positive<StreamMngrPort> streamMngrPort = requires(StreamMngrPort.class);

  private final EndpointMngrProxy endpointMngrProxy;
  private final TorrentHandlerMngr torrentStorageMngr;
  private final StreamMngrProxy streamMngrProxy;

  private final EndpointCtrl endpointCtrl;
  private final Library library;

  private final Identifier selfId;
  private final String testPath;

  public DriverComp(Init init) {
    selfId = init.selfId;
    loggingCtxPutAlways("nid", init.selfId.toString());
    testPath = init.testPath;
    library = new Library(init.torrents);

    endpointMngrProxy = new EndpointMngrProxy(init.endpointIdFactory, init.storageProviders);
    streamMngrProxy = new StreamMngrProxy();
    torrentStorageMngr = new TorrentHandlerMngr(config(), logger, streamMngrProxy);
    endpointCtrl = new EndpointCtrl(startTorrent());

    subscribe(handleStart, control);
  }

  Handler<Start> handleStart = new Handler<Start>() {
    @Override
    public void handle(Start event) {
      endpointMngrProxy.setup(proxy, logger);
      streamMngrProxy.setup(proxy);
      prepareTorrents();
    }
  };

  private void prepareTorrents() {
    library.torrents.entrySet().forEach((torrent) -> torrentStorageMngr.setupTorrent(torrent.getKey(), torrent.getValue()));
    connectEndpoints();
  }

  private void connectEndpoints() {
    connectDisk();
  }

  private void connectDisk() {
    Identifier clientId = BasicIdentifiers.eventId();
    TupleHelper.PairConsumer diskConnected
      = new TupleHelper.PairConsumer<Identifier, Identifier>() {
      @Override
      public void accept(Identifier clientId, Identifier endpointId) {
        logger.info("endpoint:{} - connected", new Object[]{endpointId});
        endpointCtrl.connected(endpointId);
      }
    };
    TupleHelper.TripletConsumer<Identifier, Identifier, Throwable> diskConnectFailed
      = new TupleHelper.TripletConsumer<Identifier, Identifier, Throwable>() {
      @Override
      public void accept(Identifier clientId, Identifier endpointId, Throwable cause) {
        logger.info("endpoint:{} - connect fail", new Object[]{endpointId});
        endpointCtrl.connectFailed(endpointId, cause);
      }
    };

    TupleHelper.PairConsumer<Identifier, Identifier> diskDisconnected
      = new TupleHelper.PairConsumer<Identifier, Identifier>() {
      @Override
      public void accept(Identifier clientId, Identifier endpointId) {
        logger.info("endpoint:{} - disconnected", new Object[]{endpointId});
        endpointCtrl.disconnected(endpointId);
      }
    };

    EndpointRegistry.ClientBuilder cb = new EndpointRegistry.ClientBuilder(clientId,
      diskConnected, diskConnectFailed, diskDisconnected);
    Identifier endpointId = endpointMngrProxy.connectEndpoint(DiskEndpoint.DISK_ENDPOINT_NAME, cb);
    endpointCtrl.connect(endpointId);
  }

  private static class EndpointCtrl {

    private final Set<Identifier> connecting = new HashSet<>();
    private final Set<Identifier> connected = new HashSet<>();
    private final Map<Identifier, Throwable> connectFailed = new HashMap<>();

    private final Consumer<Boolean> connectedCallback;

    public EndpointCtrl(Consumer<Boolean> connectedCallback) {
      this.connectedCallback = connectedCallback;
    }

    public void connect(Identifier endpointId) {
      connecting.add(endpointId);
    }

    public void connected(Identifier endpointId) {
      connecting.remove(endpointId);
      connected.add(endpointId);
      if (connecting.isEmpty() && connectFailed.isEmpty()) {
        connectedCallback.accept(true);
      }
    }

    public void connectFailed(Identifier endpointId, Throwable cause) {
      connecting.remove(endpointId);
      connectFailed.put(endpointId, cause);
    }

    public void disconnected(Identifier endpointId) {
      connected.remove(endpointId);
    }

    public boolean allConnected() {
      return connecting.isEmpty() && connectFailed.isEmpty();
    }
  }

//  private void connectStreams(OverlayId torrentId) {
//    MyTorrent torrent = torrents.get(torrentId);
//    String torrent1Path = testPath + File.separator + torrentId;
//    prepareFile(torrentId, torrent1Path, 0);
//  }
//  private void prepareFile(OverlayId torrentId, String torrentPath, int fileNr) {
//    FileId fileId = TorrentIds.fileId(torrentId, fileNr);
//    Identifier endpointId = endpointMngrProxy.registry.idRegistry.lookup(DiskEndpoint.DISK_ENDPOINT_NAME);
//    Map<StreamId, StreamStorage> readWrite = new HashMap<>();
//    StreamId stream1Id = TorrentIds.streamId(endpointId, fileId);
//    StorageEndpoint storageEndpoint1 = new DiskEndpoint();
//    StorageResource storageResource1 = new DiskResource(torrentPath, "file" + fileNr);
//    StreamStorage streamStorage1 = new StreamStorage(storageEndpoint1, storageResource1);
//    readWrite.put(stream1Id, streamStorage1);
//    Map<StreamId, StreamStorage> writeOnly = new HashMap<>();
//    streamMngrProxy.prepareFile(torrentId, fileId, readWrite, writeOnly);
//  }
  private Consumer<Boolean> startTorrent() {
    return new Consumer<Boolean>() {
      @Override
      public void accept(Boolean notUsed) {
        nextTorrent();
      }
    };
  }

  private void nextTorrent() {
    if (!library.hasPending() && !library.hasActive()) {
      logger.info("library completed");
    }
    OverlayId torrentId = library.nextTorrent();
    logger.info("torrent:{} started", torrentId);
    TorrentHandler storageHandler = torrentStorageMngr.getTorrent(torrentId);
    nextFile(torrentId, storageHandler, torrentCallback());
  }

  private Consumer<OverlayId> torrentCallback() {
    return (OverlayId torrentId) -> {
      logger.info("torrent:{} completed", new Object[]{torrentId});
      library.complete(torrentId);
      nextTorrent();
    };
  }

  private void nextFile(OverlayId torrentId, TorrentHandler torrentStorageHandler,
    Consumer<OverlayId> torrentCompleted) {
    if (torrentStorageHandler.hasPending()) {
      Consumer<FileId> fileCompletedCallback = fileCallback(torrentId, torrentStorageHandler, torrentCompleted);
      FileHandler fh = new FileHandler(logger, torrentId, torrentStorageHandler, fileCompletedCallback);
      torrentStorageHandler.prepareNextPending(fh.fileReady);
    }
    if (!torrentStorageHandler.hasWritting()) {
      torrentCompleted.accept(torrentId);
    }
  }

  private Consumer<FileId> fileCallback(OverlayId torrentId, TorrentHandler torrentStorageHandler,
    Consumer<OverlayId> torrentCompleted) {
    return (FileId fileId) -> {
      logger.info("torrent:{} file:{} completed", new Object[]{torrentId, fileId});
      nextFile(torrentId, torrentStorageHandler, torrentCompleted);
    };
  }

  private static class FileHandler {

    final Logger logger;
    final OverlayId torrentId;
    final TorrentHandler storageHandler;
    FileId fileId;
    final Consumer<FileId> onCompletion;

    public FileHandler(Logger logger, OverlayId torrentId, TorrentHandler storageHandler,
      Consumer<FileId> onCompletion) {
      this.logger = logger;
      this.torrentId = torrentId;
      this.storageHandler = storageHandler;
      this.onCompletion = onCompletion;
    }

    private Consumer<FileReady> fileReady = new Consumer<FileReady>() {
      @Override
      public void accept(FileReady file) {
        fileId = file.fileId;
        logger.info("torrent:{} file:{} ready", new Object[]{torrentId, fileId});
        storageHandler.closeWrite(file.fileId, closeWrite);
      }
    };

    private Consumer<Boolean> closeWrite = new Consumer<Boolean>() {
      @Override
      public void accept(Boolean result) {
        logger.info("torrent:{} file:{} write closed", new Object[]{torrentId, fileId});
        storageHandler.closeRead(fileId, closeRead);
      }
    };

    private Consumer<Boolean> closeRead = new Consumer<Boolean>() {
      @Override
      public void accept(Boolean result) {
        logger.info("torrent:{} file:{} read closed", new Object[]{torrentId, fileId});
        onCompletion.accept(fileId);
      }
    };
  }

  private static class Library {

    public final Map<OverlayId, MyTorrent> torrents;
    private final TreeSet<OverlayId> pending = new TreeSet<>();
    private final List<OverlayId> active = new LinkedList<>();

    public Library(Map<OverlayId, MyTorrent> torrents) {
      this.torrents = torrents;
      this.pending.addAll(torrents.keySet());
    }

    public boolean hasPending() {
      return !pending.isEmpty();
    }

    public boolean hasActive() {
      return !active.isEmpty();
    }

    public OverlayId nextTorrent() {
      OverlayId torrentId = pending.pollFirst();
      active.add(torrentId);
      return torrentId;
    }

    public void complete(OverlayId torrentId) {
      active.remove(torrentId);
    }
  }

  public static class Init extends se.sics.kompics.Init<DriverComp> {

    public final Identifier selfId;
    public final List<StorageProvider> storageProviders;
    public final MyIdentifierFactory endpointIdFactory;
    public final String testPath;
    public final Map<OverlayId, MyTorrent> torrents;

    public Init(Identifier selfId, MyIdentifierFactory endpointIdFactory, List<StorageProvider> storageProviders,
      String testPath, Map<OverlayId, MyTorrent> torrents) {
      this.selfId = selfId;
      this.storageProviders = storageProviders;
      this.endpointIdFactory = endpointIdFactory;
      this.testPath = testPath;
      this.torrents = torrents;
    }
  }
}
