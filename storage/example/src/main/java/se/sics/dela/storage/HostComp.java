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
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.javatuples.Pair;
import se.sics.dela.storage.disk.DiskComp;
import se.sics.dela.storage.mngr.StorageProvider;
import se.sics.dela.storage.mngr.endpoint.StorageMngrComp;
import se.sics.dela.storage.mngr.endpoint.EndpointMngrPort;
import se.sics.dela.storage.mngr.stream.StreamMngrPort;
import se.sics.dela.storage.operation.StreamStorageOpPort;
import se.sics.dela.util.MyIdentifierFactory;
import se.sics.dela.util.MyIntIdFactory;
import se.sics.kompics.Channel;
import se.sics.kompics.Component;
import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.Handler;
import se.sics.kompics.Kompics;
import se.sics.kompics.Start;
import se.sics.kompics.fsm.FSMException;
import se.sics.kompics.timer.Timer;
import se.sics.kompics.timer.java.JavaTimer;
import se.sics.kompics.util.Identifier;
import se.sics.ktoolbox.util.identifiable.BasicBuilders;
import se.sics.ktoolbox.util.identifiable.BasicIdentifiers;
import se.sics.ktoolbox.util.identifiable.IdentifierFactory;
import se.sics.ktoolbox.util.identifiable.IdentifierRegistry;
import se.sics.ktoolbox.util.identifiable.basic.IntIdFactory;
import se.sics.ktoolbox.util.identifiable.basic.StringByteIdFactory;
import se.sics.ktoolbox.util.identifiable.basic.UUIDIdFactory;
import se.sics.ktoolbox.util.identifiable.overlay.OverlayId;
import se.sics.ktoolbox.util.identifiable.overlay.OverlayIdFactory;
import se.sics.ktoolbox.util.identifiable.overlay.OverlayRegistry;
import se.sics.ktoolbox.util.managedStore.core.util.HashUtil;
import se.sics.nstream.FileId;
import se.sics.nstream.StreamId;
import se.sics.nstream.TorrentIds;
import se.sics.nstream.hops.SystemOverlays;
import se.sics.nstream.hops.manifest.FileInfoJSON;
import se.sics.nstream.hops.manifest.ManifestHelper;
import se.sics.nstream.hops.manifest.ManifestJSON;
import se.sics.nstream.hops.storage.disk.DiskEndpoint;
import se.sics.nstream.hops.storage.disk.DiskResource;
import se.sics.nstream.storage.durable.util.FileExtendedDetails;
import se.sics.nstream.storage.durable.util.MyStream;
import se.sics.nstream.storage.durable.util.StreamEndpoint;
import se.sics.nstream.storage.durable.util.StreamResource;
import se.sics.nstream.transfer.MyTorrent;
import se.sics.nstream.util.BlockDetails;
import se.sics.nstream.util.FileBaseDetails;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class HostComp extends ComponentDefinition {

  private Component timer;
  private Component storageMngr;
  private Component driver;
  private static final String testPath = "./src/main/resources/example/storage";

  public HostComp(Init init) {
    setup(init.seed, init.selfId);
    subscribe(handleStart, control);
  }

  private void setup(long seed, Identifier selfId) {
    timer = create(JavaTimer.class, Init.NONE);
    storageMngr = create(StorageMngrComp.class, new StorageMngrComp.Init(selfId));
    connect(timer.getPositive(Timer.class), storageMngr.getNegative(Timer.class), Channel.TWO_WAY);
    driver = create(DriverComp.class, driverInit(seed, selfId));
    connect(storageMngr.getPositive(EndpointMngrPort.class), driver.getNegative(EndpointMngrPort.class),
      Channel.TWO_WAY);
    connect(storageMngr.getPositive(StreamMngrPort.class), driver.getNegative(StreamMngrPort.class), Channel.TWO_WAY);
    connect(storageMngr.getPositive(StreamStorageOpPort.class), driver.getNegative(StreamStorageOpPort.class),
      Channel.TWO_WAY);
  }

  private DriverComp.Init driverInit(long seed, Identifier selfId) {
    MyIdentifierFactory endpointIdFactory = new MyIntIdFactory(new IntIdFactory(new Random(seed + 2)), 0);
    MyIdentifierFactory copyEndpointIdFactory = new MyIntIdFactory(new IntIdFactory(new Random(seed + 2)), 0);
    List<StorageProvider> storageProviders = new ArrayList<>();
    storageProviders.add(new DiskComp.StorageProvider(selfId));
    Map<OverlayId, MyTorrent> torrents = torrents(copyEndpointIdFactory);
    return new DriverComp.Init(selfId, endpointIdFactory, storageProviders, testPath, torrents);
  }

  Handler<Start> handleStart = new Handler<Start>() {
    @Override
    public void handle(Start event) {
    }
  };

  public static class Init extends se.sics.kompics.Init<HostComp> {

    public final long seed;
    public final Identifier selfId;

    public Init(long seed, Identifier selfId) {
      this.seed = seed;
      this.selfId = selfId;
    }
  }

  private static void setupBasicIds(long seed) {
    Random rand = new Random(seed);
    IdentifierRegistry.register(BasicIdentifiers.Values.EVENT.toString(), new UUIDIdFactory());
    IdentifierRegistry.register(BasicIdentifiers.Values.MSG.toString(), new UUIDIdFactory());
    IdentifierRegistry.register(BasicIdentifiers.Values.OVERLAY.toString(), new StringByteIdFactory(rand, 64));
    IdentifierRegistry.register(BasicIdentifiers.Values.NODE.toString(), new IntIdFactory(rand));
  }
  
  private static void setupOverlayIds() {
    OverlayRegistry.initiate(new SystemOverlays.TypeFactory(), new SystemOverlays.Comparator());
    //torrent overlays
    byte torrentOwnerId = 1;
    OverlayRegistry.registerPrefix(TorrentIds.TORRENT_OVERLAYS, torrentOwnerId);
  }

  public static void main(String[] args) throws IOException, FSMException, URISyntaxException {
    long seed = 1234;
    setupBasicIds(seed);
    setupOverlayIds();
    Init init = new Init(seed, BasicIdentifiers.nodeId());

    if (Kompics.isOn()) {
      Kompics.shutdown();
    }
    // Yes 20 is totally arbitrary
    Kompics.createAndStart(HostComp.class, init, Runtime.getRuntime().availableProcessors(), 20);
    try {
      Kompics.waitForTermination();
    } catch (InterruptedException ex) {
      System.exit(1);
    }
  }

  //torrents
  private Map<OverlayId, MyTorrent> torrents(MyIdentifierFactory endpointIdFactory) {
    Identifier endpointId = endpointIdFactory.next();
    Map<OverlayId, MyTorrent> torrents = new HashMap<>();
    OverlayId torrent1 = TorrentIds.torrentId(new BasicBuilders.IntBuilder(1));
    torrents.put(torrent1, torrent(torrent1, "torrent1", endpointId));
    OverlayId torrent2 = TorrentIds.torrentId(new BasicBuilders.IntBuilder(2));
    torrents.put(torrent2, torrent(torrent2, "torrent2", endpointId));
    return torrents;
  }
  
  private MyTorrent torrent(OverlayId torrentId, String torrentName, Identifier endpointId) {
    MyTorrent.Manifest manifest = ManifestHelper.getManifest(manifestJSON());
    Map<String, FileId> files = new HashMap<>();
    files.put("file1", TorrentIds.fileId(torrentId, 1));
    files.put("file2", TorrentIds.fileId(torrentId, 2));
    Map<FileId, FileBaseDetails> base = new HashMap<>();
    base.put(TorrentIds.fileId(torrentId, 1), fileBase());
    base.put(TorrentIds.fileId(torrentId, 2), fileBase());
    Map<FileId, FileExtendedDetails> extended = new HashMap<>();
    extended.put(TorrentIds.fileId(torrentId, 2), fileExtended(torrentId, endpointId, torrentName, 1));
    extended.put(TorrentIds.fileId(torrentId, 2), fileExtended(torrentId, endpointId, torrentName, 2));
    MyTorrent torrent = new MyTorrent(manifest, files, base, extended);
    return torrent;
  }

  public static ManifestJSON manifestJSON() {
    ManifestJSON manifest = new ManifestJSON();
    manifest.setCreatorDate("Mon Aug 27 17:35:13 CEST 2018");
    manifest.setCreatorEmail("dummy@somemail.com");
    manifest.setDatasetDescription("this is not a description");
    manifest.setDatasetName("dataset");
    manifest.setKafkaSupport(false);
    List<FileInfoJSON> fileInfos = new ArrayList<>();
    fileInfos.add(fileInfo("file1"));
    fileInfos.add(fileInfo("file2"));
    manifest.setFileInfos(fileInfos);
    List<String> metadata = new ArrayList();
    metadata.add("metadata");
    manifest.setMetaDataJsons(metadata);
    return manifest;
  }
  
  private static FileInfoJSON fileInfo(String fileName) {
    FileInfoJSON fileInfo = new FileInfoJSON();
    fileInfo.setFileName(fileName);
    fileInfo.setLength(1024);
    fileInfo.setSchema("");
    return fileInfo;
  }
  
  private static FileBaseDetails fileBase() {
    BlockDetails defaultBlock = new BlockDetails(1024, 2, 1000, 24);
    BlockDetails lastBlock = defaultBlock;
    FileBaseDetails base = new FileBaseDetails(1024, 1, defaultBlock, lastBlock, HashUtil.getAlgName(HashUtil.SHA));
    return base;
  }
  
  private static FileExtendedDetails fileExtended(OverlayId torrentId, Identifier endpointId, String dir, int fileNr) {
    FileExtendedDetails extended = new FileExtendedDetails(){
      @Override
      public Pair<StreamId, MyStream> getMainStream() {
        StreamId streamId = TorrentIds.streamId(endpointId, TorrentIds.fileId(torrentId, fileNr));
        StreamEndpoint endpoint = new DiskEndpoint();
        StreamResource resource = new DiskResource(testPath + File.separator + dir, "file" + fileNr);
        MyStream stream = new MyStream(endpoint, resource);
        return Pair.with(streamId, stream);
      }

      @Override
      public List<Pair<StreamId, MyStream>> getSecondaryStreams() {
        return new LinkedList<>();
      }
    };
    return extended;
  }
}
