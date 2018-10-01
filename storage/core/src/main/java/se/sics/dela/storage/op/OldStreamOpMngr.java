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
package se.sics.dela.storage.op;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.javatuples.Pair;
import org.slf4j.Logger;
import se.sics.dela.storage.StreamStorage;
import se.sics.dela.storage.buffer.KBuffer;
import se.sics.dela.storage.buffer.append.SimpleAppendBuffer;
import se.sics.dela.storage.buffer.nx.MultiKBuffer;
import se.sics.dela.storage.cache.SimpleCache;
import se.sics.dela.storage.op.util.AppendFileMngr;
import se.sics.dela.storage.op.util.AsyncIncompleteStorage;
import se.sics.dela.storage.op.util.AsyncOnDemandHashStorage;
import se.sics.dela.storage.operation.StreamStorageOpProxy;
import se.sics.dela.storage.remove.Converter;
import se.sics.dela.util.ResultCallback;
import se.sics.dela.util.TimerProxy;
import se.sics.kompics.ComponentProxy;
import se.sics.kompics.config.Config;
import se.sics.ktoolbox.util.identifiable.overlay.OverlayId;
import se.sics.ktoolbox.util.reference.KReferenceException;
import se.sics.ktoolbox.util.result.DelayedExceptionSyncHandler;
import se.sics.nstream.FileId;
import se.sics.nstream.StreamId;
import se.sics.nstream.storage.durable.util.FileExtendedDetails;
import se.sics.nstream.torrent.core.DataReport;
import se.sics.nstream.transfer.MyTorrent;
import se.sics.nstream.util.BlockHelper;
import se.sics.nstream.util.FileBaseDetails;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class OldStreamOpMngr {

  private final StorageOpProxy proxy;
  private final OverlayId torrentId;
  private final MyTorrent torrent;
  private final TreeMap<FileId, StreamOngoing> pending;
  private final Map<FileId, StreamOngoing> ongoing;
  private final Map<FileId, StreamComplete> completed;

  public OldStreamOpMngr(OverlayId torrentId, MyTorrent torrent, StorageOpProxy proxy,
    Map<FileId, StreamComplete> completed, Map<FileId, StreamOngoing> ongoing, TreeMap<FileId, StreamOngoing> pending) {
    this.proxy = proxy;
    this.torrentId = torrentId;
    this.torrent = torrent;
    this.completed = completed;
    this.ongoing = ongoing;
    this.pending = pending;
  }

  public static OldStreamOpMngr create(Config config, ComponentProxy proxy, DelayedExceptionSyncHandler exSyncHandler,
    OverlayId torrentId, MyTorrent torrent, Map<StreamId, Long> streamsInfo, Logger logger) throws KReferenceException {
    Map<FileId, StreamComplete> completed = new HashMap<>();
    Map<FileId, StreamOngoing> ongoing = new HashMap<>();
    TreeMap<FileId, StreamOngoing> pending = new TreeMap<>();

    for (Map.Entry<FileId, FileExtendedDetails> entry : torrent.extended.entrySet()) {
      Map<StreamId, Long> fileStreams = new HashMap<>();

      StreamStorageOpProxy storageProxy = new StreamStorageOpProxy().setup(proxy);
      TimerProxy timerProxy = new TimerProxy().setup(proxy);

      FileBaseDetails fileDetails = torrent.base.get(entry.getKey());
      Pair<StreamId, StreamStorage> mainStream = Converter.stream(entry.getValue().getMainStream());
      fileStreams.put(mainStream.getValue0(), streamsInfo.get(mainStream.getValue0()));
      SimpleCache cache = new SimpleCache(config, storageProxy, timerProxy, mainStream.getValue0(), logger);

      List<KBuffer> bufs = new ArrayList<>();

      bufs.add(new SimpleAppendBuffer(storageProxy, mainStream.getValue0(), 0, 0));
      entry.getValue().getSecondaryStreams().forEach((stream) -> {
        fileStreams.put(stream.getValue0(), streamsInfo.get(stream.getValue0()));
        bufs.add(new SimpleAppendBuffer(storageProxy, Converter.stream(stream).getValue0(), 0, 0));
      });
      KBuffer buffer = new MultiKBuffer(bufs);
      AsyncIncompleteStorage file = new AsyncIncompleteStorage(cache, buffer);
      AsyncOnDemandHashStorage hash = new AsyncOnDemandHashStorage(fileDetails, file);

      long minPos = Collections.min(fileStreams.values());
      int minBlockNr = BlockHelper.getBlockNrFromPos(minPos, fileDetails);
      AppendFileMngr fileMngr = new AppendFileMngr(fileDetails, file, hash, minBlockNr, minBlockNr);

      if (fileMngr.isComplete()) {
        completed.put(entry.getKey(), new StreamComplete(fileMngr.complete(), fileDetails));
      } else if (fileMngr.hasBlock(0)) {
        ongoing.put(entry.getKey(), new StreamOngoing(fileMngr, fileDetails));
      } else {
        pending.put(entry.getKey(), new StreamOngoing(fileMngr, fileDetails));
      }
    }
    StorageOpProxy.Old streamOpProxy = new StorageOpProxy.Old();
    streamOpProxy.setup(proxy, logger);
    return new OldStreamOpMngr(torrentId, torrent, streamOpProxy, completed, ongoing, pending);
  }

  public void setupStreams(ResultCallback<Boolean> callback) {
    if (proxy instanceof StorageOpProxy.Old) {
      ((StorageOpProxy.Old)proxy).prepare(torrentId, torrent, callback);
    }
  }

  public void startStreams() {
    completed.values().forEach((e) -> e.start());
    ongoing.values().forEach((e) -> e.start());
    pending.values().forEach((e) -> e.start());
  }

  public boolean isIdle() {
    boolean idle = true;
    for (StreamComplete e : completed.values()) {
      idle = idle && e.isIdle();
    }
    for (StreamOngoing e : ongoing.values()) {
      idle = idle && e.isIdle();
    }
    for (StreamOngoing e : pending.values()) {
      idle = idle && e.isIdle();
    }
    return idle;
  }

  public void close() throws KReferenceException {
    for (StreamComplete stream : completed.values()) {
      stream.close();
    }
    for (StreamOngoing stream : ongoing.values()) {
      stream.close();
    }
    for (StreamOngoing stream : pending.values()) {
      stream.close();
    }
  }

  public boolean complete() {
    return pending.isEmpty() && ongoing.isEmpty();
  }

  public void complete(FileId fileId) throws KReferenceException {
    StreamWrite fileWriter = ongoing.remove(fileId);
    if (fileWriter == null || !fileWriter.isComplete()) {
      throw new RuntimeException("ups");
    }
    completed.put(fileId, fileWriter.complete());
  }

  public boolean hasOngoing() {
    return !ongoing.isEmpty();
  }

  public boolean hasPending() {
    return !pending.isEmpty();
  }

  public StreamRead readFrom(FileId fileId) {
    StreamRead transferMngr = completed.get(fileId);
    if (transferMngr == null) {
      transferMngr = ongoing.get(fileId);
    }
    return transferMngr;
  }

  public StreamWrite writeTo(FileId fileId) {
    return ongoing.get(fileId);
  }

  public Pair<FileId, Map<StreamId, StreamStorage>> nextPending() {
    Map.Entry<FileId, StreamOngoing> next = pending.pollFirstEntry();
    ongoing.put(next.getKey(), next.getValue());
    Map<StreamId, StreamStorage> resources = resources(next.getKey());
    return Pair.with(next.getKey(), resources);
  }

  public Map<StreamId, StreamStorage> resources(FileId fileId) {
    FileExtendedDetails details = torrent.extended.get(fileId);
    if (details == null) {
      throw new RuntimeException("ups");
    }
    Map<StreamId, StreamStorage> result = new HashMap<>();
    Pair<StreamId, StreamStorage> mainStream = Converter.stream(details.getMainStream());
    result.put(mainStream.getValue0(), mainStream.getValue1());
    details.getSecondaryStreams().forEach((stream) -> {
      Pair<StreamId, StreamStorage> s = Converter.stream(stream);
      result.put(s.getValue0(), s.getValue1());
    });
    return result;
  }

  public DataReport report() {
    long totalMaxSize = 0;
    long totalCurrentSize = 0;
    Map<FileId, Pair<Long, Long>> ongoingReport = new HashMap<>();
    Map<FileId, Long> completedReport = new TreeMap<>();

    for (Map.Entry<FileId, StreamComplete> completedFile : completed.entrySet()) {
      Pair<Long, Long> size = completedFile.getValue().report();
      totalMaxSize += size.getValue0();
      totalCurrentSize += size.getValue1();
      completedReport.put(completedFile.getKey(), size.getValue1());
    }
    for (Map.Entry<FileId, StreamOngoing> file : ongoing.entrySet()) {
      Pair<Long, Long> size = file.getValue().report();
      totalMaxSize += size.getValue0();
      totalCurrentSize += size.getValue1();
      ongoingReport.put(file.getKey(), size);
    }
    for (StreamOngoing pendingFile : pending.values()) {
      Pair<Long, Long> size = pendingFile.report();
      totalMaxSize += size.getValue0();
      totalCurrentSize += size.getValue1();
    }
    return new DataReport(torrent, Pair.with(totalMaxSize, totalCurrentSize), new HashMap<>(completedReport),
      ongoingReport, new HashSet<>(pending.keySet()));
  }
}
