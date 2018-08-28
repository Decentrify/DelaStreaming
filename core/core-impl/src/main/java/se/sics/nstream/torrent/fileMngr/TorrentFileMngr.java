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
package se.sics.nstream.torrent.fileMngr;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.javatuples.Pair;
import se.sics.kompics.ComponentProxy;
import se.sics.kompics.config.Config;
import se.sics.ktoolbox.util.result.DelayedExceptionSyncHandler;
import se.sics.nstream.FileId;
import se.sics.nstream.StreamId;
import se.sics.nstream.storage.AsyncIncompleteStorage;
import se.sics.nstream.storage.AsyncOnDemandHashStorage;
import se.sics.nstream.storage.buffer.KBuffer;
import se.sics.nstream.storage.buffer.MultiKBuffer;
import se.sics.nstream.storage.buffer.SimpleAppendKBuffer;
import se.sics.nstream.storage.cache.SimpleKCache;
import se.sics.nstream.storage.durable.util.FileExtendedDetails;
import se.sics.nstream.storage.durable.util.MyStream;
import se.sics.nstream.storage.managed.AppendFileMngr;
import se.sics.nstream.torrent.core.DataReport;
import se.sics.nstream.transfer.MyTorrent;
import se.sics.nstream.util.BlockHelper;
import se.sics.nstream.util.FileBaseDetails;
import se.sics.nstream.util.actuator.ComponentLoadTracking;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class TorrentFileMngr {

  private final MyTorrent torrent;
  private final TreeMap<FileId, TFileIncomplete> pending;
  private final Map<FileId, TFileIncomplete> ongoing;
  private final Map<FileId, TFileComplete> completed;

  public TorrentFileMngr(MyTorrent torrent, Map<FileId, TFileComplete> completed, 
    Map<FileId, TFileIncomplete> ongoing, TreeMap<FileId, TFileIncomplete> pending) {
    this.torrent = torrent;
    this.completed = completed;
    this.ongoing = ongoing;
    this.pending = pending;
  }

  public static TorrentFileMngr create(Config config, ComponentProxy proxy, DelayedExceptionSyncHandler exSyncHandler,
    ComponentLoadTracking loadTracker,
    MyTorrent torrent, Map<StreamId, Long> streamsInfo) {
    Map<FileId, TFileComplete> completed = new HashMap<>();
    Map<FileId, TFileIncomplete> ongoing = new HashMap<>();
    TreeMap<FileId, TFileIncomplete> pending = new TreeMap<>();

    for (Map.Entry<FileId, FileExtendedDetails> entry : torrent.extended.entrySet()) {
      Map<StreamId, Long> fileStreams = new HashMap<>();

      FileBaseDetails fileDetails = torrent.base.get(entry.getKey());
      Pair<StreamId, MyStream> mainStream = entry.getValue().getMainStream();
      fileStreams.put(mainStream.getValue0(), streamsInfo.get(mainStream.getValue0()));
      SimpleKCache cache = new SimpleKCache(config, proxy, exSyncHandler, loadTracker, mainStream);

      List<KBuffer> bufs = new ArrayList<>();
      bufs.add(new SimpleAppendKBuffer(config, proxy, exSyncHandler, loadTracker, mainStream, 0));
      for (Pair<StreamId, MyStream> secondaryStream : entry.getValue().getSecondaryStreams()) {
        fileStreams.put(secondaryStream.getValue0(), streamsInfo.get(secondaryStream.getValue0()));
        bufs.add(new SimpleAppendKBuffer(config, proxy, exSyncHandler, loadTracker, secondaryStream, 0));
      }
      KBuffer buffer = new MultiKBuffer(bufs);
      AsyncIncompleteStorage file = new AsyncIncompleteStorage(cache, buffer);
      AsyncOnDemandHashStorage hash = new AsyncOnDemandHashStorage(fileDetails, exSyncHandler, file, mainStream);

      long minPos = Collections.min(fileStreams.values());
      int minBlockNr = BlockHelper.getBlockNrFromPos(minPos, fileDetails);
      AppendFileMngr fileMngr = new AppendFileMngr(fileDetails, file, hash, minBlockNr, minBlockNr);

      if (fileMngr.isComplete()) {
        completed.put(entry.getKey(), new TFileComplete(fileMngr.complete(), fileDetails));
      } else if (fileMngr.hasBlock(0)) {
        ongoing.put(entry.getKey(), new TFileIncomplete(fileMngr, fileDetails));
      } else {
        pending.put(entry.getKey(), new TFileIncomplete(fileMngr, fileDetails));
      }
    }
    return new TorrentFileMngr(torrent, completed, ongoing, pending);
  }

  public void start() {
    completed.values().forEach((e) -> e.start());
    ongoing.values().forEach((e) -> e.start());
    pending.values().forEach((e) -> e.start());
  }

  public boolean isIdle() {
    boolean idle = true;
    for (TFileComplete e : completed.values()) {
      idle = idle && e.isIdle();
    }
    for (TFileIncomplete e : ongoing.values()) {
      idle = idle && e.isIdle();
    }
    for (TFileIncomplete e : pending.values()) {
      idle = idle && e.isIdle();
    }
    return idle;
  }

  public void close() {
    completed.values().forEach((e) -> e.close());
    ongoing.values().forEach((e) -> e.close());
    pending.values().forEach((e) -> e.close());
  }

  public boolean complete() {
    return pending.isEmpty() && ongoing.isEmpty();
  }

  public void complete(FileId fileId) {
    TFileWrite fileWriter = ongoing.remove(fileId);
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

  public TFileRead readFrom(FileId fileId) {
    TFileRead transferMngr = completed.get(fileId);
    if (transferMngr == null) {
      transferMngr = ongoing.get(fileId);
    }
    return transferMngr;
  }

  public TFileWrite writeTo(FileId fileId) {
    return ongoing.get(fileId);
  }

  public Pair<FileId, Map<StreamId, MyStream>> nextPending() {
    Map.Entry<FileId, TFileIncomplete> next = pending.pollFirstEntry();
    ongoing.put(next.getKey(), next.getValue());
    Map<StreamId, MyStream> resources = resources(next.getKey());
    return Pair.with(next.getKey(), resources);
  }

  public Map<StreamId, MyStream> resources(FileId fileId) {
    FileExtendedDetails details = torrent.extended.get(fileId);
    if (details == null) {
      throw new RuntimeException("ups");
    }
    Map<StreamId, MyStream> result = new HashMap<>();
    Pair<StreamId, MyStream> mainStream = details.getMainStream();
    result.put(mainStream.getValue0(), mainStream.getValue1());
    for (Pair<StreamId, MyStream> secondaryStream : details.getSecondaryStreams()) {
      result.put(secondaryStream.getValue0(), secondaryStream.getValue1());
    }
    return result;
  }

  public DataReport report() {
    long totalMaxSize = 0;
    long totalCurrentSize = 0;
    Map<FileId, Pair<Long, Long>> ongoingReport = new HashMap<>();
    Map<FileId, Long> completedReport = new TreeMap<>();

    for (Map.Entry<FileId, TFileComplete> completedFile : completed.entrySet()) {
      Pair<Long, Long> size = completedFile.getValue().report();
      totalMaxSize += size.getValue0();
      totalCurrentSize += size.getValue1();
      completedReport.put(completedFile.getKey(), size.getValue1());
    }
    for (Map.Entry<FileId, TFileIncomplete> file : ongoing.entrySet()) {
      Pair<Long, Long> size = file.getValue().report();
      totalMaxSize += size.getValue0();
      totalCurrentSize += size.getValue1();
      ongoingReport.put(file.getKey(), size);
    }
    for (TFileIncomplete pendingFile : pending.values()) {
      Pair<Long, Long> size = pendingFile.report();
      totalMaxSize += size.getValue0();
      totalCurrentSize += size.getValue1();
    }
    return new DataReport(torrent, Pair.with(totalMaxSize, totalCurrentSize), new HashMap<>(completedReport),
      ongoingReport, new HashSet<>(pending.keySet()));
  }
}
