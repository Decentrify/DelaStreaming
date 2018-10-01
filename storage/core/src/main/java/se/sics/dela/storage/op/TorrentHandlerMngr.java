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

import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.Consumer;
import org.javatuples.Pair;
import org.slf4j.Logger;
import se.sics.dela.storage.StreamStorage;
import se.sics.dela.storage.mngr.stream.impl.StreamMngrProxy;
import se.sics.dela.storage.remove.Converter;
import se.sics.kompics.config.Config;
import se.sics.ktoolbox.util.TupleHelper;
import se.sics.ktoolbox.util.identifiable.overlay.OverlayId;
import se.sics.nstream.FileId;
import se.sics.nstream.StreamId;
import se.sics.nstream.transfer.MyTorrent;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class TorrentHandlerMngr {

  private final Config config;
  private final Logger logger;
  private final StreamMngrProxy streamMngrProxy;
  
  private final Map<OverlayId, TorrentHandler> torrents = new HashMap<>();
  
  public TorrentHandlerMngr(Config config, Logger logger, StreamMngrProxy streamMngrProxy) {
    this.streamMngrProxy = streamMngrProxy;
    this.config = config;
    this.logger = logger;
  }

  public void setup(OverlayId torrentId, MyTorrent torrent) {
    TorrentHandler files = new TorrentHandler(torrentId, torrent);
    torrents.put(torrentId, files);
  }

  public TorrentHandler getTorrent(OverlayId torrentId) {
    return torrents.get(torrentId);
  }
  
  public int filesPending(OverlayId torrentId) {
    return torrents.get(torrentId).pendingNr;
  }

  public int filesWriting(OverlayId torrentId) {
    return torrents.get(torrentId).writingNr;
  }

  public int filesReading(OverlayId torrentId) {
    return torrents.get(torrentId).readingNr;
  }

  public static enum FileStatus {
    PENDING,
    PENDING_CONN,
    WRITE,
    WRITE_DISC,
    READ,
    READ_DISC,
    IDLE,
    READ_CONN
  }

  public class TorrentHandler {

    private final OverlayId torrentId;
    private final MyTorrent torrent;

    private final Map<FileId, FileStatus> files = new TreeMap<>();
    private final TreeSet<FileId> pending = new TreeSet<>();
    private int pendingNr;
    private int writingNr;
    private int readingNr;

    public TorrentHandler(OverlayId torrentId, MyTorrent torrent) {
      this.torrentId = torrentId;
      this.torrent = torrent;
      setup();
    }

    private void setup() {
      torrent.base.keySet().forEach((fileId) -> pending.add(fileId));
      pendingNr = pending.size();
      writingNr = 0;
      readingNr = 0;
    }

    public boolean hasPending() {
      return !pending.isEmpty();
    }
    
    public boolean hasWritting() {
      return writingNr == 0;
    }
    
    public FileId prepareNextPending(Consumer<FileReady> fileReady) {
      FileId fileId = pending.pollFirst();
      Map<StreamId, StreamStorage> readWrite = Converter.readWrite(torrent, fileId);
      Map<StreamId, StreamStorage> writeOnly = Converter.writeOnly(torrent, fileId);
      streamMngrProxy.prepareFile(torrentId, fileId, readWrite, writeOnly);
      streamMngrProxy.setupWrite(torrentId, fileId, pendingCallback(fileId, fileReady));
      pendingNr--;
      writingNr++;
      files.put(fileId, FileStatus.PENDING_CONN);
      return fileId;
    }

    private TupleHelper.PairConsumer<Pair<String, Long>, Pair<String, Long>> pendingCallback(FileId fileId,
      Consumer<FileReady> fileReady) {
      return new TupleHelper.PairConsumer<Pair<String, Long>, Pair<String, Long>>() {
        @Override
        public void accept(Pair<String, Long> readPos, Pair<String, Long> writePos) {
          files.put(fileId, FileStatus.WRITE);
          fileReady.accept(new FileReady(fileId, writePos.getValue1()));
        }
      };
    }

    public void closeWrite(FileId fileId, Consumer<Boolean> callback) {
      writingNr--;
      readingNr++;
      files.put(fileId, FileStatus.WRITE_DISC);
      streamMngrProxy.closeWrite(torrentId, fileId, (result) -> {
        files.put(fileId, FileStatus.READ);
        callback.accept(result);
      });
    }

    public void closeRead(FileId fileId, Consumer<Boolean> callback) {
      readingNr--;
      files.put(fileId, FileStatus.READ_DISC);
      streamMngrProxy.closeRead(torrentId, fileId, (result) -> {
        files.put(fileId, FileStatus.IDLE);
        callback.accept(result);
      });
    }

    public void restartRead(FileId fileId, Consumer<Boolean> callback) {
      readingNr++;
      files.put(fileId, FileStatus.READ_CONN);
      streamMngrProxy.setupRead(torrentId, fileId, (result) -> {
        files.put(fileId, FileStatus.READ);
        callback.accept(result);
      });
    }
  }

  public static class FileReady {

    public final FileId fileId;
    public final long writePos;

    public FileReady(FileId fileId, long writePos) {
      this.fileId = fileId;
      this.writePos = writePos;
    }
  }

//  private TupleHelper.PairConsumer<Pair<String, Long>, Pair<String, Long>> pendingCallbackFile(FileId fileId,
//    Consumer<Try<FileReady>> fileReady) {
//    return new TupleHelper.PairConsumer<Pair<String, Long>, Pair<String, Long>>() {
//      @Override
//      public void accept(Pair<String, Long> readPos, Pair<String, Long> writePos) {
//        pendingConn.remove(fileId);
//        FileBaseDetails baseDetails = torrent.base.get(fileId);
//        FileMngrHelper.Conf conf = new FileMngrHelper.Conf(config, logger, streamStorageOpProxy, timerProxy);
//        AppendFileMngr fileMngr = FileMngrHelper.fileMngr(torrent, fileId, writePos.getValue1(), conf);
//        FileReady fr = new FileReady(fileId, writePos.getValue1());
//        if (writePos.getValue1() == baseDetails.length) {
//          try {
//            StreamComplete sc = new StreamComplete(fileMngr.complete(), baseDetails);
//            completedReadOnly.put(fileId, sc);
//          } catch (KReferenceException ex) {
//            fileReady.accept(new Try.Failure(ex));
//          }
//        } else {
//          StreamOngoing so = new StreamOngoing(fileMngr, baseDetails);
//          downloading.put(fileId, so);
//        }
//        fileReady.accept(new Try.Success(fr));
//      }
//    };
//  }
}
