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
package se.sics.dela.storage.mngr.stream.impl;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.javatuples.Pair;
import org.slf4j.Logger;
import se.sics.dela.storage.StreamStorage;
import se.sics.dela.storage.mngr.stream.StreamMngrPort;
import se.sics.dela.storage.mngr.stream.events.StreamMngrConnect;
import se.sics.dela.storage.remove.Converter;
import se.sics.dela.util.ResultCallback;
import se.sics.kompics.ComponentProxy;
import se.sics.kompics.Handler;
import se.sics.kompics.Positive;
import se.sics.kompics.util.Identifier;
import se.sics.ktoolbox.util.identifiable.overlay.OverlayId;
import se.sics.ktoolbox.util.trysf.Try;
import se.sics.nstream.FileId;
import se.sics.nstream.StreamId;
import se.sics.nstream.transfer.MyTorrent;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class StreamMngrProxy {

  private final String logPrefix = "";

  private final ComponentProxy proxy;
  private final Logger logger;
  private final Positive<StreamMngrPort> streamMngr;

  private final Map<FileId, ResultCallback<Boolean>> fileCallbacks = new HashMap<>();
  private final Map<FileId, Set<Identifier>> fileToStream = new HashMap<>();
  private final Map<Identifier, FileId> streamToFile = new HashMap<>();

  public StreamMngrProxy(ComponentProxy proxy, Logger logger) {
    this.proxy = proxy;
    this.logger = logger;
    streamMngr = proxy.getPositive(StreamMngrPort.class);
    proxy.subscribe(handleStreamConnected, streamMngr);
  }

  public void prepareFile(Identifier clientId, FileId fileId, Pair<StreamId, StreamStorage> mainStream,
    Set<Pair<StreamId, StreamStorage>> secondaryStreams, ResultCallback<Boolean> callback) {
    fileToStream.put(fileId, new HashSet<>());
    fileCallbacks.put(fileId, callback);
    prepareStream(clientId, fileId, mainStream);
    secondaryStreams.forEach((stream) -> prepareStream(clientId, fileId, stream));
  }

  private void prepareStream(Identifier clientId, FileId fileId, Pair<StreamId, StreamStorage> stream) {
    StreamMngrConnect.Request req = new StreamMngrConnect.Request(clientId, stream.getValue0(), stream.getValue1());
    fileToStream.get(fileId).add(req.getId());
    streamToFile.put(req.getId(), fileId);
    proxy.trigger(req, streamMngr);
  }

  Handler handleStreamConnected = new Handler<StreamMngrConnect.Success>() {
    @Override
    public void handle(StreamMngrConnect.Success resp) {
      logger.info("{}prepared:{}", logPrefix, resp.req.streamId);
      FileId fileId = streamToFile.remove(resp.getId());
      Set<Identifier> pendingStreams = fileToStream.get(fileId);
      pendingStreams.remove(resp.getId());
      if(pendingStreams.isEmpty()) {
        fileToStream.remove(fileId);
        ResultCallback<Boolean> fileCallback = fileCallbacks.remove(fileId);
        fileCallback.complete(new Try.Success(true));
      } 
   }
  };
  
  public static class Old extends StreamMngrProxy {

    private final Set<FileId> pendingFiles = new HashSet<>();

    public Old(ComponentProxy proxy, Logger logger) {
      super(proxy, logger);
    }

    public void prepare(OverlayId torrentId, MyTorrent torrent, ResultCallback<Boolean> callback) {
      torrent.extended.entrySet().forEach((file) -> {
        pendingFiles.add(file.getKey());
        prepareFile(torrentId, file.getKey(),
          Converter.stream(file.getValue().getMainStream()),
          Converter.streams(file.getValue().getSecondaryStreams()),
          fileCallback(file.getKey(), callback));
      });
    }

    private ResultCallback<Boolean> fileCallback(FileId fileId, ResultCallback<Boolean> torrentCallback) {
      return (Try<Boolean> result) -> {
        pendingFiles.remove(fileId);
        if (pendingFiles.isEmpty()) {
          torrentCallback.complete(new Try.Success(true));
        }
      };
    }
  }
}
