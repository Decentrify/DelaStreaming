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
package se.sics.dela.storage.op.util;

import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import se.sics.dela.storage.buffer.KBuffer;
import se.sics.dela.storage.buffer.append.SimpleAppendBuffer;
import se.sics.dela.storage.buffer.nx.MultiKBuffer;
import se.sics.dela.storage.cache.SimpleCache;
import se.sics.dela.storage.operation.StreamStorageOpProxy;
import se.sics.kompics.config.Config;
import se.sics.ktoolbox.nutil.timer.TimerProxy;
import se.sics.ktoolbox.nutil.timer.TimerProxyImpl;
import se.sics.nstream.FileId;
import se.sics.nstream.StreamId;
import se.sics.nstream.storage.durable.util.FileExtendedDetails;
import se.sics.nstream.transfer.MyTorrent;
import se.sics.nstream.util.BlockHelper;
import se.sics.nstream.util.FileBaseDetails;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class FileMngrHelper {
  public static AppendFileMngr fileMngr(MyTorrent torrent, FileId fileId, long writePos, Conf conf) {
    FileBaseDetails baseDetails = torrent.base.get(fileId);
    FileExtendedDetails extendedDetails = torrent.extended.get(fileId);
    int writeBlock = BlockHelper.getBlockNrFromPos(writePos, baseDetails);
    SimpleCache cache = cache(extendedDetails, conf);
    KBuffer buffer = buffer(extendedDetails, writePos, writeBlock, conf);
    AsyncIncompleteStorage file = new AsyncIncompleteStorage(cache, buffer);
    AsyncOnDemandHashStorage hash = new AsyncOnDemandHashStorage(baseDetails, file);
    AppendFileMngr fileMngr = new AppendFileMngr(baseDetails, file, hash, writeBlock, writeBlock);
    return fileMngr;
  }

  public static SimpleCache cache(FileExtendedDetails extendedDetails, Conf conf) {
    StreamId mainStreamId = extendedDetails.getMainStream().getValue0();
    SimpleCache cache 
      = new SimpleCache(conf.config, conf.streamStorageOpProxy, conf.timerProxy, mainStreamId, conf.logger);
    return cache;
  }

  public static KBuffer buffer(FileExtendedDetails extendedDetails, long writePos, int writeBlock, 
    Conf conf) {
    List<KBuffer> bufs = new ArrayList<>();
    StreamId mainStreamId = extendedDetails.getMainStream().getValue0();
    bufs.add(new SimpleAppendBuffer(conf.streamStorageOpProxy, mainStreamId, writePos, writeBlock));
    extendedDetails.getSecondaryStreams().forEach((stream) -> {
      bufs.add(new SimpleAppendBuffer(conf.streamStorageOpProxy, stream.getValue0(), writePos, writeBlock));
    });
    return new MultiKBuffer(bufs);
  }
  
  public static class Conf {
    public final Config config;
    public final Logger logger;
    public final StreamStorageOpProxy streamStorageOpProxy;
    public final TimerProxy timerProxy;

    public Conf(Config config, Logger logger, StreamStorageOpProxy streamStorageOpProxy, 
      TimerProxyImpl timerProxy) {
      this.config = config;
      this.logger = logger;
      this.streamStorageOpProxy = streamStorageOpProxy;
      this.timerProxy = timerProxy;
    }
  }
}
