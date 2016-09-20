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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.javatuples.Pair;
import se.sics.kompics.ComponentProxy;
import se.sics.kompics.config.Config;
import se.sics.ktoolbox.util.identifiable.Identifier;
import se.sics.ktoolbox.util.result.DelayedExceptionSyncHandler;
import se.sics.nstream.storage.AsyncCompleteStorage;
import se.sics.nstream.storage.AsyncIncompleteStorage;
import se.sics.nstream.storage.AsyncOnDemandHashStorage;
import se.sics.nstream.storage.buffer.KBuffer;
import se.sics.nstream.storage.buffer.MultiKBuffer;
import se.sics.nstream.storage.buffer.SimpleAppendKBuffer;
import se.sics.nstream.storage.cache.SimpleKCache;
import se.sics.nstream.storage.managed.AppendFileMngr;
import se.sics.nstream.storage.managed.CompleteFileMngr;
import se.sics.nstream.torrent.util.BufferName;
import se.sics.nstream.transfer.MyTorrent;
import se.sics.nstream.util.BlockDetails;
import se.sics.nstream.util.FileBaseDetails;
import se.sics.nstream.util.FileExtendedDetails;
import se.sics.nstream.util.StreamEndpoint;
import se.sics.nstream.util.StreamResource;
import se.sics.nstream.util.actuator.ComponentLoadTracking;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class TorrentFileMngr {
    private final MyTorrent torrent;
    private final Map<Integer, String> idToString = new HashMap<>();
    private final Map<Integer, BlockDetails> defaultBlocks = new HashMap<>();
    private final Map<Integer, TFileComplete> completed = new HashMap<>();
    private final Map<Integer, TFileIncomplete> ongoing = new HashMap<>();
    private final TreeMap<Integer, TFileIncomplete> pending = new TreeMap<>();
    
    public TorrentFileMngr(Config config, ComponentProxy proxy, DelayedExceptionSyncHandler exSyncHandler, ComponentLoadTracking loadTracker,
           MyTorrent torrent, boolean complete) {
        this.torrent = torrent;
        int id = 1; //0 is def phase
        for (Map.Entry<String, FileExtendedDetails> entry : torrent.extended.entrySet()) {
            FileBaseDetails fileDetails = torrent.base.get(entry.getKey());
            Pair<StreamEndpoint, StreamResource> mainResource = entry.getValue().getMainResource();
            List<Pair<StreamEndpoint, StreamResource>> secondaryResources = entry.getValue().getSecondaryResource();
            if (complete) {
                SimpleKCache cache = new SimpleKCache(config, proxy, exSyncHandler, loadTracker, mainResource.getValue0(), mainResource.getValue1());
                AsyncCompleteStorage file = new AsyncCompleteStorage(cache);
                AsyncOnDemandHashStorage hash = new AsyncOnDemandHashStorage(fileDetails, exSyncHandler, file, true);
                CompleteFileMngr fileMngr = new CompleteFileMngr(fileDetails, file, hash);
                completed.put(id, new TFileComplete(fileMngr, fileDetails));
                idToString.put(id, entry.getKey());
                defaultBlocks.put(id, fileDetails.defaultBlock);
                id++;
            } else {
                SimpleKCache cache = new SimpleKCache(config, proxy, exSyncHandler, loadTracker, mainResource.getValue0(), mainResource.getValue1());
                List<KBuffer> bufs = new ArrayList<>();
                BufferName bufName = new BufferName(id, entry.getKey(), mainResource.getValue1().getSinkName());
                bufs.add(new SimpleAppendKBuffer(config, proxy, exSyncHandler, loadTracker, mainResource.getValue0(), mainResource.getValue1(), bufName, 0));
                for (Pair<StreamEndpoint, StreamResource> writeResource : secondaryResources) {
                    BufferName bName = new BufferName(id, entry.getKey(), writeResource.getValue1().getSinkName());
                    bufs.add(new SimpleAppendKBuffer(config, proxy, exSyncHandler, loadTracker, writeResource.getValue0(), writeResource.getValue1(), bName, 0));
                }
                KBuffer buffer = new MultiKBuffer(bufs);
                AsyncIncompleteStorage file = new AsyncIncompleteStorage(cache, buffer);
                AsyncOnDemandHashStorage hash = new AsyncOnDemandHashStorage(fileDetails, exSyncHandler, file, false);
                AppendFileMngr fileMngr = new AppendFileMngr(fileDetails, file, hash);
                pending.put(id, new TFileIncomplete(fileMngr, fileDetails));
                idToString.put(id, entry.getKey());
                defaultBlocks.put(id, fileDetails.defaultBlock);
                id++;
            }
        }
    }
    
    public void start() {
        for (TFileComplete e : completed.values()) {
            e.start();
        }
        for (TFileIncomplete e : ongoing.values()) {
            e.start();
        }
        for (TFileIncomplete e : pending.values()) {
            e.start();
        }
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
        for (TFileComplete e : completed.values()) {
            e.close();
        }
        for (TFileIncomplete e : ongoing.values()) {
            e.close();
        }
        for (TFileIncomplete e : pending.values()) {
            e.close();
        }
    }
    
    public boolean complete() {
        return pending.isEmpty()&&ongoing.isEmpty();
    }
    
    public void complete(int fileId) {
        TFileWrite fileWriter = ongoing.remove(fileId);
        if(fileWriter == null || !fileWriter.isComplete()) {
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
    
    public BlockDetails getDefaultBlock(int fileId) {
        return defaultBlocks.get(fileId);
    }
    
    public TFileRead readFrom(int fileId) {
        TFileRead transferMngr = completed.get(fileId);
        if (transferMngr == null) {
            transferMngr = ongoing.get(fileId);
        }
        return transferMngr;
    }

    public TFileWrite writeTo(int fileId) {
        return ongoing.get(fileId);
    }

    public Pair<Integer, Map<Identifier, StreamResource>> nextPending() {
        Map.Entry<Integer, TFileIncomplete> next = pending.pollFirstEntry();
        ongoing.put(next.getKey(), next.getValue());
        Map<Identifier, StreamResource> resources = resources(next.getKey());
        return Pair.with(next.getKey(), resources);
    }
    
    public Map<Identifier, StreamResource> resources(int fileId) {
        String fileName = idToString.get(fileId);
        FileExtendedDetails details= torrent.extended.get(fileName);
        if(details == null) {
            throw new RuntimeException("ups");
        }
        Map<Identifier, StreamResource> result = new HashMap<>();
        result.put(details.getMainResource().getValue0().getEndpointId(), details.getMainResource().getValue1());
        for(Pair<StreamEndpoint, StreamResource> resource: details.getSecondaryResource()) {
            result.put(resource.getValue0().getEndpointId(), resource.getValue1());
        }
        return result;
    }
}
