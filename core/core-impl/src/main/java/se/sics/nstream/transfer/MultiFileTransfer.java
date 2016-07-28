/*
 * Copyright (C) 2009 Swedish Institute of Computer Science (SICS) Copyright (C)
 * 2009 Royal Institute of Technology (KTH)
 *
 * KompicsToolbox is free software; you can redistribute it and/or
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
package se.sics.nstream.transfer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.javatuples.Pair;
import se.sics.kompics.ComponentProxy;
import se.sics.kompics.config.Config;
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
import se.sics.nstream.util.FileBaseDetails;
import se.sics.nstream.util.FileExtendedDetails;
import se.sics.nstream.util.StreamControl;
import se.sics.nstream.util.StreamEndpoint;
import se.sics.nstream.util.StreamResource;
import se.sics.nstream.util.TransferDetails;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class MultiFileTransfer implements StreamControl {

    private final Map<String, UploadTransferMngr> completed = new HashMap<>();
    private final Map<String, DownloadTransferMngr> pendingComplete = new HashMap<>();
    private final TreeMap<String, DownloadTransferMngr> ongoing = new TreeMap<>();

    public MultiFileTransfer(Config config, ComponentProxy proxy, DelayedExceptionSyncHandler exSyncHandler,
            TransferDetails transferDetails, boolean complete) {
        for (Map.Entry<String, FileExtendedDetails> entry : transferDetails.extended.entrySet()) {
            FileBaseDetails fileDetails = transferDetails.base.baseDetails.get(entry.getKey());
            Pair<StreamEndpoint, StreamResource> mainResource = entry.getValue().getMainResource();
            List<Pair<StreamEndpoint, StreamResource>> secondaryResources = entry.getValue().getSecondaryResource();
            if (complete) {
                SimpleKCache cache = new SimpleKCache(config, proxy, exSyncHandler, mainResource.getValue0(), mainResource.getValue1());
                AsyncCompleteStorage file = new AsyncCompleteStorage(cache);
                AsyncOnDemandHashStorage hash = new AsyncOnDemandHashStorage(fileDetails, exSyncHandler, file);
                CompleteFileMngr fileMngr = new CompleteFileMngr(fileDetails, file, hash);
                completed.put(entry.getKey(), new UploadTransferMngr(fileDetails, fileMngr));
            } else {
                SimpleKCache cache = new SimpleKCache(config, proxy, exSyncHandler, mainResource.getValue0(), mainResource.getValue1());
                List<KBuffer> bufs = new ArrayList<>();
                bufs.add(new SimpleAppendKBuffer(config, proxy, exSyncHandler, mainResource.getValue0(), mainResource.getValue1(), 0));
                for (Pair<StreamEndpoint, StreamResource> writeResource : secondaryResources) {
                    bufs.add(new SimpleAppendKBuffer(config, proxy, exSyncHandler, writeResource.getValue0(), writeResource.getValue1(), 0));
                }
                KBuffer buffer = new MultiKBuffer(bufs);
                AsyncIncompleteStorage file = new AsyncIncompleteStorage(cache, buffer);
                AsyncOnDemandHashStorage hash = new AsyncOnDemandHashStorage(fileDetails, exSyncHandler, file);
                AppendFileMngr fileMngr = new AppendFileMngr(fileDetails, file, hash);
                ongoing.put(entry.getKey(), new DownloadTransferMngr(fileDetails, fileMngr));
            }
        }
    }

    @Override
    public void start() {
        for (UploadTransferMngr e : completed.values()) {
            e.start();
        }
        for (DownloadTransferMngr e : ongoing.values()) {
            e.start();
        }
    }

    @Override
    public boolean isIdle() {
        boolean idle = true;
        for (UploadTransferMngr e : completed.values()) {
            idle = idle && e.isIdle();
        }
        for (DownloadTransferMngr e : ongoing.values()) {
            idle = idle && e.isIdle();
        }
        for (DownloadTransferMngr e : pendingComplete.values()) {
            idle = idle && e.isIdle();
        }
        return idle;
    }

    @Override
    public void close() {
        for (UploadTransferMngr e : completed.values()) {
            e.close();
        }
        for (DownloadTransferMngr e : ongoing.values()) {
            e.close();
        }
        for (DownloadTransferMngr e : pendingComplete.values()) {
            e.close();
        }
    }

    public void complete(String file) {
        DownloadTransferMngr ongoingFileMngr = pendingComplete.remove(file);
        if (ongoingFileMngr == null) {
            ongoingFileMngr = ongoing.remove(file);
        }
        UploadTransferMngr completedFileMngr = ongoingFileMngr.complete();
        completed.put(file, completedFileMngr);
    }

    public TransferMngr.Reader readFrom(int file) {
        TransferMngr.Reader transferMngr = completed.get(file);
        if (transferMngr == null) {
            transferMngr = pendingComplete.get(file);
        }
        if (transferMngr == null) {
            transferMngr = ongoing.get(file);
        }
        return transferMngr;
    }

    public TransferMngr.Writer writeTo(int file) {
        TransferMngr.Writer transferMngr = ongoing.get(file);
        if (transferMngr == null) {
            transferMngr = pendingComplete.get(file);
        }
        return transferMngr;
    }

    public void pendingComplete(String file) {
        pendingComplete.put(file, ongoing.remove(file));
    }

    public Pair<String, TransferMngr.Writer> nextOngoing() {
        Pair<String, TransferMngr.Writer> next = Pair.with(ongoing.firstEntry().getKey(), (TransferMngr.Writer) ongoing.firstEntry().getValue());
        return next;
    }
}
