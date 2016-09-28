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
package se.sics.nstream.torrent.resourceMngr;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.Handler;
import se.sics.kompics.Negative;
import se.sics.kompics.Positive;
import se.sics.kompics.Start;
import se.sics.ktoolbox.util.identifiable.Identifier;
import se.sics.ktoolbox.util.identifiable.overlay.OverlayId;
import se.sics.nstream.FileId;
import se.sics.nstream.storage.StorageControl;
import se.sics.nstream.storage.StorageControlPort;
import se.sics.nstream.torrent.StorageProvider;
import se.sics.nstream.util.FileExtendedDetails;
import se.sics.nstream.util.MyStream;

/**
 *
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class TorrentResourceMngrComp extends ComponentDefinition {
    private static final Logger LOG = LoggerFactory.getLogger(TorrentResourceMngrComp.class);
    private String logPrefix;
    
    private final Negative<TorrentResourceMngrPort> mngrPort = provides(TorrentResourceMngrPort.class);
    private final Map<Identifier, Positive> storageControlPorts = new HashMap<>();
    //**************************************************************************
    private final OverlayId torrentId;
    //**************************************************************************
    private PrepareResources.Request prepare;
    private Set<Identifier> pendingResources = new HashSet<>();
    
    public TorrentResourceMngrComp(Init init) {
        torrentId = init.torrentId;
        logPrefix = "<" + torrentId + ">";

        prepareStorageControl(init.storageProvider);
        
        subscribe(handleStart, control);
        subscribe(handlePrepare, mngrPort);
    }
    
    private void prepareStorageControl(StorageProvider storageProvider) {
        for(Map.Entry<Identifier, Class<? extends StorageControlPort>> storageControl : storageProvider.requiredStorageControlPorts().entrySet()) {
            Positive storageControlPort = requires(storageControl.getValue());
            storageControlPorts.put(storageControl.getKey(), storageControlPort);
            subscribe(handleResourceReady, storageControlPort);
        }
    }
    
    Handler handleStart = new Handler<Start>() {
        @Override
        public void handle(Start event) {
            LOG.info("{}starting...", logPrefix);
        }
    };
    
    Handler handlePrepare = new Handler<PrepareResources.Request>() {
        @Override
        public void handle(PrepareResources.Request req) {
            if(prepare !=  null) {
                throw new RuntimeException("ups");
            }
            prepare = req;
            for(Map.Entry<FileId, FileExtendedDetails> e : req.torrent.extended.entrySet()) {
                MyStream mainStream = e.getValue().getMainStream();
                prepareResource(e.getKey(), mainStream);
                for(MyStream secondaryStreams : e.getValue().getSecondaryStreams()){
                    prepareResource(e.getKey(), secondaryStreams);
                }
            }
        }
    };
    
    private void prepareResource(FileId fileId, MyStream stream) {
        LOG.info("{}preparing file:{} resource:{} endpoint:{}", new Object[]{logPrefix, fileId, stream.resource.getSinkName(), stream.endpoint.getEndpointName()});
        
        Positive storageControlPort = storageControlPorts.get(stream.streamId.endpointId);
        if(storageControlPort == null) {
            throw new RuntimeException("ups");
        }
        StorageControl.OpenRequest req = new StorageControl.OpenRequest(stream);
        pendingResources.add(req.getId());
        trigger(req, storageControlPort);
    } 
    
    Handler handleResourceReady = new Handler<StorageControl.OpenSuccess>() {
        @Override
        public void handle(StorageControl.OpenSuccess resp) {
            LOG.info("{}prepared resource:{}", new Object[]{logPrefix, resp.req.stream.resource.getSinkName()});
            if(!pendingResources.remove(resp.getId())) {
                throw new RuntimeException("ups");
            }
            if(pendingResources.isEmpty()) {
                LOG.info("{}finished preparing resources", logPrefix);
                answer(prepare, prepare.success());
            }
        }
    };
    
    public static class Init extends se.sics.kompics.Init<TorrentResourceMngrComp> {
        public final OverlayId torrentId;
        public final StorageProvider storageProvider;
        
        public Init(OverlayId torrentId, StorageProvider storageProvider) {
            this.torrentId = torrentId;
            this.storageProvider = storageProvider;
        }
    }
}
