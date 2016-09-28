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
package se.sics.nstream.storage;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.kompics.Component;
import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.Handler;
import se.sics.kompics.Kill;
import se.sics.kompics.Killed;
import se.sics.kompics.Negative;
import se.sics.kompics.Start;
import se.sics.ktoolbox.util.identifiable.overlay.OverlayId;
import se.sics.ktoolbox.util.idextractor.EventOverlayIdExtractor;
import se.sics.ktoolbox.util.network.ports.One2NChannel;
import se.sics.nstream.StreamId;
import se.sics.nstream.util.StreamEndpoint;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class StorageMngrComp extends ComponentDefinition {

    private final static Logger LOG = LoggerFactory.getLogger(StorageMngrComp.class);
    private String logPrefix = "";

    private final Negative<StorageControlPort> storageControlPort;
    private final Negative storagePort;
    private final One2NChannel storageChannel;
    //**************************************************************************
    private final Class storageClass;
    private final StorageInitBuilder storageInit;
    private final StreamEndpoint endpoint;
    //**************************************************************************
    //<resourceId,>
    private final Map<StreamId, Component> storageComps = new HashMap<>();
    private final Map<UUID, StorageControl.CloseRequest> pendingClose = new HashMap<>();

    public StorageMngrComp(Init init) {
        logPrefix = "<" + init.endpoint.getEndpointName() + ":" + init.torrentId + ">";

        storageControlPort = provides(init.endpoint.getControlPortType());
        storagePort = provides(init.endpoint.getStoragePortType());
        storageChannel = One2NChannel.getChannel(logPrefix, storagePort, new EventOverlayIdExtractor());

        storageClass = init.storageClass;
        storageInit = init.storageInit;
        endpoint = init.endpoint;

        subscribe(handleStart, control);
        subscribe(handleKilled, control);
        subscribe(handleOpen, storageControlPort);
        subscribe(handleClose, storageControlPort);
    }

    Handler handleStart = new Handler<Start>() {
        @Override
        public void handle(Start event) {
            LOG.info("{}starting...", logPrefix);
        }
    };

    Handler handleKilled = new Handler<Killed>() {
        @Override
        public void handle(Killed event) {
            StorageControl.CloseRequest req = pendingClose.remove(event.component.id());
            if(req == null) {
                throw new RuntimeException("ups");
            }
            Component storageComp = storageComps.remove(req.stream.streamId);
            if(storageComp == null) {
                throw new RuntimeException("ups");
            }
            storageChannel.removeChannel(req.stream.streamId, storageComp.getPositive(endpoint.getStoragePortType()));
            LOG.info("{}closed storage for:{}", new Object[]{logPrefix, req.stream.streamId});
            answer(req, req.success());
        }
    };

    Handler handleOpen = new Handler<StorageControl.OpenRequest>() {
        @Override
        public void handle(StorageControl.OpenRequest req) {
            LOG.info("{}starting storage for:{}", new Object[]{logPrefix, req.stream.streamId});
            Component storageComp = create(storageClass, storageInit.buildWith(req.stream));
            storageChannel.addChannel(req.stream.streamId, storageComp.getPositive(endpoint.getStoragePortType()));
            trigger(Start.event, storageComp.control());
            
            storageComps.put(req.stream.streamId, storageComp);
            answer(req, req.success());
        }
    };

    Handler handleClose = new Handler<StorageControl.CloseRequest>() {
        @Override
        public void handle(StorageControl.CloseRequest req) {
            LOG.info("{}closing storage for:{}", new Object[]{logPrefix, req.stream.streamId});
            Component storageComp = storageComps.get(req.stream.streamId);
            if(storageComp == null) {
                throw new RuntimeException("ups");
            }
            trigger(Kill.event, storageComp.control());
            pendingClose.put(storageComp.id(), req);
        }
    };

    public static class Init extends se.sics.kompics.Init<StorageMngrComp> {

        public final Class storageClass;
        public final StorageInitBuilder storageInit;
        public final StreamEndpoint endpoint;
        public final OverlayId torrentId;

        public Init(Class storageClass, StorageInitBuilder storageInit, StreamEndpoint endpoint, OverlayId torrentId) {
            this.storageClass = storageClass;
            this.storageInit = storageInit;
            this.endpoint = endpoint;
            this.torrentId = torrentId;
        }
    }
}
