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
package se.sics.nstream.storage.durable;

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
import se.sics.ktoolbox.util.identifiable.Identifier;
import se.sics.ktoolbox.util.network.ports.One2NChannel;
import se.sics.nstream.storage.durable.events.DEndpointConnect;
import se.sics.nstream.storage.durable.events.DEndpointDisconnect;

/**
 *
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class DStorageMngrComp extends ComponentDefinition {

    private static final Logger LOG = LoggerFactory.getLogger(DStorageMngrComp.class);
    private final String logPrefix;

    private final Negative<DStoragePort> storagePort = provides(DStoragePort.class);
    private final Negative<DStreamControlPort> streamControlPort = provides(DStreamControlPort.class);
    private final Negative<DEndpointCtrlPort> endpointControlPort = provides(DEndpointCtrlPort.class);
    private final One2NChannel streamControlChannel;
    private final One2NChannel storageChannel;
    //**************************************************************************
    private final Identifier self;
    //**************************************************************************
    private final Map<UUID, Identifier> compIdToEndpointId = new HashMap<>();
    private final Map<Identifier, Component> storageEndpoints = new HashMap<>();
    private final Map<Identifier, DEndpointDisconnect.Request> pendingDisconnects = new HashMap<>();

    public DStorageMngrComp(Init init) {
        self = init.self;
        logPrefix = "<nid:" + self + ">";
        LOG.info("{}initiating...", logPrefix);

        streamControlChannel = One2NChannel.getChannel(logPrefix + ":dstream_control", streamControlPort, new DEndpointIdExtractor());
        storageChannel = One2NChannel.getChannel(logPrefix + ":dstorage", storagePort, new DEndpointIdExtractor());

        subscribe(handleStart, control);
        subscribe(handleKilled, control);
        subscribe(handleConnect, endpointControlPort);
        subscribe(handleDisconnect, endpointControlPort);
    }

    Handler handleStart = new Handler<Start>() {
        @Override
        public void handle(Start event) {
            LOG.info("{}starting", logPrefix);
        }
    };

    Handler handleKilled = new Handler<Killed>() {
        @Override
        public void handle(Killed event) {
            Identifier endpointId = compIdToEndpointId.remove(event.component.id());
            if (endpointId != null) {
                LOG.info("{}disconnected endpoint:{}", logPrefix, endpointId);
                Component endpoint = storageEndpoints.remove(endpointId);

                streamControlChannel.removeChannel(endpointId, endpoint.getPositive(DStreamControlPort.class));
                storageChannel.removeChannel(endpointId, endpoint.getPositive(DStoragePort.class));
                
                DEndpointDisconnect.Request req = pendingDisconnects.remove(endpointId);
                answer(req, req.success());
            } else {
                LOG.warn("{}double killed? no endpoint - component:{}", logPrefix, event.component.id());
            }
        }
    };

    Handler handleConnect = new Handler<DEndpointConnect.Request>() {
        @Override
        public void handle(DEndpointConnect.Request req) {
            LOG.info("{}connecting endpoint:{}", logPrefix, req.endpointId);
            Component endpointComp = create(DStreamMngrComp.class, new DStreamMngrComp.Init(self, req.endpointProvider));
            streamControlChannel.addChannel(req.endpointId, endpointComp.getPositive(DStreamControlPort.class));
            storageChannel.addChannel(req.endpointId, endpointComp.getPositive(DStoragePort.class));

            compIdToEndpointId.put(endpointComp.id(), req.endpointId);
            storageEndpoints.put(req.endpointId, endpointComp);

            trigger(Start.event, endpointComp.control());
            answer(req, req.success());
        }
    };

    Handler handleDisconnect = new Handler<DEndpointDisconnect.Request>() {
        @Override
        public void handle(DEndpointDisconnect.Request req) {
            LOG.info("{}disconnecting endpoint:{}", logPrefix, req.endpointId);
            Component endpointComp = storageEndpoints.get(req.endpointId);
            if (endpointComp == null) {
                LOG.warn("{}endpoint:{} already disconnected", logPrefix, req.endpointId);
                answer(req, req.success());
                return;
            }
            pendingDisconnects.put(req.endpointId, req);
            trigger(Kill.event, endpointComp.control());
        }
    };

    public static class Init extends se.sics.kompics.Init<DStorageMngrComp> {

        public final Identifier self;

        public Init(Identifier self) {
            this.self = self;
        }
    }
}
