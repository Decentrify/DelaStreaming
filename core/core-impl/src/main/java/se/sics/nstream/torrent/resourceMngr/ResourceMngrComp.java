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
import org.javatuples.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.Handler;
import se.sics.kompics.Negative;
import se.sics.kompics.Positive;
import se.sics.kompics.Start;
import se.sics.kompics.id.Identifier;
import se.sics.ktoolbox.util.identifiable.overlay.OverlayId;
import se.sics.nstream.FileId;
import se.sics.nstream.StreamId;
import se.sics.nstream.storage.durable.DStreamControlPort;
import se.sics.nstream.storage.durable.events.DStreamConnect;
import se.sics.nstream.storage.durable.util.FileExtendedDetails;
import se.sics.nstream.storage.durable.util.MyStream;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class ResourceMngrComp extends ComponentDefinition {

    private static final Logger LOG = LoggerFactory.getLogger(ResourceMngrComp.class);
    private final String logPrefix;

    private Negative<ResourceMngrPort> resourceMngrPort = provides(ResourceMngrPort.class);
    private Positive<DStreamControlPort> streamControlPort = requires(DStreamControlPort.class);
    //**************************************************************************
    private final Identifier self;
    //**************************************************************************
    private final Map<OverlayId, Op> preparingResources = new HashMap<>();
    private final Map<Identifier, Op> eventsToOp = new HashMap<>();
    
    public ResourceMngrComp(Init init) {
        self = init.self;
        logPrefix = "<nid:" + self + ">";
        LOG.info("{}initiating...", logPrefix);

        subscribe(handleStart, control);
        subscribe(handlePrepare, resourceMngrPort);
        subscribe(handlePrepareSuccess, streamControlPort);
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
            LOG.info("{}preparing resources for torrent:{}", logPrefix, req.torrentId);
            Op op = new Op(req);
            preparingResources.put(op.getId(), op);
            
            for(Map.Entry<FileId, FileExtendedDetails> e : req.torrent.extended.entrySet()) {
                Pair<StreamId, MyStream> mainStream = e.getValue().getMainStream();
                prepareResource(op, e.getKey(), mainStream);
                for(Pair<StreamId, MyStream> secondaryStreams : e.getValue().getSecondaryStreams()){
                    prepareResource(op, e.getKey(), secondaryStreams);
                }
            }
        }
    };
    
    private void prepareResource(Op op, FileId fileId, Pair<StreamId, MyStream> stream) {
        LOG.info("{}preparing file:{} resource:{} endpoint:{}", new Object[]{logPrefix, fileId, stream.getValue1().resource.getSinkName(), stream.getValue1().endpoint.getEndpointName()});
        
        DStreamConnect.Request req = new DStreamConnect.Request(stream);
        op.preparing(req.getId());
        eventsToOp.put(req.getId(), op);
        trigger(req, streamControlPort);
    } 

    Handler handlePrepareSuccess = new Handler<DStreamConnect.Success>() {
        @Override
        public void handle(DStreamConnect.Success resp) {
            LOG.info("{}prepared:{}", logPrefix, resp.req.stream.getValue0());
            Op op = eventsToOp.remove(resp.getId());
            if(op == null) {
                LOG.warn("{}weird - investigate", logPrefix);
                return;
            }
            op.prepared(resp.getId(), resp.req.stream.getValue0(), resp.streamPos);
            if(op.ready()) {
                LOG.info("{}prepared all resources for:{}", logPrefix, op.getId());
                preparingResources.remove(op.getId());
                answer(op.req, op.req.success(op.streamsInfo));
            }
        }
    };
    
    public static class Init extends se.sics.kompics.Init<ResourceMngrComp> {
        public Identifier self;
        
        public Init(Identifier self) {
            this.self = self;
        }
    }
    
    public static class Op {
        public final PrepareResources.Request req;
        public final Set<Identifier> preparingResources = new HashSet<>();
        public final Map<StreamId, Long> streamsInfo = new HashMap<>();
        
        public Op(PrepareResources.Request req) {
            this.req = req;
        }
        
        public OverlayId getId() {
            return req.torrentId;
        }

        public void preparing(Identifier eventId) {
            preparingResources.add(eventId);
        }
        
        public void prepared(Identifier eventId, StreamId stream, long streamPos) {
            preparingResources.remove(eventId);
            streamsInfo.put(stream, streamPos);
        }
        
        public boolean ready() {
            return preparingResources.isEmpty();
        }
    }
}
