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
package se.sics.nstream.torrent;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.kompics.Channel;
import se.sics.kompics.Component;
import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.Handler;
import se.sics.kompics.Negative;
import se.sics.kompics.Positive;
import se.sics.kompics.Start;
import se.sics.kompics.network.Network;
import se.sics.kompics.timer.Timer;
import se.sics.ktoolbox.util.identifiable.overlay.OverlayId;
import se.sics.ktoolbox.util.idextractor.EventOverlayIdExtractor;
import se.sics.ktoolbox.util.idextractor.MsgOverlayIdExtractor;
import se.sics.ktoolbox.util.network.KAddress;
import se.sics.ktoolbox.util.network.ports.One2NChannel;
import se.sics.nstream.storage.durable.DStoragePort;
import se.sics.nstream.storage.durable.DStreamControlPort;
import se.sics.nstream.torrent.event.StartDownload;
import se.sics.nstream.torrent.event.StartUpload;
import se.sics.nstream.torrent.resourceMngr.ResourceMngrComp;
import se.sics.nstream.torrent.resourceMngr.ResourceMngrPort;
import se.sics.nstream.torrent.tracking.TorrentStatusPort;

/**
 *
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class TorrentMngrComp extends ComponentDefinition {

    private static final Logger LOG = LoggerFactory.getLogger(TorrentMngrComp.class);
    private String logPrefix;

    private final Positive<Timer> timerPort = requires(Timer.class);
    private final Positive<Network> networkPort = requires(Network.class);
    private final Positive<DStreamControlPort> streamControlPort = requires(DStreamControlPort.class);
    private final Positive<DStoragePort> storagePort = requires(DStoragePort.class);
    
    private final Negative<TorrentStatusPort> torrentStatusPort = provides(TorrentStatusPort.class);
    private final Negative<TorrentMngrPort> torrentMngrPort = provides(TorrentMngrPort.class);
    
    private final One2NChannel networkChannel; 
    private final One2NChannel reportChannel;
    //**************************************************************************
    private final KAddress selfAdr;
    //**************************************************************************
    private Component resourceMngrComp;
    private Map<OverlayId, Component> torrentComps = new HashMap<>();
    private Map<UUID, OverlayId> compIdToTorrentId = new HashMap<>();
    
    public TorrentMngrComp(Init init) {
        selfAdr = init.selfAdr;
        logPrefix = "<nid:" + selfAdr.getId() + ">";
        LOG.info("{}initiating...", logPrefix);
        
        networkChannel = One2NChannel.getChannel(logPrefix + "torrent", networkPort, new MsgOverlayIdExtractor());
        reportChannel = One2NChannel.getChannel("hopsTorrentMngrReport", torrentStatusPort, new EventOverlayIdExtractor());
        
        subscribe(handleStart, control);
        subscribe(handleUpload, torrentMngrPort);
        subscribe(handleDownload1, torrentMngrPort);
    }

    Handler handleStart = new Handler<Start>() {
        @Override
        public void handle(Start event) {
            LOG.info("{}starting", logPrefix);
            
            baseSetup();
            baseStart();
        }
    };
    
    private void baseSetup() {
        resourceMngrComp = create(ResourceMngrComp.class, new ResourceMngrComp.Init(selfAdr.getId()));
        connect(resourceMngrComp.getNegative(DStreamControlPort.class), streamControlPort, Channel.TWO_WAY);
    }
    
    private void baseStart() {
        trigger(Start.event, resourceMngrComp.control());
    }
    
    Handler handleUpload = new Handler<StartUpload.Request>() {
        @Override
        public void handle(StartUpload.Request event) {
            LOG.info("{}upload:{}", logPrefix, event.torrentId);
            
            Component torrentComp = create(TorrentComp.class, TorrentComp.Init.upload(selfAdr, event.torrentId, event.torrent));
            connect(torrentComp.getNegative(Timer.class), timerPort, Channel.TWO_WAY);
            networkChannel.addChannel(event.torrentId, torrentComp.getNegative(Network.class));
            //1toN connection, but we do not need a One2NChannel since we use Direct.Request/Response and the multiplexing is on Response
            connect(torrentComp.getNegative(ResourceMngrPort.class), resourceMngrComp.getPositive(ResourceMngrPort.class), Channel.TWO_WAY); 
            connect(torrentComp.getNegative(DStoragePort.class), storagePort, Channel.TWO_WAY);
            reportChannel.addChannel(event.torrentId, torrentComp.getPositive(TorrentStatusPort.class));
            
            torrentComps.put(event.torrentId, torrentComp);
            compIdToTorrentId.put(torrentComp.id(), event.torrentId);
            
            trigger(Start.event, torrentComp.control());
            answer(event, event.success());
        }
    };
            
    Handler handleDownload1 = new Handler<StartDownload.Request>() {
        @Override
        public void handle(StartDownload.Request event) {
            LOG.info("{}download1:{}", logPrefix, event.torrentId);
            
            Component torrentComp = create(TorrentComp.class, TorrentComp.Init.download(selfAdr, event.torrentId, event.partners));
            connect(torrentComp.getNegative(Timer.class), timerPort, Channel.TWO_WAY);
            networkChannel.addChannel(event.torrentId, torrentComp.getNegative(Network.class));
            //1toN connection, but we do not need a One2NChannel since we use Direct.Request/Response and the multiplexing is on Response
            connect(torrentComp.getNegative(ResourceMngrPort.class), resourceMngrComp.getPositive(ResourceMngrPort.class), Channel.TWO_WAY); 
            connect(torrentComp.getNegative(DStoragePort.class), storagePort, Channel.TWO_WAY);
            reportChannel.addChannel(event.torrentId, torrentComp.getPositive(TorrentStatusPort.class));
            
            torrentComps.put(event.torrentId, torrentComp);
            compIdToTorrentId.put(torrentComp.id(), event.torrentId);
            
            trigger(Start.event, torrentComp.control());
            answer(event, event.success());
        }
    };
    
    public static class Init extends se.sics.kompics.Init<TorrentMngrComp> {

        public final KAddress selfAdr;

        public Init(KAddress selfAdr) {
            this.selfAdr = selfAdr;
        }
    }
}
