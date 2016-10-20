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
import se.sics.nstream.torrent.event.StartTorrent;
import se.sics.nstream.torrent.resourceMngr.ResourceMngrComp;
import se.sics.nstream.torrent.resourceMngr.ResourceMngrPort;
import se.sics.nstream.torrent.status.event.TorrentReady;
import se.sics.nstream.torrent.tracking.TorrentStatusPort;
import se.sics.nstream.torrent.transfer.TransferCtrlPort;

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

    private final Negative<TorrentMngrPort> torrentMngrPort = provides(TorrentMngrPort.class);
    private final Negative<TransferCtrlPort> transferCtrlPort = provides(TransferCtrlPort.class);
    private final Negative<TorrentStatusPort> torrentStatusPort = provides(TorrentStatusPort.class);

    private final One2NChannel networkChannel;
    private final One2NChannel transferCtrlChannel;
    private final One2NChannel reportChannel;
    //**************************************************************************
    private final KAddress selfAdr;
    //**************************************************************************
    private Component resourceMngrComp;
    private Map<OverlayId, Component> torrentComps = new HashMap<>();
    private Map<UUID, OverlayId> compIdToTorrentId = new HashMap<>();
    //**************************************************************************
    private Map<OverlayId, StartTorrent.Request> pendingStarts = new HashMap<>();
    
    public TorrentMngrComp(Init init) {
        selfAdr = init.selfAdr;
        logPrefix = "<nid:" + selfAdr.getId() + ">";
        LOG.info("{}initiating...", logPrefix);

        networkChannel = One2NChannel.getChannel(logPrefix + "torrent", networkPort, new MsgOverlayIdExtractor());
        transferCtrlChannel = One2NChannel.getChannel(logPrefix + "transferCtrl", transferCtrlPort, new EventOverlayIdExtractor());
        reportChannel = One2NChannel.getChannel("hopsTorrentMngrReport", torrentStatusPort, new EventOverlayIdExtractor());

        subscribe(handleStart, control);
        subscribe(handleTorrentStart, torrentMngrPort);
        subscribe(handleTorrentReady, torrentStatusPort.getPair());
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

    Handler handleTorrentStart = new Handler<StartTorrent.Request>() {
        @Override
        public void handle(StartTorrent.Request req) {
            LOG.info("{}starting torrent:{}", logPrefix, req.torrentId);

            Component torrentComp = create(TorrentComp.class, new TorrentComp.Init(selfAdr, req.torrentId, req.partners));
            connect(torrentComp.getNegative(Timer.class), timerPort, Channel.TWO_WAY);
            networkChannel.addChannel(req.torrentId, torrentComp.getNegative(Network.class));
            connect(torrentComp.getNegative(ResourceMngrPort.class), resourceMngrComp.getPositive(ResourceMngrPort.class), Channel.TWO_WAY);
            connect(torrentComp.getNegative(DStoragePort.class), storagePort, Channel.TWO_WAY);
            transferCtrlChannel.addChannel(req.torrentId, torrentComp.getPositive(TransferCtrlPort.class));
            reportChannel.addChannel(req.torrentId, torrentComp.getPositive(TorrentStatusPort.class));

            torrentComps.put(req.torrentId, torrentComp);
            compIdToTorrentId.put(torrentComp.id(), req.torrentId);

            trigger(Start.event, torrentComp.control());
            pendingStarts.put(req.torrentId, req);
        }
    };

    Handler handleTorrentReady = new Handler<TorrentReady>() {
        @Override
        public void handle(TorrentReady event) {
            LOG.info("{}started torrent:{}", logPrefix, event.torrentId);
            StartTorrent.Request req = pendingStarts.remove(event.torrentId);
            if(req == null) {
                //TODO - what?
                return;
            }
            answer(req, req.success());
        }
    };

    public static class Init extends se.sics.kompics.Init<TorrentMngrComp> {

        public final KAddress selfAdr;

        public Init(KAddress selfAdr) {
            this.selfAdr = selfAdr;
        }
    }
}
