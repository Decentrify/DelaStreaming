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

import java.util.List;
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
import se.sics.ktoolbox.util.network.KAddress;
import se.sics.nstream.storage.durable.DStoragePort;
import se.sics.nstream.torrent.resourceMngr.ResourceMngrPort;
import se.sics.nstream.torrent.status.event.TorrentReady;
import se.sics.nstream.torrent.tracking.TorrentStatusPort;
import se.sics.nstream.torrent.tracking.TorrentTrackingComp;
import se.sics.nstream.torrent.tracking.TorrentTrackingPort;
import se.sics.nstream.torrent.transfer.TransferComp;
import se.sics.nstream.torrent.transfer.TransferCtrlPort;
import se.sics.nutil.network.bestEffort.BestEffortNetworkComp;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class TorrentComp extends ComponentDefinition {

    public static final long REPORT_DELAY = 1000;

    private static final Logger LOG = LoggerFactory.getLogger(TorrentComp.class);
    private final String logPrefix;

    //**************************EXTERNAL_CONNECT_TO*****************************
    private final Positive<Timer> timerPort = requires(Timer.class);
    private final Positive<Network> networkPort = requires(Network.class);
    private final Positive<ResourceMngrPort> resourceMngrPort = requires(ResourceMngrPort.class);
    private final Positive<DStoragePort> storagePort = requires(DStoragePort.class);
    private final Negative<TransferCtrlPort> transferCtrlPort = provides(TransferCtrlPort.class);
    private final Negative<TorrentStatusPort> statusPort = provides(TorrentStatusPort.class);
    //**************************************************************************
    private final KAddress selfAdr;
    private final OverlayId torrentId;
    private final List<KAddress> partners;
    //**************************************************************************
    private Component networkRetryComp;
    private Component transferComp;
    private Component reportComp;

    public TorrentComp(Init init) {
        selfAdr = init.selfAdr;
        torrentId = init.torrentId;
        logPrefix = "<nid:" + selfAdr.getId() + ",tid:" + torrentId + ">";
        LOG.info("{}initiating...", logPrefix);

        partners = init.partners;
        subscribe(handleStart, control);
    }

    Handler handleStart = new Handler<Start>() {
        @Override
        public void handle(Start event) {
            LOG.info("{}starting...", logPrefix);
            connectComp();
            startComp();
            trigger(new TorrentReady(torrentId), statusPort);
        }
    };
    
    private void connectComp() {
        networkRetryComp = create(BestEffortNetworkComp.class, new BestEffortNetworkComp.Init(selfAdr));
        connect(networkRetryComp.getNegative(Timer.class), timerPort, Channel.TWO_WAY);
        connect(networkRetryComp.getNegative(Network.class), networkPort, Channel.TWO_WAY);

        transferComp = create(TransferComp.class, new TransferComp.Init(selfAdr, torrentId, partners));
        connect(transferComp.getNegative(Timer.class), timerPort, Channel.TWO_WAY);
        connect(transferComp.getNegative(Network.class), networkRetryComp.getPositive(Network.class), Channel.TWO_WAY);
        connect(transferComp.getNegative(ResourceMngrPort.class), resourceMngrPort, Channel.TWO_WAY);
        connect(transferComp.getNegative(DStoragePort.class), storagePort, Channel.TWO_WAY);
        connect(transferComp.getPositive(TransferCtrlPort.class), transferCtrlPort, Channel.TWO_WAY);

        reportComp = create(TorrentTrackingComp.class, new TorrentTrackingComp.Init(torrentId, REPORT_DELAY));
        connect(reportComp.getNegative(Timer.class), timerPort, Channel.TWO_WAY);
        connect(reportComp.getNegative(TorrentTrackingPort.class), transferComp.getPositive(TorrentTrackingPort.class), Channel.TWO_WAY);

        connect(statusPort, reportComp.getPositive(TorrentStatusPort.class), Channel.TWO_WAY);
    }

    private void startComp() {
        trigger(Start.event, networkRetryComp.control());
        trigger(Start.event, transferComp.control());
        trigger(Start.event, reportComp.control());
    }

    public static class Init extends se.sics.kompics.Init<TorrentComp> {

        public final KAddress selfAdr;
        public final OverlayId torrentId;
        public final List<KAddress> partners;

        public Init(KAddress selfAdr, OverlayId torrentId, List<KAddress> partners) {
            this.selfAdr = selfAdr;
            this.torrentId = torrentId;
            this.partners = partners;
        }
    }
}
