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
package se.sics.gvod.stream;

import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.gvod.core.util.TorrentDetails;
import se.sics.gvod.stream.congestion.PLedbatPort;
import se.sics.gvod.stream.congestion.PullLedbatComp;
import se.sics.gvod.stream.connection.ConnMngrComp;
import se.sics.gvod.stream.connection.ConnMngrPort;
import se.sics.gvod.stream.report.ReportComp;
import se.sics.gvod.stream.report.ReportPort;
import se.sics.gvod.stream.torrent.TorrentComp;
import se.sics.gvod.stream.torrent.TorrentStatus;
import se.sics.kompics.Channel;
import se.sics.kompics.Component;
import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.Handler;
import se.sics.kompics.Negative;
import se.sics.kompics.Positive;
import se.sics.kompics.Start;
import se.sics.kompics.network.Network;
import se.sics.kompics.timer.Timer;
import se.sics.ktoolbox.util.config.impl.SystemKCWrapper;
import se.sics.ktoolbox.util.network.KAddress;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class StreamHostComp extends ComponentDefinition {
    private static final Logger LOG = LoggerFactory.getLogger(StreamHostComp.class);
    private String logPrefix = " ";    
    
    private Negative<ReportPort> reportPort = provides(ReportPort.class);
    private Negative<TorrentStatus> streamStatusPort = provides(TorrentStatus.class);
    private final ExtPort extPorts;
    
    private Component connComp;
    private Component congestionComp;
    private Component torrentComp;
    private Component reportComp;
    
    private SystemKCWrapper systemConfig;
    private final long reportingPeriod = 100;
    
    private final KAddress selfAdr;
    private final TorrentDetails torrentDetails;
    private final List<KAddress> partners;
    
    public StreamHostComp(Init init) {
        systemConfig = new SystemKCWrapper(config());
        selfAdr = init.selfAdr;
        logPrefix = "<nid:" + systemConfig.id + "> ";
        LOG.info("{}starting...", logPrefix);
        
        extPorts = init.extPorts;
        torrentDetails = init.torrentDetails;
        partners = init.partners;
        
        subscribe(handleStart, control);
    }
    
    Handler handleStart = new Handler<Start>() {
        @Override
        public void handle(Start event) {
            LOG.info("{}starting...", logPrefix);
            connect();
            trigger(Start.event, connComp.control());
            trigger(Start.event, congestionComp.control());
            trigger(Start.event, torrentComp.control());
            trigger(Start.event, reportComp.control());
        }
    };
    
    private void connect() {
        connComp = create(ConnMngrComp.class, new ConnMngrComp.Init(partners));
        
        congestionComp = create(PullLedbatComp.class, new PullLedbatComp.Init(selfAdr, systemConfig.seed));
        connect(congestionComp.getNegative(Timer.class), extPorts.timerPort, Channel.TWO_WAY);

        torrentComp = create(TorrentComp.class, new TorrentComp.Init(selfAdr, torrentDetails));
        connect(torrentComp.getNegative(Timer.class), extPorts.timerPort, Channel.TWO_WAY);
        connect(torrentComp.getNegative(ConnMngrPort.class), connComp.getPositive(ConnMngrPort.class), Channel.TWO_WAY);
        connect(torrentComp.getNegative(PLedbatPort.class), congestionComp.getPositive(PLedbatPort.class), Channel.TWO_WAY);

        //connect driver, congestion, torrent components
//            ShortCircuitChannel scc = ShortCircuitChannel.getChannel(
//                    driverComp.getPositive(Network.class), congestionComp.getPositive(Network.class), new PLedbatTrafficSelector(),
//                    torrentComp.getNegative(Network.class), congestionComp.getNegative(Network.class), new PLedbatTrafficSelector());
        connect(congestionComp.getNegative(Network.class), extPorts.networkPort, Channel.TWO_WAY);
        connect(torrentComp.getNegative(Network.class), congestionComp.getPositive(Network.class), Channel.TWO_WAY);

        reportComp = create(ReportComp.class, new ReportComp.Init(reportingPeriod));
        connect(reportComp.getNegative(Timer.class), extPorts.timerPort, Channel.TWO_WAY);
        connect(reportComp.getNegative(TorrentStatus.class), torrentComp.getPositive(TorrentStatus.class), Channel.TWO_WAY);

        connect(streamStatusPort, torrentComp.getPositive(TorrentStatus.class), Channel.TWO_WAY);
        connect(reportPort, reportComp.getPositive(ReportPort.class), Channel.TWO_WAY);
    }
    
    public static class Init extends se.sics.kompics.Init<StreamHostComp> {
        public final ExtPort extPorts;
        public final KAddress selfAdr;
        public final TorrentDetails torrentDetails;
        public final List<KAddress> partners;
        
        public Init(ExtPort extPorts, KAddress selfAdr, TorrentDetails torrentDetails, List<KAddress> partners) {
            this.extPorts = extPorts;
            this.selfAdr = selfAdr;
            this.torrentDetails = torrentDetails;
            this.partners = partners;
        }
    }
    
    public static class ExtPort {
        public final Positive<Timer> timerPort;
        public final Positive<Network> networkPort;
        
        public ExtPort(Positive<Timer> timerPort, Positive<Network> networkPort) {
            this.timerPort = timerPort;
            this.networkPort = networkPort;
        }
    }
}
