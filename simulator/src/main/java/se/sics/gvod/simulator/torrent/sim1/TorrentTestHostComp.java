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
package se.sics.gvod.simulator.torrent.sim1;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.gvod.stream.congestion.PLedbatTrafficSelector;
import se.sics.gvod.stream.congestion.PullLedbatComp;
import se.sics.gvod.stream.torrent.TorrentComp;
import se.sics.gvod.stream.connection.ConnMngrPort;
import se.sics.gvod.stream.report.ReportComp;
import se.sics.gvod.stream.torrent.TorrentStatus;
import se.sics.kompics.Channel;
import se.sics.kompics.Component;
import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.Handler;
import se.sics.kompics.Positive;
import se.sics.kompics.Start;
import se.sics.kompics.network.Network;
import se.sics.kompics.timer.Timer;
import se.sics.ktoolbox.util.network.ports.ShortCircuitChannel;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class TorrentTestHostComp extends ComponentDefinition {

    private static final Logger LOG = LoggerFactory.getLogger(TorrentTestHostComp.class);
    private String logPrefix = "";

    //****************************CONNECTIONS***********************************
    //***********************EXTERNAL_CONNECT_TO********************************
    Positive<Timer> timerPort = requires(Timer.class);
    Positive<Network> networkPort = requires(Network.class);
    //**************************INTERNAL_STATE**********************************
    private Component congestionComp;
    private Component torrentComp;
    private Component driverComp;
    private Component reportComp;

    private TorrentComp.Init torrentInit;
    private TorrentDriverComp.Init driverInit;

    public TorrentTestHostComp(Init init) {
        LOG.info("{}initiating...", logPrefix);
        subscribe(handleStart, control);

        torrentInit = init.torrentInit;
        driverInit = init.driverInit;
    }

    Handler handleStart = new Handler<Start>() {
        @Override
        public void handle(Start event) {
            LOG.info("{}starting...", logPrefix);

            driverComp = create(TorrentDriverComp.class, driverInit);
            connect(driverComp.getNegative(Network.class), networkPort, Channel.TWO_WAY);
            connect(driverComp.getNegative(Timer.class), timerPort, Channel.TWO_WAY);

            congestionComp = create(PullLedbatComp.class, new PullLedbatComp.Init());
            connect(congestionComp.getNegative(Timer.class), timerPort, Channel.TWO_WAY);

            torrentComp = create(TorrentComp.class, torrentInit);
            connect(torrentComp.getNegative(Timer.class), timerPort, Channel.TWO_WAY);
            connect(torrentComp.getNegative(ConnMngrPort.class), driverComp.getPositive(ConnMngrPort.class), Channel.TWO_WAY);
            connect(torrentComp.getPositive(TorrentStatus.class), driverComp.getNegative(TorrentStatus.class), Channel.TWO_WAY);

            //connect driver, congestion, torrent components
            ShortCircuitChannel scc = ShortCircuitChannel.getChannel(
                    driverComp.getPositive(Network.class), congestionComp.getPositive(Network.class), new PLedbatTrafficSelector(),
                    torrentComp.getNegative(Network.class), congestionComp.getNegative(Network.class), new PLedbatTrafficSelector());

            reportComp = create(ReportComp.class, new ReportComp.Init(1000));
            connect(reportComp.getNegative(Timer.class), timerPort, Channel.TWO_WAY);
            connect(reportComp.getNegative(TorrentStatus.class), torrentComp.getPositive(TorrentStatus.class), Channel.TWO_WAY);

            trigger(Start.event, driverComp.control());
            trigger(Start.event, congestionComp.control());
            trigger(Start.event, torrentComp.control());
            trigger(Start.event, reportComp.control());
        }
    };

    public static class Init extends se.sics.kompics.Init<TorrentTestHostComp> {

        public TorrentComp.Init torrentInit;
        public TorrentDriverComp.Init driverInit;

        public Init(TorrentComp.Init torrentInit, TorrentDriverComp.Init driverInit) {
            this.torrentInit = torrentInit;
            this.driverInit = driverInit;
        }
    }
}
