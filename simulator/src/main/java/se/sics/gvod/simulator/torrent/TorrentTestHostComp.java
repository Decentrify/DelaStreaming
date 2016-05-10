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
package se.sics.gvod.simulator.torrent;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.gvod.core.TorrentComp;
import se.sics.gvod.stream.connection.ConnectionPort;
import se.sics.kompics.Channel;
import se.sics.kompics.Component;
import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.Handler;
import se.sics.kompics.Positive;
import se.sics.kompics.Start;
import se.sics.kompics.network.Network;
import se.sics.kompics.timer.Timer;

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
    private Component torrentComp;
    private Component torrentDriverComp;

    private TorrentComp.Init torrentInit;
    private TorrentDriverComp.Init torrentDriverInit;

    public TorrentTestHostComp(Init init) {
        LOG.info("{}initiating...", logPrefix);
        subscribe(handleStart, control);

        torrentInit = init.torrentInit;
        torrentDriverInit = init.torrentDriverInit;
    }

    Handler handleStart = new Handler<Start>() {
        @Override
        public void handle(Start event) {
            LOG.info("{}starting...", logPrefix);

            torrentDriverComp = create(TorrentDriverComp.class, torrentDriverInit);
            torrentComp = create(TorrentComp.class, torrentInit);
            connect(torrentComp.getNegative(Timer.class), timerPort, Channel.TWO_WAY);
            connect(torrentComp.getNegative(Network.class), torrentDriverComp.getPositive(Network.class), Channel.TWO_WAY);
            connect(torrentComp.getNegative(ConnectionPort.class), torrentDriverComp.getPositive(ConnectionPort.class), Channel.TWO_WAY);

            trigger(Start.event, torrentDriverComp.control());
            trigger(Start.event, torrentComp.control());
        }
    };

    public static class Init extends se.sics.kompics.Init<TorrentTestHostComp> {

        public TorrentComp.Init torrentInit;
        public TorrentDriverComp.Init torrentDriverInit;

        public Init(TorrentComp.Init torrentInit, TorrentDriverComp.Init torrentDriverInit) {
            this.torrentInit = torrentInit;
            this.torrentDriverInit = torrentDriverInit;
        }
    }
}
