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
import se.sics.gvod.stream.connection.ConnectionPort;
import se.sics.gvod.stream.connection.StreamConnections;
import se.sics.gvod.stream.torrent.TorrentStatus;
import se.sics.gvod.stream.torrent.event.DownloadFinished;
import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.Handler;
import se.sics.kompics.Negative;
import se.sics.kompics.Positive;
import se.sics.kompics.Start;
import se.sics.kompics.network.Msg;
import se.sics.kompics.network.Network;
import se.sics.kompics.simulator.util.GlobalView;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class TorrentDriverComp extends ComponentDefinition {
    private static final Logger LOG = LoggerFactory.getLogger(TorrentTestHostComp.class);
    private String logPrefix = "";

    //****************************CONNECTIONS***********************************
    //***********************EXTERNAL_CONNECT_TO********************************
    Negative<Network> providedNetworkPort = provides(Network.class);
    Positive<Network> requiredNetworkPort = requires(Network.class);
    Negative<ConnectionPort> connectionPort = provides(ConnectionPort.class);
    Positive<TorrentStatus> torrentStatus = requires(TorrentStatus.class);
    
    private final TorrentDriver torrentDriver;
    private final GlobalView gv;
    
    public TorrentDriverComp(Init init) {
        LOG.info("{}initiating...", logPrefix);
        subscribe(handleStart, control);
        subscribe(handleStreamConnectionRequest, connectionPort);
        subscribe(handleOutgoing, providedNetworkPort);
        subscribe(handleIncoming, requiredNetworkPort);
        subscribe(handleDownloadFinished, torrentStatus);
        
        gv = config().getValue("simulation.globalview", GlobalView.class);
        torrentDriver = init.torrentDriver;
    }
    
    Handler handleStart = new Handler<Start>() {
        @Override
        public void handle(Start event) {
            LOG.info("{}starting...", logPrefix);
        }
    };
    
    Handler handleStreamConnectionRequest = new Handler<StreamConnections.Request>() {
        @Override
        public void handle(StreamConnections.Request event) {
            torrentDriver.next(TorrentDriverComp.this.proxy, gv, event);
        }
    };
    
    Handler handleOutgoing = new Handler<Msg>() {
        @Override
        public void handle(Msg msg) {
             torrentDriver.next(proxy, gv, msg);
        }
    };
    
     Handler handleIncoming = new Handler<Msg>() {
        @Override
        public void handle(Msg msg) {
             torrentDriver.next(proxy, gv, msg);
        }
    };
     
     Handler handleDownloadFinished = new Handler<DownloadFinished>() {
        @Override
        public void handle(DownloadFinished event) {
            torrentDriver.next(proxy, gv, event);
        }
     };
    
    public static class Init extends se.sics.kompics.Init<TorrentDriverComp> {
        public final TorrentDriver torrentDriver;
        
        public Init(TorrentDriver torrentDriver) {
            this.torrentDriver = torrentDriver;
        }
    }
}
