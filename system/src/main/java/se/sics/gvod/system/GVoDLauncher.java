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
package se.sics.gvod.system;

import com.google.common.util.concurrent.SettableFuture;
import com.typesafe.config.ConfigFactory;
import java.net.InetAddress;
import java.util.EnumSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.gvod.bootstrap.cclient.CaracalPSManagerComp;
import se.sics.gvod.bootstrap.server.peermanager.PeerManagerPort;
import se.sics.gvod.bootstrap.server.peermanager.msg.CaracalReady;
import se.sics.kompics.Component;
import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.Handler;
import se.sics.kompics.Init;
import se.sics.kompics.Positive;
import se.sics.kompics.Start;
import se.sics.kompics.Stop;
import se.sics.kompics.network.Network;
import se.sics.kompics.network.netty.NettyInit;
import se.sics.kompics.network.netty.NettyNetwork;
import se.sics.kompics.timer.Timer;
import se.sics.kompics.timer.java.JavaTimer;
import se.sics.ktoolbox.ipsolver.IpSolverComp;
import se.sics.ktoolbox.ipsolver.IpSolverPort;
import se.sics.ktoolbox.ipsolver.msg.GetIp;

/**
 * @author Alex Ormenisan <aaor@sics.se>
 */
public class GVoDLauncher extends ComponentDefinition {

    private static final Logger LOG = LoggerFactory.getLogger("GVoDHost");

    private static SettableFuture gvodSyncIFuture;
    private static GetIp.NetworkInterfacesMask ipType;

    private Component ipSolver;
    private Component timer;
    private Component network;
    private Component manager;
    private Component caracalPSManager;

    private HostManagerConfig config;

    public static void setSyncIFuture(SettableFuture future) {
        gvodSyncIFuture = future;
    }
    
    public static void setIpType(GetIp.NetworkInterfacesMask setIpType) {
        ipType = setIpType;
    }

    public GVoDLauncher() {
        LOG.info("initiating...");

        if (gvodSyncIFuture == null) {
            LOG.error("launcher logic error - gvodSyncI not set");
            throw new RuntimeException("launcher logic error - gvodSyncI not set");
        }
        if(ipType == null) {
            LOG.error("launcher logic error - ipType not set");
            throw new RuntimeException("launcher logic error - ipType not set");
        }

        subscribe(handleStart, control);
        subscribe(handleStop, control);

        ipSolver = create(IpSolverComp.class, new IpSolverComp.IpSolverInit());
    }

    public Handler handleStart = new Handler<Start>() {
        @Override
        public void handle(Start event) {
            LOG.info("starting: solving ip...");
            Positive<IpSolverPort> ipSolverPort = ipSolver.getPositive(IpSolverPort.class);
            subscribe(handleGetIp, ipSolverPort);
            trigger(new GetIp.Req(EnumSet.of(ipType)), ipSolverPort);
        }
    };

    public Handler handleStop = new Handler<Stop>() {
        @Override
        public void handle(Stop event) {
            LOG.info("stopping...");
        }
    };

    public Handler handleGetIp = new Handler<GetIp.Resp>() {
        @Override
        public void handle(GetIp.Resp resp) {
            LOG.info("starting: setting up caracal connection");

            InetAddress ip = null;
            if(!resp.addrs.isEmpty()) {
                ip = resp.addrs.get(0).getAddr();
                if(resp.addrs.size() > 1) {
                    LOG.warn("multiple ips detected, proceeding with:{}", ip);
                }
            }
            config = new HostManagerConfig(ConfigFactory.load(), ip);
            GVoDSystemSerializerSetup.oneTimeSetup();

            timer = create(JavaTimer.class, Init.NONE);
            trigger(Start.event, timer.control());
            network = create(NettyNetwork.class, new NettyInit(config.getSelf()));
            trigger(Start.event, network.control());
            caracalConnectPhase();
        }
    };

    private void caracalConnectPhase() {
        //TODO Alex should create and start only on open nodes
        caracalPSManager = create(CaracalPSManagerComp.class, new CaracalPSManagerComp.CaracalPSManagerInit(config.getCaracalPSManagerConfig()));
        connect(caracalPSManager.getNegative(Timer.class), timer.getPositive(Timer.class));
        trigger(Start.event, caracalPSManager.control());
        subscribe(handleCaracalReady, caracalPSManager.getPositive(PeerManagerPort.class));
    }

    private Handler handleCaracalReady = new Handler<CaracalReady>() {

        @Override
        public void handle(CaracalReady event) {
            LOG.info("starting system...");
            startGVoDPhase();
        }
    };

    private void startGVoDPhase() {
        manager = create(HostManagerComp.class, new HostManagerComp.HostManagerInit(config, caracalPSManager, gvodSyncIFuture));
        connect(manager.getNegative(Network.class), network.getPositive(Network.class));
        connect(manager.getNegative(Timer.class), timer.getPositive(Timer.class));
        trigger(Start.event, manager.control());
    }
}
