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
import se.sics.kompics.Component;
import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.Fault;
import se.sics.kompics.Handler;
import se.sics.kompics.Init;
import se.sics.kompics.Start;
import se.sics.kompics.Stop;
import se.sics.kompics.network.Network;
import se.sics.kompics.network.netty.NettyInit;
import se.sics.kompics.network.netty.NettyNetwork;
import se.sics.kompics.timer.Timer;
import se.sics.kompics.timer.java.JavaTimer;
import se.sics.ktoolbox.cc.bootstrap.CCBootstrapComp;
import se.sics.ktoolbox.cc.bootstrap.CCBootstrapComp.CCBootstrapInit;
import se.sics.ktoolbox.cc.bootstrap.CCBootstrapPort;
import se.sics.ktoolbox.cc.bootstrap.msg.CCReady;
import se.sics.ktoolbox.cc.common.op.CCSimpleReady;
import se.sics.ktoolbox.cc.heartbeat.CCHeartbeatComp;
import se.sics.ktoolbox.cc.heartbeat.CCHeartbeatPort;
import se.sics.ktoolbox.ipsolver.IpSolverComp;
import se.sics.ktoolbox.ipsolver.IpSolverPort;
import se.sics.ktoolbox.ipsolver.msg.GetIp;

/**
 * @author Alex Ormenisan <aaor@sics.se>
 */
public class GVoDLauncher extends ComponentDefinition {

    private static final Logger LOG = LoggerFactory.getLogger(GVoDLauncher.class);

    private static SettableFuture gvodSyncIFuture;

    public static void setSyncIFuture(SettableFuture future) {
        gvodSyncIFuture = future;
    }
    private static GetIp.NetworkInterfacesMask ipType;

    public static void setIpType(GetIp.NetworkInterfacesMask setIpType) {
        ipType = setIpType;
    }

    private Component timerComp;
    private Component ipSolverComp;
    private Component networkComp;
    private Component caracalClientComp;
    private Component heartbeatComp;
    private Component vodHostComp;

    private HostManagerConfig config;
    private byte[] vodSchemaId;

    public GVoDLauncher() {
        LOG.info("initiating...");

        if (gvodSyncIFuture == null) {
            LOG.error("exception: gvodSyncIFuture not set shutting down");
            System.exit(1);
        }
        if (ipType == null) {
            LOG.error("exception: ipType not set shutting down");
            System.exit(1);
        }
        GVoDSystemSerializerSetup.oneTimeSetup();
        timerComp = create(JavaTimer.class, Init.NONE);

        subscribe(handleStart, control);
        subscribe(handleStop, control);
    }

    @Override
    public Fault.ResolveAction handleFault(Fault fault) {
        LOG.error("exception:{} shutting down", fault.getCause());
        System.exit(1);
        return Fault.ResolveAction.RESOLVED;
    }

    private Handler handleStart = new Handler<Start>() {
        @Override
        public void handle(Start event) {
            LOG.info("starting: solving ip...");
            phase1();
        }
    };

    private Handler handleStop = new Handler<Stop>() {
        @Override
        public void handle(Stop event) {
            LOG.info("stopping...");
        }
    };

    private void phase1() {
        connectIpSolver();
        subscribe(handleGetIp, ipSolverComp.getPositive(IpSolverPort.class));
        trigger(new GetIp.Req(EnumSet.of(ipType)), ipSolverComp.getPositive(IpSolverPort.class));
    }

    private void connectIpSolver() {
        ipSolverComp = create(IpSolverComp.class, new IpSolverComp.IpSolverInit());
        trigger(Start.event, ipSolverComp.control());
    }

    private Handler handleGetIp = new Handler<GetIp.Resp>() {
        @Override
        public void handle(GetIp.Resp resp) {
            LOG.info("starting: setting up caracal connection");

            InetAddress ip = null;
            if (!resp.addrs.isEmpty()) {
                ip = resp.addrs.get(0).getAddr();
                if (resp.addrs.size() > 1) {
                    LOG.warn("multiple ips detected, proceeding with:{}", ip);
                }
            }
            config = new HostManagerConfig(ConfigFactory.load(), ip);
            phase2();
        }
    };

    private void phase2() {
        connectNetwork();
        connectCaracalClient();
        connectHeartbeat();
        subscribe(handleCaracalReady, caracalClientComp.getPositive(CCBootstrapPort.class));
        subscribe(handleHeartbeatReady, heartbeatComp.getPositive(CCHeartbeatPort.class));
    }

    private void connectNetwork() {
        networkComp = create(NettyNetwork.class, new NettyInit(config.getSelf()));
        trigger(Start.event, networkComp.control());
    }

    private void connectCaracalClient() {
        caracalClientComp = create(CCBootstrapComp.class, new CCBootstrapInit(config.getSystemConfig(), config.getCaracalClientConfig(), config.getCaracalNodes()));
        connect(caracalClientComp.getNegative(Timer.class), timerComp.getPositive(Timer.class));
        connect(caracalClientComp.getNegative(Network.class), networkComp.getPositive(Network.class));
        trigger(Start.event, caracalClientComp.control());
    }

    private void connectHeartbeat() {
        heartbeatComp = create(CCHeartbeatComp.class, new CCHeartbeatComp.CCHeartbeatInit(config.getSystemConfig(), config.getCaracalClientConfig()));
        connect(heartbeatComp.getNegative(Timer.class), timerComp.getPositive(Timer.class));
        connect(heartbeatComp.getNegative(CCBootstrapPort.class), caracalClientComp.getPositive(CCBootstrapPort.class));
        trigger(Start.event, heartbeatComp.control());
    }

    private Handler handleCaracalReady = new Handler<CCReady>() {
        @Override
        public void handle(CCReady event) {
            LOG.info("starting: received schemas");
            vodSchemaId = event.caracalSchemaData.getId("gvod.metadata");
            if (vodSchemaId == null) {
                LOG.error("exception:vod schema undefined shutting down");
                System.exit(1);
            }
        }
    };

    private Handler handleHeartbeatReady = new Handler<CCSimpleReady>() {
        @Override
        public void handle(CCSimpleReady e) {
            LOG.info("starting: system");
            connectVoDHost();
        }
    };

    private void connectVoDHost() {
        vodHostComp = create(HostManagerComp.class, new HostManagerComp.HostManagerInit(config, gvodSyncIFuture, vodSchemaId));
        connect(vodHostComp.getNegative(Network.class), networkComp.getPositive(Network.class));
        connect(vodHostComp.getNegative(Timer.class), timerComp.getPositive(Timer.class));
        connect(vodHostComp.getNegative(CCBootstrapPort.class), caracalClientComp.getPositive(CCBootstrapPort.class));
        connect(vodHostComp.getNegative(CCHeartbeatPort.class), heartbeatComp.getPositive(CCHeartbeatPort.class));
        trigger(Start.event, vodHostComp.control());
    }
}
