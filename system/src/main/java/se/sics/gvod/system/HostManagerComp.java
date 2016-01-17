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
import se.sics.gvod.manager.VoDManagerImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.gvod.cc.VoDCaracalClientComp;
import se.sics.gvod.cc.VoDCaracalClientComp.VoDCaracalClientInit;
import se.sics.gvod.cc.VoDCaracalClientConfig;
import se.sics.gvod.cc.VoDCaracalClientPort;
import se.sics.gvod.common.utility.UtilityUpdatePort;
import se.sics.gvod.core.VoDComp;
import se.sics.gvod.core.VoDInit;
import se.sics.gvod.core.VoDPort;
import se.sics.kompics.Channel;
import se.sics.kompics.Component;
import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.Init;
import se.sics.kompics.Positive;
import se.sics.kompics.network.Network;
import se.sics.kompics.timer.Timer;
import se.sics.ktoolbox.cc.bootstrap.CCOperationPort;
import se.sics.ktoolbox.cc.heartbeat.CCHeartbeatPort;
import se.sics.ktoolbox.util.address.AddressUpdatePort;
import se.sics.ktoolbox.util.status.StatusPort;

/**
 * @author Alex Ormenisan <aaor@sics.se>
 */
public class HostManagerComp extends ComponentDefinition {

    private static final Logger log = LoggerFactory.getLogger(HostManagerComp.class);

    private Positive<Network> network = requires(Network.class);
    private Positive<Timer> timer = requires(Timer.class);
    private Positive<StatusPort> externalStatus = requires(StatusPort.class);
    private Positive<CCHeartbeatPort> heartbeat = requires(CCHeartbeatPort.class);
    private Positive<AddressUpdatePort> addressUpdate = requires(AddressUpdatePort.class);

    private Component vodMngrComp;
    private Component vodComp;
    private Component vodCaracalClientComp;

    private final HostManagerKCWrapper config;

    public HostManagerComp(HostManagerInit init) {
        log.debug("starting... - self {}, bootstrap server {}",
                new Object[]{init.config.self.getId(), init.config.caracalClient});
        this.config = init.config;
        connectVoDCaracalClient(init.schemaId);
        connectVoD();
        connectVoDMngr(init.gvodSyncIFuture);
    }

    private void connectVoDCaracalClient(byte[] schemaId) {
        vodCaracalClientComp = create(VoDCaracalClientComp.class, new VoDCaracalClientInit(new VoDCaracalClientConfig(), schemaId));
        connect(vodCaracalClientComp.getNegative(Timer.class), timer, Channel.TWO_WAY);
        connect(vodCaracalClientComp.getNegative(StatusPort.class), externalStatus, Channel.TWO_WAY);
    }

    private void connectVoD() {
        vodComp = create(VoDComp.class, new VoDInit(config.getVoDConfig()));
        connect(vodComp.getNegative(Network.class), network, Channel.TWO_WAY);
        connect(vodComp.getNegative(Timer.class), timer, Channel.TWO_WAY);
        connect(vodComp.getNegative(CCHeartbeatPort.class), heartbeat, Channel.TWO_WAY);
        connect(vodComp.getNegative(VoDCaracalClientPort.class), vodCaracalClientComp.getPositive(VoDCaracalClientPort.class), Channel.TWO_WAY);
        connect(vodComp.getNegative(AddressUpdatePort.class), addressUpdate, Channel.TWO_WAY);
    }
    
    private void connectVoDMngr(SettableFuture gvodSyncIFuture) {
        this.vodMngrComp = create(VoDManagerImpl.class, new VoDManagerImpl.VoDManagerInit(config.getVoDManagerConfig()));
        gvodSyncIFuture.set(vodMngrComp.getComponent());
        connect(vodMngrComp.getNegative(VoDPort.class), vodComp.getPositive(VoDPort.class), Channel.TWO_WAY);
        connect(vodMngrComp.getNegative(UtilityUpdatePort.class), vodComp.getPositive(UtilityUpdatePort.class), Channel.TWO_WAY);
    }

    public static class HostManagerInit extends Init<HostManagerComp> {

        public final HostManagerKCWrapper config;
        public final SettableFuture gvodSyncIFuture;
        public final byte[] schemaId;

        public HostManagerInit(HostManagerKCWrapper config, SettableFuture gvodSyncIFuture, byte[] schemaId) {
            this.config = config;
            this.gvodSyncIFuture = gvodSyncIFuture;
            this.schemaId = schemaId;
        }
    }
}
