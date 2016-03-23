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
import org.javatuples.Pair;
import se.sics.gvod.manager.VoDManagerImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.gvod.cc.VoDCaracalClientComp;
import se.sics.gvod.cc.VoDCaracalClientComp.VoDCaracalClientInit;
import se.sics.gvod.cc.VoDCaracalClientConfig;
import se.sics.gvod.cc.VoDCaracalClientPort;
import se.sics.gvod.common.utility.UtilityUpdatePort;
import se.sics.gvod.core.VoDComp;
import se.sics.gvod.core.VoDPort;
import se.sics.kompics.Channel;
import se.sics.kompics.Component;
import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.Init;
import se.sics.kompics.Negative;
import se.sics.kompics.Positive;
import se.sics.kompics.network.Network;
import se.sics.kompics.timer.Timer;
import se.sics.ktoolbox.cc.bootstrap.CCOperationPort;
import se.sics.ktoolbox.croupier.CroupierPort;
import se.sics.ktoolbox.overlaymngr.OverlayMngrPort;
import se.sics.ktoolbox.util.config.impl.SystemKCWrapper;
import se.sics.ktoolbox.util.network.KAddress;
import se.sics.ktoolbox.util.overlays.view.OverlayViewUpdatePort;

/**
 * @author Alex Ormenisan <aaor@sics.se>
 */
public class HostManagerComp extends ComponentDefinition {

    private static final Logger log = LoggerFactory.getLogger(HostManagerComp.class);
    private String logPrefix = " ";

    private final ExtPort extPorts;
    
    private Component vodMngrComp;
    private Pair<Component, Channel[]> vod;
    private Component vodCaracalClientComp;

    private final HostManagerKCWrapper config;
    private KAddress selfAdr;

    public HostManagerComp(HostManagerInit init) {
        selfAdr = init.selfAdr;
        logPrefix = "<nid:" + selfAdr.getId() + "> ";
        log.debug("{}starting...", logPrefix);
        extPorts = init.extPorts;
        config = init.config;
        connectVoDCaracalClient(init.schemaId);
        connectVoD();
        connectVoDMngr(init.gvodSyncIFuture);
    }

    private void connectVoDCaracalClient(byte[] schemaId) {
        vodCaracalClientComp = create(VoDCaracalClientComp.class, new VoDCaracalClientInit(new VoDCaracalClientConfig(), schemaId));
        connect(vodCaracalClientComp.getNegative(Timer.class), extPorts.timerPort, Channel.TWO_WAY);
        connect(vodCaracalClientComp.getNegative(CCOperationPort.class), extPorts.ccOpPort, Channel.TWO_WAY);
    }

    private void connectVoD() {
        VoDComp.ExtPort vodExtPorts = new VoDComp.ExtPort(extPorts.timerPort, extPorts.networkPort,
               extPorts.croupierPort, extPorts.viewUpdatePort);
        Component vodComp = create(VoDComp.class, new VoDComp.Init(selfAdr, vodExtPorts));
        Channel[] vodChannels = new Channel[2];
        vodChannels[0] = connect(vodComp.getNegative(VoDCaracalClientPort.class), 
                vodCaracalClientComp.getPositive(VoDCaracalClientPort.class), Channel.TWO_WAY);
        vodChannels[1] = connect(vodComp.getNegative(OverlayMngrPort.class), extPorts.omngrPort, Channel.TWO_WAY);
        vod = Pair.with(vodComp, vodChannels);
    }

    private void connectVoDMngr(SettableFuture gvodSyncIFuture) {
        this.vodMngrComp = create(VoDManagerImpl.class, new VoDManagerImpl.Init());
        gvodSyncIFuture.set(vodMngrComp.getComponent());
        connect(vodMngrComp.getNegative(VoDPort.class), vod.getValue0().getPositive(VoDPort.class), Channel.TWO_WAY);
        connect(vodMngrComp.getNegative(UtilityUpdatePort.class), vod.getValue0().getPositive(UtilityUpdatePort.class), Channel.TWO_WAY);
    }

    public static class HostManagerInit extends Init<HostManagerComp> {

        public final KAddress selfAdr;
        public final ExtPort extPorts;
        public final HostManagerKCWrapper config;
        public final SettableFuture gvodSyncIFuture;
        public final byte[] schemaId;

        public HostManagerInit(KAddress selfAdr, ExtPort extPorts, HostManagerKCWrapper config, 
                SettableFuture gvodSyncIFuture, byte[] schemaId) {
            this.selfAdr = selfAdr;
            this.extPorts = extPorts;
            this.config = config;
            this.gvodSyncIFuture = gvodSyncIFuture;
            this.schemaId = schemaId;
        }
    }

    public static class ExtPort {

        public final Positive<Timer> timerPort;
        public final Positive<Network> networkPort;
        public final Positive<CCOperationPort> ccOpPort;
        public final Positive<OverlayMngrPort> omngrPort;
        public final Positive<CroupierPort> croupierPort;
        public final Negative<OverlayViewUpdatePort> viewUpdatePort;

        public ExtPort(Positive<Timer> timerPort, Positive<Network> networkPort, Positive<CCOperationPort> ccOpPort, 
                Positive<OverlayMngrPort> omngrPort, Positive<CroupierPort> croupierPort,
                Negative<OverlayViewUpdatePort> viewUpdatePort) {
            this.timerPort = timerPort;
            this.networkPort = networkPort;
            this.ccOpPort = ccOpPort;
            this.omngrPort = omngrPort;
            this.croupierPort = croupierPort;
            this.viewUpdatePort = viewUpdatePort;
        }
    }
}
