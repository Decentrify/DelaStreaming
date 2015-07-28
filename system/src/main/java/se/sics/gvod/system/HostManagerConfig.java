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

import com.typesafe.config.Config;
import java.net.InetAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.gvod.bootstrap.cclient.CaracalPSManagerConfig;
import se.sics.gvod.bootstrap.client.BootstrapClientConfig;
import se.sics.gvod.bootstrap.server.BootstrapServerConfig;
import se.sics.gvod.common.util.GVoDConfigException;
import se.sics.gvod.common.utility.GVoDHostConfig;
import se.sics.gvod.common.utility.GVoDReferenceConfig;
import se.sics.gvod.core.VoDConfig;
import se.sics.gvod.manager.VoDManagerConfig;
import se.sics.p2ptoolbox.util.network.impl.DecoratedAddress;

/**
 * @author Alex Ormenisan <aaor@sics.se>
 */
public class HostManagerConfig {
    private static final Logger LOG = LoggerFactory.getLogger("GVoDConfig");
    
    private GVoDHostConfig hostConfig;
    private GVoDReferenceConfig referenceConfig;

    public HostManagerConfig(Config config) {
        this(config, null);
    }
    
    public HostManagerConfig(Config config, InetAddress ip) {
        this.hostConfig = new GVoDHostConfig(config, ip);
        this.referenceConfig = new GVoDReferenceConfig(config);
    }

    public long getSeed() {
        return hostConfig.getSeed();
    }

    public DecoratedAddress getSelf() {
        return hostConfig.getSelf();
    }

    public DecoratedAddress getCaracalClient() {
        return hostConfig.getCaracalClient();
    }

    public CaracalPSManagerConfig getCaracalPSManagerConfig() {
        DecoratedAddress self = hostConfig.getSelf();
        //TODO Alex - low priority fix - remove
        byte[] bSeed = new byte[]{1, 2, 3, 4};
        return new CaracalPSManagerConfig.Builder(hostConfig.getConfig(), bSeed).setSelfAddress(self.getIp(), self.getPort() + 1, self.getId()).finalise();
    }

    public VoDManagerConfig getVoDManagerConfig() {
        return new VoDManagerConfig(hostConfig, referenceConfig);
    }
    
    public VoDConfig getVoDConfig() {
        return new VoDConfig(hostConfig, referenceConfig);
    }
    
    //TODO Alex all Config.Missing exception should be caught in the parsing classes GVoDHostConfig and GVoDReferenceConfig
    public BootstrapClientConfig getBootstrapClientConfig() {
        try {
            return new BootstrapClientConfig.Builder(hostConfig.getConfig(), hostConfig.getSelf(), hostConfig.getCaracalClient(), hostConfig.getBSeed()).finalise();
        } catch (GVoDConfigException.Missing ex) {
            LOG.error("configuration error:{}", ex.getMessage());
            throw new RuntimeException("configuration error", ex);
        }
    }
    
    public BootstrapServerConfig getBootstrapServerConfig() {
            return new BootstrapServerConfig(hostConfig.getConfig(), hostConfig.getSelf(), hostConfig.getBSeed());
    }
}
