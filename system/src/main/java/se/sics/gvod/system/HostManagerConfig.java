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
import com.typesafe.config.ConfigException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.caracaldb.Address;
import se.sics.gvod.common.utility.GVoDHostConfig;
import se.sics.gvod.common.utility.GVoDReferenceConfig;
import se.sics.gvod.core.VoDConfig;
import se.sics.gvod.manager.VoDManagerConfig;
import se.sics.ktoolbox.cc.common.config.CCBootstrapConfig;
import se.sics.ktoolbox.cc.common.config.CaracalClientConfig;
import se.sics.p2ptoolbox.croupier.CroupierConfig;
import se.sics.p2ptoolbox.util.config.SystemConfig;
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

    public VoDManagerConfig getVoDManagerConfig() {
        return new VoDManagerConfig(hostConfig, referenceConfig);
    }

    public VoDConfig getVoDConfig() {
        return new VoDConfig(hostConfig, referenceConfig);
    }

    public CroupierConfig getCroupierConfig() {
        return new CroupierConfig(hostConfig.getConfig());
    }

    public SystemConfig getSystemConfig() {
        return new SystemConfig(hostConfig.getConfig());
    }

    public CaracalClientConfig getCaracalClientConfig() {
        return new CaracalClientConfig(hostConfig.getConfig());
    }

    public List<Address> getCaracalNodes() {
        try {
            Config config = hostConfig.getConfig();
            ArrayList<Address> cBootstrap = new ArrayList<Address>();
            InetAddress ip = InetAddress.getByName(config.getString("caracal.address.ip"));
            int port = config.getInt("caracal.address.port");
            cBootstrap.add(new Address(ip, port, null));
            return cBootstrap;
        } catch (ConfigException.Missing ex) {
            throw new RuntimeException("Caracal Bootstrap configuration problem - missing config", ex);
        } catch (UnknownHostException ex) {
            throw new RuntimeException("Caracal Bootstrap configuration problem - bad ip", ex);
        }
    }
}
