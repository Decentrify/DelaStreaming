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

import java.net.InetAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.gvod.core.VoDKCWrapper;
import se.sics.gvod.manager.VoDManagerKCWrapper;
import se.sics.kompics.config.Config;
import se.sics.ktoolbox.util.config.impl.SystemKCWrapper;
import se.sics.ktoolbox.util.network.KAddress;
import se.sics.ktoolbox.util.network.basic.BasicAddress;
import se.sics.ktoolbox.util.network.nat.NatAwareAddress;
import se.sics.ktoolbox.util.network.nat.NatAwareAddressImpl;

/**
 * @author Alex Ormenisan <aaor@sics.se>
 */
public class HostManagerKCWrapper {

    private static final Logger LOG = LoggerFactory.getLogger("GVoDConfig");

    public final Config config;
    public final SystemKCWrapper systemConfig;
    public final KAddress self;
    public final KAddress caracalClient;
    
    public HostManagerKCWrapper(Config config, InetAddress selfIp) {
        this.config = config;
        this.systemConfig = new SystemKCWrapper(config);
        this.self = NatAwareAddressImpl.open(new BasicAddress(selfIp, 30000, systemConfig.id));
        this.caracalClient = self;
    }


//    public DecoratedAddress getCaracalClient() {
//        return hostConfig.getCaracalClient();
//    }

    public VoDManagerKCWrapper getVoDManagerConfig() {
        return new VoDManagerKCWrapper(config, self);
    }

    public VoDKCWrapper getVoDConfig() {
        return new VoDKCWrapper(config, self);
    }

//    public List<Address> getCaracalNodes() {
//        try {
//            Config config = hostConfig.getConfig();
//            ArrayList<Address> cBootstrap = new ArrayList<Address>();
//            InetAddress ip = InetAddress.getByName(config.getString("caracal.address.ip"));
//            int port = config.getInt("caracal.address.port");
//            cBootstrap.add(new Address(ip, port, null));
//            return cBootstrap;
//        } catch (ConfigException.Missing ex) {
//            throw new RuntimeException("Caracal Bootstrap configuration problem - missing config", ex);
//        } catch (UnknownHostException ex) {
//            throw new RuntimeException("Caracal Bootstrap configuration problem - bad ip", ex);
//        }
//    }
}
