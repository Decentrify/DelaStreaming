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
package se.sics.gvod.bootstrap.server;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.net.InetAddress;
import java.net.UnknownHostException;
import se.sics.gvod.address.Address;
import se.sics.gvod.bootstrap.server.peerManager.PeerManagerConfig;
import se.sics.gvod.net.VodAddress;
import se.sics.gvod.common.util.GVoDConfigException;

/**
 * @author Alex Ormenisan <aaor@sics.se>
 */
public class HostConfiguration {

    public final VodAddress self;
    public final Config config;
    public final byte[] seed;

    private HostConfiguration(Config config, VodAddress self, byte[] seed) {
        this.config = config;
        this.self = self;
        this.seed = seed;
    }

    public PeerManagerConfig.Builder getVodPeerManagerConfig() {
        return new PeerManagerConfig.Builder(config, seed);
    }

    public int getSeed() {
        return Integer.valueOf(config.getString("bootstrap.seed"));
    }

    public static class SimBuilder {

        private Config config;
        private Integer id;
        private byte[] seed;

        public SimBuilder() {
            this.config = ConfigFactory.load();
        }

        public SimBuilder(String configFile) {
            this.config = ConfigFactory.load(configFile);
        }

        public SimBuilder setId(int id) {
            this.id = id;
            return this;
        }

        public SimBuilder setSeed(byte[] seed) {
            this.seed = seed;
            return this;
        }

        public HostConfiguration finalise() throws GVoDConfigException.Missing {
            try {
                Address self = new Address(
                        InetAddress.getByName(config.getString("bootstrap.address.ip")),
                        config.getInt("bootstrap.address.port"),
                        id == null ? config.getInt("bootstrap.address.id") : id
                );
                if (seed == null) {
                    throw new GVoDConfigException.Missing("missing seed");
                }

                return new HostConfiguration(config, new VodAddress(self, -1), seed);
            } catch (UnknownHostException ex) {
                throw new GVoDConfigException.Missing(ex.getMessage());
            } catch (com.typesafe.config.ConfigException.Missing ex) {
                throw new GVoDConfigException.Missing(ex.getMessage());
            }
        }
    }

    public static class ExecBuilder {

        private Config config;
        private Address selfAddress;
        private byte[] seed;

        public ExecBuilder() {
            this.config = ConfigFactory.load();
        }

        public ExecBuilder(String configFile) {
            this.config = ConfigFactory.load(configFile);
        }

        public ExecBuilder setSelfAddress(Address selfAddress) {
            this.selfAddress = selfAddress;
            return this;
        }

        public ExecBuilder setSeed(byte[] seed) {
            this.seed = seed;
            return this;
        }

        public HostConfiguration finalise() throws GVoDConfigException.Missing {
            if (selfAddress == null) {
                throw new GVoDConfigException.Missing("self Address");
            }
            if (seed == null) {
                throw new GVoDConfigException.Missing("missing seed");
            }

            return new HostConfiguration(config, new VodAddress(selfAddress, -1), seed);
        }
    }
}