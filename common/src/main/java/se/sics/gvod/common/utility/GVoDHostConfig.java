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
package se.sics.gvod.common.utility;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.security.SecureRandom;
import java.util.Random;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.p2ptoolbox.util.network.impl.BasicAddress;
import se.sics.p2ptoolbox.util.network.impl.DecoratedAddress;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class GVoDHostConfig {

    private static final Logger LOG = LoggerFactory.getLogger("GVoDConfig");
    private static final String logPrefix = "host configuration";

    //TODO Alex to remove
    private byte[] bSeed = new byte[]{1, 2, 3, 4};

    private final Config config;
    private InetAddress ip;

    private long seed;
    private DecoratedAddress self;
    private DecoratedAddress caracalClient;

    private String videoLibrary;

    public GVoDHostConfig(Config config) {
        this(config, null);
    }

    public GVoDHostConfig(Config config, InetAddress ip) {
        this.ip = ip;
        this.config = config;
        try {
            loadHostConfig();
            loadVoDConfig();
        } catch (ConfigException.Missing ex) {
            LOG.error("{} error:{}", logPrefix, ex.getMessage());
            throw new RuntimeException("host configuration error", ex);
        }
    }

    private void loadHostConfig() throws ConfigException.Missing {
        if (config.hasPath("vod.seed")) {
            this.seed = config.getLong("vod.seed");
            LOG.info("{}: seed:{}", logPrefix, seed);
        } else {
            Random rand = new SecureRandom();
            this.seed = rand.nextLong();
            LOG.info("{}: no seed - generating random seed:{}", logPrefix, seed);
        }

        try {
            InetAddress configIp = null;
            try {
                 configIp = InetAddress.getByName(config.getString("vod.address.ip"));
            } catch (ConfigException.Missing ex) {
                if(ip == null) {
                    throw ex;
                }
            }
            if(ip == null) {
                ip = configIp;
            } else if(!ip.equals(configIp)) {
                LOG.warn("direct providedIp:{} is different than configIp:{}. Proceeding with providedIp:{}", 
                        new Object[]{ip, configIp, ip});
            }
            
            Integer selfPort = config.getInt("vod.address.port");
            Integer selfId;
            if (config.hasPath("vod.address.id")) {
                selfId = config.getInt("vod.address.id");
            } else {
                LOG.info("{}: no id - generating random id for system");
                Random rand = new Random(seed);
                selfId = rand.nextInt();
            }
            self = new DecoratedAddress(new BasicAddress(ip, selfPort, selfId));
            LOG.info("{}: self address:{}", logPrefix, self);
            if (config.hasPath("caracalClient")) {
                InetAddress ccIp = InetAddress.getByName(config.getString("caracalClient.address.ip"));
                Integer ccPort = config.getInt("caracalClient.address.port");
                Integer ccId = config.getInt("caracalClient.address.id");
                caracalClient = new DecoratedAddress(new BasicAddress(ccIp, ccPort, ccId));
            } else {
                caracalClient = self;
                DecoratedAddress src;
                
            }
            LOG.info("{}: caracalClient address:{}", logPrefix, caracalClient);
        } catch (UnknownHostException ex) {
            LOG.error("{} error:{}", logPrefix, ex.getMessage());
            throw new RuntimeException("host configuration error", ex);
        }
    }

    private void loadVoDConfig() {
        videoLibrary = config.getString("vod.libDir");
    }

    //TODO Alex eventually this should be removed
    @Deprecated
    public Config getConfig() {
        return config;
    }

    @Deprecated
    public byte[] getBSeed() {
        return bSeed;
    }

    public long getSeed() {
        return seed;
    }

    public DecoratedAddress getSelf() {
        return self;
    }

    public DecoratedAddress getCaracalClient() {
        return caracalClient;
    }

    public String getVideoLibrary() {
        return videoLibrary;
    }
}
