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

package se.sics.gvod.core.connMngr;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigException;
import se.sics.gvod.common.util.GVoDConfigException;
import se.sics.p2ptoolbox.util.network.impl.DecoratedAddress;

/**
 * @author Alex Ormenisan <aaor@sics.se>
 */
public class ConnMngrConfig {
    public final long periodicStateCheck = 30000; //30s
    public final long periodicConnUpdate = 1000; //1s
            
    private final Config config;
    public final int defaultMaxPipeline;
    private DecoratedAddress selfAddress;
    public final long updatePeriod;
    public final long reqTimeoutPeriod;
    public final int overlayId;
    public final int piecesPerBlock;
    
    private ConnMngrConfig(Config config, DecoratedAddress selfAddress, int overlayId, int defaulMaxPipeline, long updatePeriod, long reqTimeoutPeriod, int piecesPerBlock) {
        this.config = config;
        this.selfAddress = selfAddress;
        this.overlayId =  overlayId;
        this.defaultMaxPipeline = defaulMaxPipeline;
        this.updatePeriod = updatePeriod;
        this.reqTimeoutPeriod = reqTimeoutPeriod;
        this.piecesPerBlock = piecesPerBlock;
    }
    
    public DecoratedAddress getSelf() {
        return selfAddress;
    }
    
    public static class Builder {
        private final Config config;
        private final DecoratedAddress selfAddress;
        private final int overlayId;
        
        public Builder(Config config, DecoratedAddress selfAddress, int overlayId) {
            this.config = config;
            this.selfAddress = selfAddress;
            this.overlayId = overlayId;
        }
        
        public ConnMngrConfig finalise() throws GVoDConfigException.Missing {
            try {
                int defaultMaxPipeline = config.getInt("vod.connection.maxPipeline");
                long updatePeriod = config.getLong("vod.connection.updatePeriod");
                long reqTimeoutPeriod = config.getLong("vod.connection.reqTimeoutPeriod");
                int piecesPerBlock = config.getInt("vod.video.piecesPerBlock");
                return new ConnMngrConfig(config, selfAddress, overlayId, defaultMaxPipeline, updatePeriod, reqTimeoutPeriod, piecesPerBlock);
            } catch(ConfigException.Missing ex) {
                throw new GVoDConfigException.Missing(ex);
            }
        }
    }
}
