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

import se.sics.kompics.config.Config;
import se.sics.ktoolbox.util.aggregation.AggregationLevel;
import se.sics.ktoolbox.util.config.KConfigHelper;
import se.sics.ktoolbox.util.identifiable.Identifier;
import se.sics.ktoolbox.util.network.KAddress;


/**
 * @author Alex Ormenisan <aaor@sics.se>
 */
public class ConnMngrKCWrapper {
    public final long periodicStateCheck = 30000; //30s
    public final long periodicConnUpdate = 1000; //1s
            
    private final Config configCore;
    public final int defaultMaxPipeline;
    public final KAddress selfAddress;
    public final long updatePeriod;
    public final long reqTimeoutPeriod;
    public final Identifier overlayId;
    public final int piecesPerBlock;
    
    public final AggregationLevel connMngrAggLevel;
    public final long connMngrAggPeriod;
    
    public ConnMngrKCWrapper(Config config, KAddress selfAddress, Identifier overlayId) {
        this.configCore = config;
        this.selfAddress = selfAddress;
        this.overlayId =  overlayId;
        defaultMaxPipeline = KConfigHelper.read(config, ConnMngrKConfig.defaultMaxPipeline);
        updatePeriod = KConfigHelper.read(config, ConnMngrKConfig.updatePeriod);
        reqTimeoutPeriod = KConfigHelper.read(config, ConnMngrKConfig.reqTimeoutPeriod);
        piecesPerBlock = KConfigHelper.read(config, ConnMngrKConfig.piecesPerBlock);
        connMngrAggLevel = KConfigHelper.read(configCore, ConnMngrKConfig.aggLevel);
        connMngrAggPeriod = KConfigHelper.read(configCore, ConnMngrKConfig.aggPeriod);
    }
}
