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
package se.sics.gvod.core.aggregation;

import se.sics.gvod.common.event.vod.Download;
import se.sics.gvod.common.utility.UtilityUpdate;
import se.sics.gvod.common.utility.UtilityUpdatePort;
import se.sics.gvod.core.connMngr.ConnMngrPort;
import se.sics.gvod.core.connMngr.event.Ready;
import se.sics.gvod.core.downloadMngr.Data;
import se.sics.gvod.core.downloadMngr.DownloadMngrPort;
import se.sics.ktoolbox.util.aggregation.AggregationRegistry;

/**
 *
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class VodCoreAggregation {
    public static void registerPorts() {
        //utility update port
        AggregationRegistry.registerPositive(UtilityUpdate.class, UtilityUpdatePort.class);
        
        //download manager port
        AggregationRegistry.registerPositive(Data.Response.class, DownloadMngrPort.class);
        AggregationRegistry.registerNegative(Data.Request.class, DownloadMngrPort.class);

        //connection manager port
        AggregationRegistry.registerPositive(Ready.class, ConnMngrPort.class);
        AggregationRegistry.registerNegative(Download.HashRequest.class, ConnMngrPort.class);
        AggregationRegistry.registerNegative(Download.HashResponse.class, ConnMngrPort.class);
        AggregationRegistry.registerNegative(Download.DataRequest.class, ConnMngrPort.class);
        AggregationRegistry.registerNegative(Download.DataResponse.class, ConnMngrPort.class);
        AggregationRegistry.registerPositive(Download.HashRequest.class, ConnMngrPort.class);
        AggregationRegistry.registerPositive(Download.HashResponse.class, ConnMngrPort.class);
        AggregationRegistry.registerPositive(Download.DataRequest.class, ConnMngrPort.class);
        AggregationRegistry.registerPositive(Download.DataResponse.class, ConnMngrPort.class);
    }
}
