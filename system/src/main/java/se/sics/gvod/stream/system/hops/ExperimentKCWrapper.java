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
package se.sics.gvod.stream.system.hops;

import se.sics.kompics.config.Config;
import se.sics.ktoolbox.util.config.KConfigHelper;
import se.sics.ktoolbox.util.identifiable.Identifier;
import se.sics.ktoolbox.util.identifiable.basic.IntIdentifier;
import se.sics.ktoolbox.util.network.KAddress;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class ExperimentKCWrapper {
    public final Identifier torrentId;
    public final String torrentName;
    public final String hopsURL;
    public final String hopsInDir;
    public final String hopsOutDir;
    public final KAddress senderAdr;
    public final KAddress receiverAdr;
    
    public ExperimentKCWrapper(Config config) {
        torrentId = new IntIdentifier(KConfigHelper.read(config, ExperimentKConfig.torrentId));
        torrentName = KConfigHelper.read(config, ExperimentKConfig.torrentName);
        hopsURL = KConfigHelper.read(config, ExperimentKConfig.hopsURL);
        hopsInDir = KConfigHelper.read(config, ExperimentKConfig.hopsInDir);
        hopsOutDir = KConfigHelper.read(config, ExperimentKConfig.hopsOutDir);
        senderAdr = KConfigHelper.read(config, ExperimentKConfig.senderAdr);
        receiverAdr = KConfigHelper.read(config, ExperimentKConfig.receiverAdr);
    }
}
