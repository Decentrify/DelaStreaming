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
package se.sics.gvod.stream.torrent;

import se.sics.kompics.config.Config;
import se.sics.ktoolbox.util.config.KConfigHelper;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class LoadModifiersKCWrapper {
    public final double speedUpModifier;
    public final double normalSlowDownModifier;
    public final double timeoutSlowDownModifier;
    public final int targetQueueingDelay;
    public final int maxQueueingDelay;
    public final int maxLinkRTT;
    
    public LoadModifiersKCWrapper(Config config) {
        speedUpModifier = KConfigHelper.read(config, LoadModifiersKConfig.speedUpModifier);
        normalSlowDownModifier = KConfigHelper.read(config, LoadModifiersKConfig.normalSlowDownModifier);
        timeoutSlowDownModifier = KConfigHelper.read(config, LoadModifiersKConfig.timeoutSlowDownModifier);
        targetQueueingDelay = KConfigHelper.read(config, LoadModifiersKConfig.targetQueueingDelay);
        maxQueueingDelay = KConfigHelper.read(config, LoadModifiersKConfig.maxQueueingDelay);
        maxLinkRTT = KConfigHelper.read(config, LoadModifiersKConfig.maxLinkRTT);
    }
}
