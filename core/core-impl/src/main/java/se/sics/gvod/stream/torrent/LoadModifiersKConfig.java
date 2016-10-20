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

import se.sics.ktoolbox.util.config.KConfigOption.Basic;

/**
 *
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class LoadModifiersKConfig {
    public static final Basic<Double> speedUpModifier = new Basic("loadModifier.speedUp", Double.class);
    public static final Basic<Double> normalSlowDownModifier = new Basic("loadModifier.normalSlowDown", Double.class);
    public static final Basic<Double> timeoutSlowDownModifier = new Basic("loadModifier.timeoutSlowDown", Double.class);
    public static final Basic<Integer> targetQueueingDelay = new Basic("loadModifier.targetQueueingDelay", Integer.class);
    public static final Basic<Integer> maxQueueingDelay = new Basic("loadModifier.maxQueueingDelay", Integer.class);
    public static final Basic<Integer> maxLinkRTT = new Basic("loadModifier.maxLinkRTT", Integer.class);
}
