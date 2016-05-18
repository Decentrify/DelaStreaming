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
package se.sics.gvod.stream.system;

import java.util.List;
import se.sics.kompics.config.Config;
import se.sics.ktoolbox.util.config.KConfigHelper;
import se.sics.ktoolbox.util.identifiable.Identifier;
import se.sics.ktoolbox.util.identifiable.basic.IntIdentifier;
import se.sics.ktoolbox.util.network.KAddress;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class StreamHostKCWrapper {
    public final KAddress selfAdr;
    public final Identifier torrentId;
    public final List<KAddress> partners;
    public final String filePath;
    public final String hashPath;
    public final boolean download;
    
    public StreamHostKCWrapper(Config config) {
        selfAdr = KConfigHelper.read(config, StreamHostKConfig.selfAdr);
        partners = KConfigHelper.read(config, StreamHostKConfig.partners);
        torrentId = new IntIdentifier(KConfigHelper.read(config, StreamHostKConfig.torrentId));
        filePath = KConfigHelper.read(config, StreamHostKConfig.filePath);
        hashPath = KConfigHelper.read(config, StreamHostKConfig.hashPath);
        download = KConfigHelper.read(config, StreamHostKConfig.download);
    }
}
