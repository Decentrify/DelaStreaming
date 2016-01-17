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
package se.sics.gvod.core;

import se.sics.gvod.core.downloadMngr.DownloadMngrKCWrapper;
import se.sics.gvod.core.connMngr.ConnMngrKCWrapper;
import se.sics.kompics.config.Config;
import se.sics.ktoolbox.util.config.KConfigHelper;
import se.sics.ktoolbox.util.identifiable.Identifier;
import se.sics.ktoolbox.util.network.KAddress;

/**
 * @author Alex Ormenisan <aaor@sics.se>
 */
public class VoDKCWrapper {

    public final Config config;
    public final KAddress self;
    public final String videoLibrary;
    
    public VoDKCWrapper(Config config, KAddress self) {
        this.config = config;
        this.self = self;
        this.videoLibrary = KConfigHelper.read(config, VoDKConfig.videoLibrary);
    }

    public DownloadMngrKCWrapper getDownloadMngrConfig(Identifier overlayId) {
        return new DownloadMngrKCWrapper(config, self, overlayId);
    }

    public ConnMngrKCWrapper getConnMngrConfig(Identifier overlayId) {
        return new ConnMngrKCWrapper(config, self, overlayId);
    }
}