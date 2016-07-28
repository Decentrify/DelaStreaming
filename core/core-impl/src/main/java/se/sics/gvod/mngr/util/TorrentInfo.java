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
package se.sics.gvod.mngr.util;

import java.util.HashMap;
import java.util.Map;
import se.sics.nstream.library.util.TorrentStatus;
import se.sics.ktoolbox.util.identifiable.Identifier;
import se.sics.ktoolbox.util.network.KAddress;

/**
 *
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class TorrentInfo {

    private TorrentStatus status;
    public final Map<Identifier, KAddress> partners;
    public final double progress; //percentage
    public final long downloadSpeed; //byte/s
    public final long uploadSpeed; //byte/s
    
    public TorrentInfo(TorrentStatus status, Map<Identifier, KAddress> partners, double progress, long downloadSpeed, long uploadSpeed) {
        this.status = status;
        this.partners = partners;
        this.progress = progress;
        this.downloadSpeed = downloadSpeed;
        this.uploadSpeed = uploadSpeed;
    }
    
    public TorrentStatus getStatus() {
        return status;
    }
    
    public void finishDownload() {
        status = TorrentStatus.UPLOADING;
    }
    
    public static TorrentInfo none() {
        return new TorrentInfo(TorrentStatus.NONE, new HashMap<Identifier, KAddress>(), 0, 0, 0);
    }
}
