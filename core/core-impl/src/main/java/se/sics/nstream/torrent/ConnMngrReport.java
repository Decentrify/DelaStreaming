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
package se.sics.nstream.torrent;

import java.util.Map;
import se.sics.ktoolbox.util.identifiable.Identifier;
import se.sics.ledbat.core.AppCongestionWindow;
import se.sics.ledbat.core.util.ThroughputHandler;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class ConnMngrReport {
    public final int totalDownloadSpeed;
    public final int totalUploadSpeed;
    
    public ConnMngrReport(int totalDownloadSpeed, int totalUploadSpeed) {
        this.totalDownloadSpeed = totalDownloadSpeed;
        this.totalUploadSpeed = totalUploadSpeed;
    }

    public ConnMngrReport resetDownloadSpeed() {
        return new ConnMngrReport(0, totalUploadSpeed);
    }
    
    public static ConnMngrReport transferReport(Map<Identifier, AppCongestionWindow> seeders, Map<Identifier, ThroughputHandler> leechers) {
        long now = System.currentTimeMillis();
        int totalDownloadSpeed = 0;
        for (AppCongestionWindow pc : seeders.values()) {
            totalDownloadSpeed += pc.downloadSpeed(now);
        }
        int totalUploadSpeed = 0;
        for(ThroughputHandler th : leechers.values()) {
            totalUploadSpeed += th.speed(now);
        }
        return new ConnMngrReport(totalDownloadSpeed, totalUploadSpeed);
    }
}
