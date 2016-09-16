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

import java.util.LinkedList;
import se.sics.nstream.util.actuator.ComponentLoadTracking;

/**
 *
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class TorrentSupervisor {
    public static final int MAX_ONGOING_FILES = 1;
    public static final int MAX_FILE_BUF = 10;
    
    private final ComponentLoadTracking loadTracking;
    private final LinkedList<Integer> ongoingFiles = new LinkedList<>();
    
    public TorrentSupervisor(ComponentLoadTracking loadTracking) {
        this.loadTracking = loadTracking;
    }
    
    public int advanceFile() {
        for(Integer fileId : ongoingFiles) {
            int bufSize = loadTracking.getMaxBufferSize(fileId);
            if(bufSize < MAX_FILE_BUF) {
                return fileId;
            }
        }
        return -1;
    }
    
    public boolean canStartNewFile() {
        if(ongoingFiles.size() < MAX_ONGOING_FILES) {
            return true;
        }
        return false;
    }
    
    public void startNewFile(int fileId) {
        ongoingFiles.add(fileId);
    }
    
    public void finishFile(int fileId) {
        ongoingFiles.remove(fileId);
    }
}
