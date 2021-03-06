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
package se.sics.nstream.torrent.connMngr;

import java.util.HashMap;
import java.util.Map;
import se.sics.ktoolbox.util.network.KAddress;
import se.sics.nstream.FileId;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class SimplePeerConnection implements PeerConnection {
    private final KAddress peer;
    private final Map<FileId, FilePeerConnection> fpcs = new HashMap<>();
    
    public SimplePeerConnection(KAddress peer) {
        this.peer = peer;
    }
    
    @Override
    public KAddress getPeer() {
        return peer;
    }

    @Override
    public boolean available(FileId fileId) {
        //manual or seeder imposed limitations - none atm
        return true;
    }

    @Override
    public void addFilePeerConnection(FileId fileId, FilePeerConnection fpc) {
        fpcs.put(fileId, fpc);
    }

    @Override
    public FilePeerConnection removeFileConnection(FileId fileId) {
        return fpcs.remove(fileId);
    }
    
}
