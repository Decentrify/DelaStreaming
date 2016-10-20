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

import java.util.HashSet;
import java.util.Set;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class SimpleFilePeerConnection implements FilePeerConnection {
    private final FileConnection fc;
    private final PeerConnection pc;
    private final Set<Integer> blockSlots = new HashSet<>();
    
    public SimpleFilePeerConnection(FileConnection fc, PeerConnection pc) {
        this.fc = fc;
        this.pc = pc;
    }
    
    @Override
    public PeerConnection getPeerConnection() {
        return pc;
    }

    @Override
    public FileConnection getFileConnection() {
        return fc;
    }

    @Override
    public void useSlot(int blockNr) {
        fc.useSlot();
        blockSlots.add(blockNr);
    }

    @Override
    public void releaseSlot(int blockNr) {
        fc.releaseSlot();
        blockSlots.remove(blockNr);
    }

    @Override
    public boolean isActive() {
        return blockSlots.size() > 0;
    }

    @Override
    public void close() {
        if(isActive()) {
            throw new RuntimeException("still active - or bad slot management");
        }
    }
}
