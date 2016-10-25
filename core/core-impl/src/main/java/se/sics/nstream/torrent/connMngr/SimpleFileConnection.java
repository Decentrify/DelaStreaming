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

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import se.sics.ktoolbox.util.identifiable.Identifier;
import se.sics.nstream.FileId;
import se.sics.nstream.util.actuator.ComponentLoadTracking;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class SimpleFileConnection implements FileConnection {

    private final FileId fileId;
    //**************************************************************************
    private final ComponentLoadTracking loadTracking;
    private final int maxBufSize;
    private final int maxTransferSize;
    private int transferSize = 0;
    private int potentialSlots = 1;
    //**************************************************************************
    private final Map<Identifier, FilePeerConnection> fpcs = new HashMap<>();

    public SimpleFileConnection(FileId fileId, ComponentLoadTracking loadTracking, int maxBufSize, int maxTransferSize) {
        this.fileId = fileId;
        this.loadTracking = loadTracking;
        this.maxBufSize = maxBufSize;
        this.maxTransferSize = maxTransferSize;
    }

    @Override
    public FileId getId() {
        return fileId;
    }

    @Override
    public void useSlot() {
        transferSize++;
        potentialSlots--;
    }

    @Override
    public void releaseSlot() {
        transferSize--;
    }
    
    @Override
    public void potentialSlots(int slots) {
        potentialSlots += slots;
    }

    @Override
    public boolean available() {
        //connection imposed limitation - like manual speed limitation or seeder communicated limitation- none so far
        return availableBufferSpace();
    }

    @Override
    public boolean available(Identifier peerId) {
        //connection imposed limitation - like manual speed limitation or seeder communicated limitation- none so far
        return availableBufferSpace();
    }

    private boolean availableBufferSpace() {
        int bufSize = loadTracking.getMaxBufferSize(fileId);
        if(bufSize < maxBufSize && transferSize < maxTransferSize && potentialSlots > 0) {
            return true;
        }
        return false;
    }

    @Override
    public Collection<FilePeerConnection> getPeerConnections() {
        return fpcs.values();
    }

    @Override
    public void addFilePeerConnection(Identifier peerId, FilePeerConnection fpc) {
        fpcs.put(peerId, fpc);
    }

    @Override
    public FilePeerConnection getFilePeerConnection(Identifier peerId) {
        return fpcs.get(peerId);
    }

    @Override
    public FilePeerConnection removeFilePeerConnection(Identifier peerId) {
        return fpcs.remove(peerId);
    }

    @Override
    public Set<Identifier> closeAll() {
        Set<Identifier> result = new HashSet<>();
        for(FilePeerConnection fpc : fpcs.values()) {
            Identifier peerId = fpc.getPeerConnection().getPeer().getId();
            result.add(peerId);
            fpc.close();
        }
        return result;
    }
}
