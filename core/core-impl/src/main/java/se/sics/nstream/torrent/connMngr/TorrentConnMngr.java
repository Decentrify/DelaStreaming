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

import com.google.common.collect.Sets;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import se.sics.ktoolbox.util.identifiable.Identifier;
import se.sics.ktoolbox.util.network.KAddress;
import se.sics.nstream.torrent.FileIdentifier;

/**
 *
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class TorrentConnMngr {
    
    private final Map<Identifier, PeerConnection> peerConnections = new HashMap<>();
    private final Map<FileIdentifier, FileConnection> fileConnections = new HashMap<>();
    private final TreeMap<Identifier, KAddress> connected = new TreeMap<>();
    private final LinkedList<KAddress> connCandidates = new LinkedList<>();

    public TorrentConnMngr(List<KAddress> peers) {
        connCandidates.addAll(peers);
    }
    
    public void newCandidates(LinkedList<KAddress> peers) {
        connCandidates.addAll(peers);
    }
    
    public boolean hasConnCandidates() {
        return !connCandidates.isEmpty();
    }
    
    public KAddress getConnCandidate() {
        return connCandidates.pollFirst();
    }
    
    public void connected(KAddress peer) {
        connected.put(peer.getId(), peer);
    }
    
    public KAddress randomPeer() {
        return connected.firstEntry().getValue();
    }
    
    public void newFileConnection(FileIdentifier fileId) {
        FileConnection fileConnection = new SimpleFileConnection(fileId);
        fileConnections.put(fileId, fileConnection);
    }
    
    public ConnResult getSlot(FileIdentifier fileId, int block) {
        FileConnection fileConnection = fileConnections.get(fileId);
        if (fileConnection == null) {
            throw new RuntimeException("ups");
        }
        if (!fileConnection.available()) {
            return new FileConnectionBusy();
        }
        //check through already established peer-file connections
        Set<FilePeerConnection> ongoing = fileConnection.getPeerConnections();
        Set<Identifier> auxOngoing = new HashSet<>();
        for (FilePeerConnection fpc : ongoing) {
            KAddress peer = fpc.getPeerConnection().getPeer();
            if (fileConnection.available(peer.getId()) && fpc.getPeerConnection().available(fileId)) {
                fpc.useSlot();
                return new OpenedConnection(peer);
            }
            auxOngoing.add(peer.getId());
        }
        //check through already established peer connections - that are not established on a peer-file yet (maybe used by other files
        Set<Identifier> aux = Sets.difference(peerConnections.keySet(), auxOngoing);
        for (Identifier peer : aux) {
            PeerConnection peerConnection = peerConnections.get(peer);
            if (peerConnection.available(fileId)) {
                FilePeerConnection fpc = new SimpleFilePeerConnection(fileConnection, peerConnection);
                fileConnection.addFilePeerConnection(peer, fpc);
                peerConnection.addFilePeerConnection(fileId, fpc);
                fpc.useSlot();
                return new NewConnection(peerConnection.getPeer());
            }
        }
        //establish a connection with a completely new peer
        for (KAddress peer : connected.values()) {
            PeerConnection peerConnection = new SimplePeerConnection(peer);
            FilePeerConnection fpc = new SimpleFilePeerConnection(fileConnection, peerConnection);
            fileConnection.addFilePeerConnection(peer.getId(), fpc);
            peerConnection.addFilePeerConnection(fileId, fpc);
            fpc.useSlot();
            return new NewConnection(peerConnection.getPeer());
        }
        return new NoConnections();
    }
    
    public void releaseSlot(FileIdentifier fileId, Identifier peerId) {
        FileConnection fc = fileConnections.get(fileId);
        if(fc == null) {
            throw new RuntimeException("ups");
        }
        FilePeerConnection fpc = fc.getFilePeerConnection(peerId);
        if(fpc == null) {
            throw new RuntimeException("ups");
        }
        fpc.releaseSlot();
        if(!fpc.isActive()) {
            PeerConnection pc = peerConnections.get(peerId);
            if(pc == null) {
                throw new RuntimeException("ups");
            }
            pc.removeFileConnection(fileId);
            fc.removeFilePeerConnection(peerId);
        }
    }

    public static interface ConnResult {
    }

    public static class NoConnections implements ConnResult {
    }

    public static class FileConnectionBusy implements ConnResult {
    }

    public static class OpenedConnection implements ConnResult {

        public final KAddress peer;

        public OpenedConnection(KAddress peer) {
            this.peer = peer;
        }
    }

    public static class NewConnection implements ConnResult {

        public final KAddress peer;

        public NewConnection(KAddress peer) {
            this.peer = peer;
        }
    }
}
