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

import com.google.common.base.Optional;
import com.google.common.collect.Sets;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import se.sics.ktoolbox.util.identifiable.Identifier;
import se.sics.ktoolbox.util.network.KAddress;
import se.sics.nstream.ConnId;
import se.sics.nstream.FileId;
import se.sics.nstream.TorrentIds;
import se.sics.nstream.torrent.conn.event.OpenTransfer;
import se.sics.nstream.torrent.transfer.dwnl.event.DownloadBlocks;
import se.sics.nstream.util.BlockDetails;
import se.sics.nstream.util.actuator.ComponentLoadTracking;

/**
 *
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class TorrentConnMngr {

    // MAX_TORRNET_BUF > MAX_FILE_BUF + MAX_FILE_TRANSFER
    // if sink is fast enough and connection is fast enough, a transfer connection will use up MAX_FILE_BUF+MAX_FILE_TRANSFER buffer slots.
    // having the MAX_TORRENT_BUF bigger than the sum of the other two, we make sure at least two files are always active. 
    // Ramping up a connection can be a bit slow, so we want to have one already ongoing when the previous one finishes.
    // Ideally we would be able to appoint LEDBAT priority to them, so that main connection could get more bandwidth through LEDBAT.
    public static final int MAX_FILE_BUF = 10;
    public static final int MAX_FILE_TRANSFER = 90;
    public static final int MAX_TORRENT_BUF = 150; 
    //**************************************************************************
    //all control from fileConnection
    private final ComponentLoadTracking loadTracking;
    //**************************************************************************
    private final Map<Identifier, PeerConnection> peerConnections = new HashMap<>();
    private final Map<FileId, FileConnection> fileConnections = new HashMap<>();
    private final TreeMap<Identifier, KAddress> connected = new TreeMap<>();
    private final LinkedList<KAddress> connCandidates = new LinkedList<>();
    //**************************************************************************
    private final int maxFileBuf;
    private final int maxFileTransfer;
    private final int maxTorrentBuf;
    private int usedSlots = 0;

    public TorrentConnMngr(ComponentLoadTracking loadTracking, List<KAddress> peers) {
        this.loadTracking = loadTracking;
        connCandidates.addAll(peers);

        this.maxFileBuf = MAX_FILE_BUF;
        this.maxFileTransfer = MAX_FILE_TRANSFER;
        this.maxTorrentBuf = MAX_TORRENT_BUF;
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
        peerConnections.put(peer.getId(), new SimplePeerConnection(peer));
    }

    public KAddress randomPeer() {
        return connected.firstEntry().getValue();
    }

    public FileId canAdvanceFile() {
        Iterator<FileConnection> it = fileConnections.values().iterator();
        while (it.hasNext()) {
            FileConnection fileConnection = it.next();
            if (fileConnection.available()) {
                return fileConnection.getId();
            }
        }
        return null;
    }

    public boolean canStartNewFile() {
        return true;
    }

    public void newFileConnection(FileId fileId) {
        FileConnection fileConnection = new SimpleFileConnection(fileId, loadTracking, maxFileBuf, maxFileTransfer);
        fileConnections.put(fileId, fileConnection);
    }

    public Set<Identifier> closeFileConnection(FileId fileId) {
        FileConnection fc = fileConnections.remove(fileId);
        return fc.closeAll();
    }
    
    public void potentialSlots(ConnId connId, int slots) {
        FileConnection fileConnection = fileConnections.get(connId.fileId);
        if (fileConnection == null) {
            throw new RuntimeException("ups");
        }
        fileConnection.potentialSlots(slots);
    }

    public ConnResult attemptSlot(FileId fileId, int blockNr, Optional<BlockDetails> irregularBlock) {
        if(usedSlots >= maxTorrentBuf) {
            return new NoConnections();
        }
        FileConnection fileConnection = fileConnections.get(fileId);
        if (fileConnection == null) {
            throw new RuntimeException("ups");
        }
        if (!fileConnection.available()) {
            return new FileConnectionBusy();
        }
        //check through already established peer-file connections
        Collection<FilePeerConnection> fileConnEstablished = fileConnection.getPeerConnections();
        Set<Identifier> checked = new HashSet<>();
        for (FilePeerConnection fpc : fileConnEstablished) {
            KAddress peer = fpc.getPeerConnection().getPeer();
            if (fileConnection.available(peer.getId()) && fpc.getPeerConnection().available(fileId)) {
                return new UseFileConnection(TorrentIds.connId(fileId, peer.getId(), true), peer, blockNr, irregularBlock);
            }
            checked.add(peer.getId());
        }
        //check through already established peer connections - that are not established on a peer-file yet (maybe used by other files)
        Set<Identifier> peerConnEstablished = Sets.difference(peerConnections.keySet(), checked);
        for (Identifier peerId : peerConnEstablished) {
            PeerConnection peerConnection = peerConnections.get(peerId);
            if (peerConnection.available(fileId)) {
                return new NewFileConnection(TorrentIds.connId(fileId, peerId, true), peerConnection.getPeer(), blockNr, irregularBlock);
            }
            checked.add(peerId);
        }

        //establish a connection to a candidate peer
        if (!connCandidates.isEmpty()) {
            KAddress peer = connCandidates.poll();
            return new NewPeerConnection(TorrentIds.connId(fileId, peer.getId(), true), peer, blockNr, irregularBlock);
        }
        return new NoConnections();
    }

    public void connectPeerFile(ConnId connId) {
        FileConnection fc = fileConnections.get(connId.fileId);
        PeerConnection pc = peerConnections.get(connId.peerId);
        FilePeerConnection fpc = new SimpleFilePeerConnection(fc, pc);
        fc.addFilePeerConnection(connId.peerId, fpc);
        pc.addFilePeerConnection(connId.fileId, fpc);
    }

    public void useSlot(UseFileConnection conn) {
        FileConnection fc = fileConnections.get(conn.connId.fileId);
        if (fc == null) {
            throw new RuntimeException("ups");
        }
        FilePeerConnection fpc = fc.getFilePeerConnection(conn.peer.getId());
        if (fpc == null) {
            throw new RuntimeException("ups");
        }
        usedSlots++;
        fpc.useSlot(conn.blockNr);
    }

    public void releaseSlot(ConnId connId, int blockNr) {
        FileConnection fc = fileConnections.get(connId.fileId);
        if (fc == null) {
            throw new RuntimeException("ups");
        }
        FilePeerConnection fpc = fc.getFilePeerConnection(connId.peerId);
        if (fpc == null) {
            throw new RuntimeException("ups");
        }
        usedSlots--;
        fpc.releaseSlot(blockNr);
    }

    public static interface ConnResult {
    }

    public static abstract class FailConnection implements ConnResult {
    }

    public static class NoConnections extends FailConnection {
    }

    public static class FileConnectionBusy extends FailConnection {
    }

    public static abstract class SuccConnection implements ConnResult {

        public final ConnId connId;
        public final KAddress peer;
        public final int blockNr;
        protected final Optional<BlockDetails> irregularBlock;

        public SuccConnection(ConnId connId, KAddress peer, int blockNr, Optional<BlockDetails> irregularBlock) {
            this.connId = connId;
            this.peer = peer;
            this.blockNr = blockNr;
            this.irregularBlock = irregularBlock;
        }
    }

    public static class UseFileConnection extends SuccConnection {

        public UseFileConnection(ConnId connId, KAddress peer, int blockNr, Optional<BlockDetails> irregularBlock) {
            super(connId, peer, blockNr, irregularBlock);
        }

        public DownloadBlocks getMsg() {
            Set<Integer> blocks = new TreeSet<>();
            blocks.add(blockNr);
            Map<Integer, BlockDetails> irregularBlocks = new HashMap<>();
            if (irregularBlock.isPresent()) {
                irregularBlocks.put(blockNr, irregularBlock.get());
            }
            return new DownloadBlocks(connId, blocks, irregularBlocks);
        }
    }

    public static class NewFileConnection extends SuccConnection {

        public NewFileConnection(ConnId connId, KAddress peer, int blockNr, Optional<BlockDetails> irregularBlock) {
            super(connId, peer, blockNr, irregularBlock);
        }

        public UseFileConnection advance() {
            return new UseFileConnection(connId, peer, blockNr, irregularBlock);
        }

        public OpenTransfer.LeecherRequest getMsg() {
            return new OpenTransfer.LeecherRequest(peer, connId);
        }
    }

    public static class NewPeerConnection extends SuccConnection {

        public NewPeerConnection(ConnId connId, KAddress peer, int blockNr, Optional<BlockDetails> irregularBlock) {
            super(connId, peer, blockNr, irregularBlock);
        }

        public NewFileConnection advance() {
            return new NewFileConnection(connId, peer, blockNr, irregularBlock);
        }
    }
}
