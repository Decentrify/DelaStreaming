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
package se.sics.nstream.hops.library;

import com.google.common.base.Optional;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.javatuples.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.kompics.Channel;
import se.sics.kompics.Component;
import se.sics.kompics.ComponentProxy;
import se.sics.kompics.Positive;
import se.sics.kompics.Start;
import se.sics.kompics.network.Network;
import se.sics.kompics.timer.Timer;
import se.sics.ktoolbox.util.identifiable.Identifier;
import se.sics.ktoolbox.util.idextractor.MsgOverlayIdExtractor;
import se.sics.ktoolbox.util.network.KAddress;
import se.sics.ktoolbox.util.network.ports.One2NChannel;
import se.sics.nstream.hops.hdfs.HDFSComp;
import se.sics.nstream.hops.hdfs.HDFSEndpoint;
import se.sics.nstream.hops.hdfs.HDFSPort;
import se.sics.nstream.library.LibraryMngrComp;
import se.sics.nstream.torrent.TorrentComp;
import se.sics.nstream.transfer.TransferMngrPort;
import se.sics.nstream.util.CoreExtPorts;
import se.sics.nstream.util.TransferDetails;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class HopsTorrentCompMngr {
    private static final Logger LOG = LoggerFactory.getLogger(LibraryMngrComp.class);
    private final String logPrefix;
    private final KAddress selfAdr;
    private final ComponentProxy proxy;
    private final CoreExtPorts extPorts;
    private final One2NChannel networkChannel;
    private final Map<Identifier, Component> torrent = new HashMap<>();
    private final Map<Identifier, Component> hdfs = new HashMap<>();
    private final Map<Identifier, Component> kafka = new HashMap<>();
    
    public HopsTorrentCompMngr(KAddress selfAdr, ComponentProxy proxy, CoreExtPorts extPorts, String logPrefix) {
        this.selfAdr = selfAdr;
        this.proxy = proxy;
        this.extPorts = extPorts;
        networkChannel = One2NChannel.getChannel("hopsTorrentMngrNetwork", extPorts.networkPort, new MsgOverlayIdExtractor());
        this.logPrefix = logPrefix;
    }
    
    public void startDownload(Positive<TransferMngrPort> transferMngrPort, Identifier torrentId, List<KAddress> partners) {
        LOG.info("{}setting up torrent download {}", logPrefix, torrentId);
        Component torrentComp = setupTorrent(transferMngrPort, false, torrentId, (Optional)Optional.absent(), partners);
        proxy.trigger(Start.event, torrentComp.control());
    }
    
    public void startUpload(Positive<TransferMngrPort> transferMngrPort, Identifier torrentId, HDFSEndpoint hdfsEndpoint, Pair<byte[], TransferDetails> extendedDetails) {
        LOG.info("{}setting up torrent upload {}", logPrefix, torrentId);
        Component torrentComp = setupTorrent(transferMngrPort, true, torrentId, Optional.of(extendedDetails), new ArrayList<KAddress>());
        LOG.info("{}setting up hdfs {}", logPrefix, torrentId);
        Component hdfsComp = setupHDFS(torrentId, hdfsEndpoint, torrentComp);
        proxy.trigger(Start.event, torrentComp.control());
        proxy.trigger(Start.event, hdfsComp.control());
    }
    
    private Component setupTorrent(Positive<TransferMngrPort> transferMngrPort, boolean upload, Identifier torrentId, 
            Optional<Pair<byte[], TransferDetails>> extendedDetails, List<KAddress> partners) {
        Component torrentComp = proxy.create(TorrentComp.class, new TorrentComp.Init(selfAdr, torrentId, new HopsStorageProvider(), partners, upload, extendedDetails));
        proxy.connect(extPorts.timerPort, torrentComp.getNegative(Timer.class), Channel.TWO_WAY);
//        proxy.connect(extPorts.networkPort, torrentComp.getNegative(Network.class), Channel.TWO_WAY);
        networkChannel.addChannel(torrentId, torrentComp.getNegative(Network.class));
        proxy.connect(transferMngrPort, torrentComp.getNegative(TransferMngrPort.class), Channel.TWO_WAY);
        torrent.put(torrentId, torrentComp);
        return torrentComp;
    }
    
    private Component setupHDFS(Identifier torrentId, HDFSEndpoint hdfsEndpoint, Component torrentComp) {
        Component hdfsComp = proxy.create(HDFSComp.class, new HDFSComp.Init(hdfsEndpoint));
        proxy.connect(hdfsComp.getPositive(HDFSPort.class), torrentComp.getNegative(HDFSPort.class), Channel.TWO_WAY);
        hdfs.put(torrentId, hdfsComp);
        return hdfsComp;
    }
}
