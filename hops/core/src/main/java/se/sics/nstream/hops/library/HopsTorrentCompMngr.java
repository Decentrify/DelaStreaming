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
import org.javatuples.Triplet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.kompics.Channel;
import se.sics.kompics.Component;
import se.sics.kompics.ComponentProxy;
import se.sics.kompics.Kill;
import se.sics.kompics.Positive;
import se.sics.kompics.Start;
import se.sics.kompics.network.Network;
import se.sics.kompics.timer.Timer;
import se.sics.ktoolbox.util.identifiable.Identifier;
import se.sics.ktoolbox.util.identifiable.overlay.OverlayId;
import se.sics.ktoolbox.util.idextractor.EventOverlayIdExtractor;
import se.sics.ktoolbox.util.idextractor.MsgOverlayIdExtractor;
import se.sics.ktoolbox.util.network.KAddress;
import se.sics.ktoolbox.util.network.ports.One2NChannel;
import se.sics.nstream.hops.hdfs.HDFSComp;
import se.sics.nstream.hops.hdfs.HDFSControlPort;
import se.sics.nstream.hops.hdfs.HDFSEndpoint;
import se.sics.nstream.hops.hdfs.HDFSPort;
import se.sics.nstream.hops.kafka.KafkaComp;
import se.sics.nstream.hops.kafka.KafkaControlPort;
import se.sics.nstream.hops.kafka.KafkaEndpoint;
import se.sics.nstream.hops.kafka.KafkaPort;
import se.sics.nstream.library.LibraryMngrComp;
import se.sics.nstream.report.ReportComp;
import se.sics.nstream.report.ReportPort;
import se.sics.nstream.report.TransferStatusPort;
import se.sics.nstream.storage.StorageMngrComp;
import se.sics.nstream.torrent.TorrentComp;
import se.sics.nstream.torrent.resourceMngr.TorrentResourceMngrComp;
import se.sics.nstream.torrent.resourceMngr.TorrentResourceMngrPort;
import se.sics.nstream.transfer.MyTorrent;
import se.sics.nstream.transfer.TransferMngrPort;
import se.sics.nstream.util.CoreExtPorts;
import se.sics.nutil.network.bestEffort.BestEffortNetworkComp;

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
    private final One2NChannel reportChannel;
    private final Map<Identifier, Component> report = new HashMap<>();
    private final Map<Identifier, Component> torrent = new HashMap<>();
    private final Map<Identifier, Component> networkRetry = new HashMap<>();
    private final Map<Identifier, Component> resourceMngrs = new HashMap<>();
    private final Map<Identifier, Component> hdfsMngrs = new HashMap<>();
    private final Map<Identifier, Component> kafkaMngrs = new HashMap<>();

    public HopsTorrentCompMngr(KAddress selfAdr, ComponentProxy proxy, CoreExtPorts extPorts, String logPrefix) {
        this.selfAdr = selfAdr;
        this.proxy = proxy;
        this.extPorts = extPorts;
        networkChannel = One2NChannel.getChannel("hopsTorrentMngrNetwork", extPorts.networkPort, new MsgOverlayIdExtractor());
        reportChannel = One2NChannel.getChannel("hopsTorrentMngrReport", proxy.getNegative(ReportPort.class), new EventOverlayIdExtractor());
        this.logPrefix = logPrefix;
    }

    public void startDownload(Positive<TransferMngrPort> transferMngrPort, OverlayId torrentId, List<KAddress> partners) {
        LOG.info("{}setting up torrent download {}", logPrefix, torrentId);
        Triplet<Component, Component, Component> torrentComp = setupTorrent(transferMngrPort, false, torrentId, (Optional) Optional.absent(), partners);
        proxy.trigger(Start.event, torrentComp.getValue0().control());
        proxy.trigger(Start.event, torrentComp.getValue1().control());
        proxy.trigger(Start.event, torrentComp.getValue2().control());
    }

    public void advanceDownload(Identifier torrentId, HDFSEndpoint hdfsEndpoint, Optional<KafkaEndpoint> kafkaEndpoint) {
        Component torrentComp = torrent.get(torrentId);
        LOG.info("{}setting up hdfs {}", logPrefix, torrentId);
        Component hdfsComp = setupHDFS(torrentId, hdfsEndpoint, torrentComp);
       
        Component kafkaComp = null;
        if (kafkaEndpoint.isPresent()) {
            LOG.info("{}setting up kafka {}", logPrefix, torrentId);
            kafkaComp = setupKafka(torrentId, kafkaEndpoint.get(), torrentComp);
        }
        
        LOG.info("{}setting up resource mngr {}", logPrefix, torrentId);
        Component resourceMngr = setupResourceMngr(torrentId, torrentComp, hdfsComp, Optional.fromNullable(kafkaComp));
        
        proxy.trigger(Start.event, hdfsComp.control());
        if(kafkaEndpoint.isPresent() && kafkaComp != null) {
            proxy.trigger(Start.event, kafkaComp.control());
        }
        proxy.trigger(Start.event, resourceMngr.control());
    }

    public void startUpload(Positive<TransferMngrPort> transferMngrPort, OverlayId torrentId, HDFSEndpoint hdfsEndpoint, MyTorrent torrentDef) {
        LOG.info("{}setting up torrent upload {}", logPrefix, torrentId);
        Triplet<Component, Component, Component> torrentComp = setupTorrent(transferMngrPort, true, torrentId, Optional.of(torrentDef), new ArrayList<KAddress>());
        LOG.info("{}setting up hdfs {}", logPrefix, torrentId);
        Component hdfsComp = setupHDFS(torrentId, hdfsEndpoint, torrentComp.getValue0());
        LOG.info("{}setting up resource mngr {}", logPrefix, torrentId);
        Optional<Component> kafkaComp = Optional.absent();
        Component resourceMngr = setupResourceMngr(torrentId, torrentComp.getValue0(), hdfsComp, kafkaComp);
        
        proxy.trigger(Start.event, torrentComp.getValue0().control());
        proxy.trigger(Start.event, torrentComp.getValue1().control());
        proxy.trigger(Start.event, torrentComp.getValue2().control());
        proxy.trigger(Start.event, hdfsComp.control());
        proxy.trigger(Start.event, resourceMngr.control());
    }

    private Triplet<Component, Component, Component> setupTorrent(Positive<TransferMngrPort> transferMngrPort, boolean upload, OverlayId torrentId,
            Optional<MyTorrent> torrentDef, List<KAddress> partners) {
        Component networkRetryComp = proxy.create(BestEffortNetworkComp.class, new BestEffortNetworkComp.Init(selfAdr));
        networkRetry.put(torrentId, networkRetryComp);
        proxy.connect(extPorts.timerPort, networkRetryComp.getNegative(Timer.class), Channel.TWO_WAY);
        networkChannel.addChannel(torrentId, networkRetryComp.getNegative(Network.class));
        Component torrentComp = proxy.create(TorrentComp.class, new TorrentComp.Init(selfAdr, torrentId, new HopsStorageProvider(), partners, upload, torrentDef));
        torrent.put(torrentId, torrentComp);
        Component reportComp = proxy.create(ReportComp.class, new ReportComp.Init(torrentId, 1000));
        report.put(torrentId, reportComp);
        proxy.connect(extPorts.timerPort, torrentComp.getNegative(Timer.class), Channel.TWO_WAY);
        proxy.connect(networkRetryComp.getPositive(Network.class), torrentComp.getNegative(Network.class), Channel.TWO_WAY);
        proxy.connect(transferMngrPort, torrentComp.getNegative(TransferMngrPort.class), Channel.TWO_WAY);
        proxy.connect(extPorts.timerPort, reportComp.getNegative(Timer.class), Channel.TWO_WAY);
        proxy.connect(torrentComp.getPositive(TransferStatusPort.class), reportComp.getNegative(TransferStatusPort.class), Channel.TWO_WAY);
        reportChannel.addChannel(torrentId, reportComp.getPositive(ReportPort.class));
        return Triplet.with(torrentComp, reportComp, networkRetryComp);
    }
    
    private Component setupResourceMngr(Identifier torrentId, Component torrentComp, Component hdfsComp, Optional<Component> kafkaComp) {
        TorrentResourceMngrComp.Init resourceMngrInit = new TorrentResourceMngrComp.Init(torrentId, new HopsStorageProvider());
        Component resourceMngrComp = proxy.create(TorrentResourceMngrComp.class, resourceMngrInit);
        proxy.connect(hdfsComp.getPositive(HDFSControlPort.class), resourceMngrComp.getNegative(HDFSControlPort.class), Channel.TWO_WAY);
        if(kafkaComp.isPresent()) {
            proxy.connect(kafkaComp.get().getPositive(KafkaControlPort.class), resourceMngrComp.getNegative(KafkaControlPort.class), Channel.TWO_WAY);
        }
        proxy.connect(resourceMngrComp.getPositive(TorrentResourceMngrPort.class), torrentComp.getNegative(TorrentResourceMngrPort.class), Channel.TWO_WAY);
        return resourceMngrComp;
    }

    private Component setupHDFS(Identifier torrentId, HDFSEndpoint hdfsEndpoint, Component torrentComp) {
        StorageMngrComp.Init hdfsMngrInit = new StorageMngrComp.Init(HDFSComp.class, new HDFSComp.InitBuilder(), hdfsEndpoint, torrentId);
        Component hdfsMngrComp = proxy.create(StorageMngrComp.class, hdfsMngrInit);
        proxy.connect(hdfsMngrComp.getPositive(HDFSPort.class), torrentComp.getNegative(HDFSPort.class), Channel.TWO_WAY);
        hdfsMngrs.put(torrentId, hdfsMngrComp);
        return hdfsMngrComp;
    }

    private Component setupKafka(Identifier torrentId, KafkaEndpoint kafkaEndpoint, Component torrentComp) {
        StorageMngrComp.Init kafkaMngrInit = new StorageMngrComp.Init(KafkaComp.class, new KafkaComp.InitBuilder(), kafkaEndpoint, torrentId);
        Component kafkaMngrComp = proxy.create(StorageMngrComp.class, kafkaMngrInit);
        proxy.connect(kafkaMngrComp.getPositive(KafkaPort.class), torrentComp.getNegative(KafkaPort.class), Channel.TWO_WAY);
        kafkaMngrs.put(torrentId, kafkaMngrComp);
        return kafkaMngrComp;
    }

    public void destroy(Identifier torrentId) {
        Component networkComp = networkRetry.remove(torrentId);
        Component torrentComp = torrent.remove(torrentId);
        Component hdfsComp = hdfsMngrs.remove(torrentId);
        Component kafkaComp = kafkaMngrs.remove(torrentId);

        networkChannel.removeChannel(torrentId, networkComp.getNegative(Network.class));
        proxy.trigger(Kill.event, networkComp.control());
        proxy.trigger(Kill.event, torrentComp.control());
        if (hdfsComp != null) {
            proxy.trigger(Kill.event, hdfsComp.control());
        }
        if (kafkaComp != null) {
            proxy.trigger(Kill.event, kafkaComp.control());
        }
    }
}
