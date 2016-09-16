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

import com.google.common.base.Optional;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import org.javatuples.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.kompics.Channel;
import se.sics.kompics.Component;
import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.Handler;
import se.sics.kompics.Kill;
import se.sics.kompics.Killed;
import se.sics.kompics.Negative;
import se.sics.kompics.PortType;
import se.sics.kompics.Positive;
import se.sics.kompics.Start;
import se.sics.kompics.network.Network;
import se.sics.kompics.timer.SchedulePeriodicTimeout;
import se.sics.kompics.timer.Timeout;
import se.sics.kompics.timer.Timer;
import se.sics.ktoolbox.util.identifiable.Identifier;
import se.sics.ktoolbox.util.idextractor.MsgOverlayIdExtractor;
import se.sics.ktoolbox.util.idextractor.SourceHostIdExtractor;
import se.sics.ktoolbox.util.network.KAddress;
import se.sics.ktoolbox.util.network.ports.ChannelIdExtractor;
import se.sics.ktoolbox.util.network.ports.One2NChannel;
import se.sics.ktoolbox.util.reference.KReference;
import se.sics.ktoolbox.util.reference.KReferenceException;
import se.sics.ktoolbox.util.reference.KReferenceFactory;
import se.sics.ktoolbox.util.result.DelayedExceptionSyncHandler;
import se.sics.ktoolbox.util.result.Result;
import se.sics.nstream.report.TransferStatusPort;
import se.sics.nstream.torrent.conn.ConnectionComp;
import se.sics.nstream.torrent.conn.ConnectionPort;
import se.sics.nstream.torrent.conn.event.CloseTransfer;
import se.sics.nstream.torrent.conn.event.DetailedState;
import se.sics.nstream.torrent.conn.event.OpenTransferDefinition;
import se.sics.nstream.torrent.conn.event.Seeder;
import se.sics.nstream.torrent.connMngr.TorrentConnMngr;
import se.sics.nstream.torrent.fileMngr.TFileWrite;
import se.sics.nstream.torrent.fileMngr.TorrentFileMngr;
import se.sics.nstream.torrent.old.TorrentConfig;
import se.sics.nstream.torrent.transfer.DwnlConnComp;
import se.sics.nstream.torrent.transfer.DwnlConnPort;
import se.sics.nstream.torrent.transfer.UpldConnComp;
import se.sics.nstream.torrent.transfer.UpldConnPort;
import se.sics.nstream.torrent.transfer.dwnl.event.CompletedBlocks;
import se.sics.nstream.torrent.transfer.dwnl.event.DownloadBlocks;
import se.sics.nstream.torrent.transfer.upld.event.GetBlocks;
import se.sics.nstream.torrent.util.EventTorrentConnIdExtractor;
import se.sics.nstream.torrent.util.MsgTorrentConnIdExtractor;
import se.sics.nstream.torrent.util.TorrentConnId;
import se.sics.nstream.transfer.Transfer;
import se.sics.nstream.transfer.TransferMngrPort;
import se.sics.nstream.util.BlockDetails;
import se.sics.nstream.util.TransferDetails;
import se.sics.nstream.util.actuator.ComponentLoadTracking;
import se.sics.nutil.tracking.load.QueueLoadConfig;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class TorrentComp extends ComponentDefinition {

    private static final Logger LOG = LoggerFactory.getLogger(TorrentComp.class);
    private String logPrefix;

    private static final int DEF_FILE_ID = 0;
    private static final int DEF_PIECE_SIZE = 1024;
    private static final int DEF_BLOCK_SIZE = 1024;
    private static final long REFRESH_CONN_PERIOD = 10000;
    private static final int CONN_RETRY = 5;
    private static final BlockDetails defaultDefBlock = new BlockDetails(DEF_BLOCK_SIZE * DEF_PIECE_SIZE, DEF_BLOCK_SIZE, DEF_PIECE_SIZE, DEF_PIECE_SIZE);

    private final TorrentConfig torrentConfig;
    private final Identifier torrentId;
    private final KAddress selfAdr;
    //**************************************************************************
    Positive<Network> networkPort = requires(Network.class);
    //storage ports
    List<Positive> requiredPorts = new ArrayList<>();
    //**************************************************************************
    Positive<Timer> timerPort = requires(Timer.class);
    //torrent ports
    Positive<DwnlConnPort> dwnlConnPort = requires(DwnlConnPort.class);
    Positive<UpldConnPort> upldConnPort = requires(UpldConnPort.class);
    //conn ports
    Positive<ConnectionPort> connPort = requires(ConnectionPort.class);
    //other
    Positive<TransferMngrPort> transferMngrPort = requires(TransferMngrPort.class);
    Negative<TransferStatusPort> transferStatusPort = provides(TransferStatusPort.class);
    //local multiplexing channels
    private One2NChannel<Network> connNetworkChannel;
    private One2NChannel<Network> transferNetworkChannel;
    private One2NChannel<DwnlConnPort> dwnlConnChannel;
    private One2NChannel<UpldConnPort> upldConnChannel;
    //**************************************************************************
    private final ComponentLoadTracking componentTracking;
    private TorrentFileMngr fileMngr;
    private final TorrentConnMngr connMngr;
    //**************************************************************************
    private ConnectionState connState;
    private ServeDefinitionState serveDefState;
    private GetDefinitionState getDefState;
    //**************************************************************************
    private UUID refreshConnTid;

    public TorrentComp(Init init) {
        torrentId = init.torrentId;
        selfAdr = init.selfAdr;
        logPrefix = "<nid:" + selfAdr.getId() + ",oid:" + torrentId + ">";
        torrentConfig = new TorrentConfig();

        componentTracking = new ComponentLoadTracking("torrent", this.proxy, new QueueLoadConfig(config()));
        buildChannels();
        storageProvider(init.storageProvider);

        Optional<Pair<Integer, BlockDetails>> details = Optional.absent();
        if (init.transferDetails.isPresent()) {
            TransferDetails transferDetails = init.transferDetails.get();
            TorrentDef td = TorrentDef.buildDefinition(transferDetails.base.baseDetails, transferDetails.extended, transferDetails.torrent, defaultDefBlock);
            initializeTorrentFileMngr(transferDetails, true);
            details = Optional.of(td.details);
            serveDefState = new ServeDefinitionState(td);
        }
        connState = new ConnectionState(details);
        connMngr = new TorrentConnMngr(init.partners);

        subscribe(handleStart, control);
        subscribe(handleKilled, control);

        subscribe(handleSeederConnectSuccess, connPort);
        subscribe(handleSeederConnectTimeout, connPort);
        subscribe(handleSeederConnectSuspect, connPort);
        subscribe(handleDetailedStateSuccess, connPort);
        subscribe(handleDetailedStateTimeout, connPort);
        subscribe(handleDetailedStateNone, connPort);
        subscribe(handleOpenTransferDefLeecherResp, connPort);
        subscribe(handleOpenTransferDefLeecherTimeout, connPort);
        subscribe(handleOpenTransferDefSeeder, connPort);
        subscribe(handleCloseTransfer, connPort);

        subscribe(handleGetBlocks, upldConnPort);
        subscribe(handleCompletedBlocks, dwnlConnPort);
        subscribe(handleTransferDetails, transferMngrPort);
    }

    private void buildChannels() {
        ChannelIdExtractor<?, Identifier> overlayIdExtractor = new MsgOverlayIdExtractor();
        connNetworkChannel = One2NChannel.getChannel("torrentNetwork", networkPort, overlayIdExtractor, TorrentCompFilters.connInclusionFilter);
        ChannelIdExtractor<?, Identifier> connIdExtractor = new MsgTorrentConnIdExtractor(new SourceHostIdExtractor());
        transferNetworkChannel = One2NChannel.getChannel("torrentPeerFileNetwork", networkPort, connIdExtractor, TorrentCompFilters.transferInclusionFilter);
        dwnlConnChannel = One2NChannel.getChannel("torrentDwnlConn", (Negative) dwnlConnPort.getPair(), new EventTorrentConnIdExtractor());
        upldConnChannel = One2NChannel.getChannel("torrentUpldConn", (Negative) upldConnPort.getPair(), new EventTorrentConnIdExtractor());
    }

    private void storageProvider(StorageProvider storageProvider) {
        for (Class<PortType> r : storageProvider.requiresPorts()) {
            requiredPorts.add(requires(r));
        }
    }

    private void initializeTorrentFileMngr(TransferDetails td, boolean upload) {
        DelayedExceptionSyncHandler deh = new DelayedExceptionSyncHandler() {

            @Override
            public boolean fail(Result<Object> result) {
                throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
            }
        };
        fileMngr = new TorrentFileMngr(config(), proxy, deh, componentTracking, td, upload);
    }

    Handler handleStart = new Handler<Start>() {

        @Override
        public void handle(Start event) {
            LOG.info("{}starting", logPrefix);
            scheduleRefreshConnections();
            if (connState != null) {
                connState.connectComp();
                connState.start();
            }
            if (serveDefState != null) {
                serveDefState.start();
            }
        }
    };

    @Override
    public void tearDown() {
        if (connState != null) {
            connState.tearDown();
        }
        if (serveDefState != null) {
            serveDefState.tearDown();
        }
        if (getDefState != null) {
            getDefState.tearDown();
        }
    }

    Handler handleKilled = new Handler<Killed>() {
        @Override
        public void handle(Killed event) {
            if (connState != null) {
                connState.handleKilled(event);
            }
            if (serveDefState != null) {
                serveDefState.handleKilled(event);
            }
            if (getDefState != null) {
                getDefState.handleKilled(event);
            }
        }
    };

    //**************************************************************************
    Handler handleSeederConnectSuccess = new Handler<Seeder.Success>() {
        @Override
        public void handle(Seeder.Success resp) {
            LOG.info("{}connect to:{} success", logPrefix, resp.target);
            connMngr.connected(resp.target);
            if (serveDefState == null && getDefState == null) {
                trigger(new DetailedState.Request(connMngr.randomPeer()), connPort);
            }
        }
    };

    Handler handleSeederConnectTimeout = new Handler<Seeder.Timeout>() {
        @Override
        public void handle(Seeder.Timeout resp) {
            LOG.warn("{}connect timeout", logPrefix);
        }
    };

    Handler handleSeederConnectSuspect = new Handler<Seeder.Suspect>() {
        @Override
        public void handle(Seeder.Suspect event) {
            LOG.warn("{}connect suspect", logPrefix);
        }
    };

    Handler handleDetailedStateSuccess = new Handler<DetailedState.Success>() {
        @Override
        public void handle(DetailedState.Success event) {
            LOG.info("{}detailed state - success", logPrefix);
            if (serveDefState == null && getDefState == null) {
                trigger(new OpenTransferDefinition.LeecherRequest(connMngr.randomPeer()), connPort);
                getDefState = new GetDefinitionState(event.lastBlockDetails);
                getDefState.start();
            }
        }
    };

    Handler handleDetailedStateTimeout = new Handler<DetailedState.Timeout>() {
        @Override
        public void handle(DetailedState.Timeout event) {
            LOG.warn("{}detailed state - timeout", logPrefix);
        }
    };

    Handler handleDetailedStateNone = new Handler<DetailedState.None>() {
        @Override
        public void handle(DetailedState.None event) {
            LOG.warn("{}detailed state - none", logPrefix);
        }
    };

    Handler handleOpenTransferDefLeecherResp = new Handler<OpenTransferDefinition.LeecherResponse>() {
        @Override
        public void handle(OpenTransferDefinition.LeecherResponse resp) {
            LOG.info("{}transfer definition - leecher response", logPrefix);
            if (serveDefState != null) {
                LOG.warn("{}missing logic", logPrefix);
                return;
            }
            getDefState.startInstance(connMngr.randomPeer());
        }
    };

    Handler handleOpenTransferDefLeecherTimeout = new Handler<OpenTransferDefinition.LeecherTimeout>() {
        @Override
        public void handle(OpenTransferDefinition.LeecherTimeout resp) {
            LOG.warn("{}transfer definition - leecher timeout", logPrefix);
        }
    };

    Handler handleOpenTransferDefSeeder = new Handler<OpenTransferDefinition.SeederRequest>() {
        @Override
        public void handle(OpenTransferDefinition.SeederRequest req) {
            LOG.info("{}transfer definition - seeder request", logPrefix);
            serveDefState.startInstance(req.peer);
            answer(req, req.answer(true));
        }
    };

    Handler handleCloseTransfer = new Handler<CloseTransfer.Indication>() {
        @Override
        public void handle(CloseTransfer.Indication event) {
            if (event.connId.leecher) {
                LOG.warn("{}close transfer - conn:{} as leecher", new Object[]{logPrefix, event.connId});
                return;
            } else {
                LOG.info("{}close transfer - conn:{} as seeder", new Object[]{logPrefix, event.connId});
                serveDefState.killInstance(event.connId.targetId);
            }
        }
    };

    //**************************************************************************
    Handler handleRefreshConnections = new Handler<TorrentComp.RefreshConnectionsTimeout>() {
        @Override
        public void handle(TorrentComp.RefreshConnectionsTimeout event) {
            LOG.warn("{}refresh connections", logPrefix);
        }
    };
    //**************************************************************************
    Handler handleTransferDetails = new Handler<Transfer.DownloadResponse>() {
        @Override
        public void handle(Transfer.DownloadResponse resp) {
        }
    };
    //**************************************************************************
    Handler handleGetBlocks = new Handler<GetBlocks.Request>() {
        @Override
        public void handle(GetBlocks.Request req) {
            LOG.debug("{}conn:{} req blocks:{}", new Object[]{logPrefix, req.connId, req.blocks});
            if (req.connId.fileId.fileId == DEF_FILE_ID) {
                serveDefState.getBlocks(req);
            }
        }
    };

    Handler handleCompletedBlocks = new Handler<CompletedBlocks>() {
        @Override
        public void handle(CompletedBlocks event) {
            if (getDefState != null && event.connId.fileId.fileId == DEF_FILE_ID) {
                getDefState.handleBlockCompleted(event);
            } else {
                writeToFile(event.connId.fileId.fileId, event.blocks);
                updateConn(event.connId, event.blocks);
            }
        }
    };

    private void writeToFile(int fileNr, Map<Integer, byte[]> blocks) {
        TFileWrite fileWriter = fileMngr.writeTo(fileNr);
        for (Map.Entry<Integer, byte[]> block : blocks.entrySet()) {
            KReference<byte[]> ref = KReferenceFactory.getReference(block.getValue());
            fileWriter.block(block.getKey(), ref);
            try {
                ref.release();
            } catch (KReferenceException ex) {
                throw new RuntimeException(ex);
            }
        }
    }
    
    private void updateConn(TorrentConnId connId, Map<Integer, byte[]> blocks) {
        for(Integer blockNr : blocks.keySet()) {
            connMngr.releaseSlot(connId.fileId, connId.targetId);
        }
    }

    //*********************************STATES***********************************
    private void completeGetDefinitionState(TorrentDef torrentDef) {
        getDefState = null;
        if (serveDefState == null) {
            throw new RuntimeException("ups");
        }
        serveDefState = new ServeDefinitionState(torrentDef);
        serveDefState.start();
    }

    public class ConnectionState {

        private final Optional<Pair<Integer, BlockDetails>> details;
        private Component connComp;

        public ConnectionState(Optional<Pair<Integer, BlockDetails>> details) {
            this.details = details;
        }

        public void connectComp() {
            connComp = create(ConnectionComp.class, new ConnectionComp.Init(torrentId, selfAdr, details));
            connect(connComp.getNegative(Timer.class), timerPort, Channel.TWO_WAY);
            connNetworkChannel.addChannel(torrentId, connComp.getNegative(Network.class));
            connect(connComp.getPositive(ConnectionPort.class), connPort.getPair(), Channel.TWO_WAY);
        }

        public void start() {
            trigger(Start.event, connComp.control());
            if (connMngr.hasConnCandidates()) {
                trigger(new Seeder.Connect(connMngr.getConnCandidate()), connPort);
            }
        }

        public void tearDown() {
            trigger(Kill.event, connComp.control());
        }

        public void handleKilled(Killed event) {
            if (connComp != null && event.component.id().equals(connComp.id())) {
                connNetworkChannel.removeChannel(torrentId, connComp.getNegative(Network.class));
            }
        }
    }

    public class GetDefinitionState {

        private final TorrentDef.Builder tdBuilder;
        //**********************************************************************
        private Component leecherComp;
        private TorrentConnId connId;

        public GetDefinitionState(Pair<Integer, BlockDetails> details) {
            this.tdBuilder = new TorrentDef.Builder(details);
        }

        public void start() {
            LOG.info("{}get definition - start", logPrefix);
        }

        public void tearDown() {
        }

        public void startInstance(KAddress peer) {
            connId = new TorrentConnId(peer.getId(), new FileIdentifier(torrentId, DEF_FILE_ID), true);
            LOG.info("{}get definition - start connection:{}", logPrefix, connId);

            leecherComp = create(DwnlConnComp.class, new DwnlConnComp.Init(connId, selfAdr, peer, defaultDefBlock));
            connect(leecherComp.getNegative(Timer.class), timerPort, Channel.TWO_WAY);
            transferNetworkChannel.addChannel(connId, leecherComp.getNegative(Network.class));
            dwnlConnChannel.addChannel(connId, leecherComp.getPositive(DwnlConnPort.class));
            trigger(Start.event, leecherComp.control());

            Set<Integer> blocks = new HashSet<>();
            Map<Integer, BlockDetails> irregularBlockDetails = new HashMap<>();
            int lastBlockNr = tdBuilder.details.getValue0() - 1;
            for (int i = 0; i <= lastBlockNr; i++) {
                blocks.add(i);
            }
            irregularBlockDetails.put(lastBlockNr, tdBuilder.details.getValue1());
            trigger(new DownloadBlocks(connId, blocks, irregularBlockDetails), dwnlConnPort);
        }

        private void killInstance() {
            if (leecherComp == null) {
                return;
            }
            LOG.info("{}get definition - killing connection:{}", logPrefix, connId);
            trigger(Kill.event, leecherComp.control());
        }

        public void handleKilled(Killed event) {
            if (leecherComp == null || !leecherComp.id().equals(event.component.id())) {
                return;
            }
            LOG.info("{}get definition - killed connection:{}", logPrefix, connId);

            transferNetworkChannel.removeChannel(connId, leecherComp.getNegative(Network.class));
            dwnlConnChannel.removeChannel(connId, leecherComp.getPositive(DwnlConnPort.class));
            leecherComp = null;
            connId = null;
        }

        public void handleBlockCompleted(CompletedBlocks event) {
            if (!event.connId.equals(connId)) {
                throw new RuntimeException("ups");
            }

            tdBuilder.addBlocks(event.blocks);

            if (tdBuilder.blocksComplete()) {
                interpretTorrentByte(tdBuilder.assembleTorrent());
            }
        }

        private void interpretTorrentByte(byte[] torrentBytes) {
            trigger(new Transfer.DownloadRequest(torrentId, Result.success(torrentBytes)), transferMngrPort);
            trigger(new CloseTransfer.Request(connId), connPort);
            killInstance();
        }

        public void handleTransferDetails(Transfer.DownloadResponse resp) {
            tdBuilder.setBase(resp.transferDetails.base.baseDetails);
            tdBuilder.setExtended(resp.transferDetails.extended);
            tearDown();
            completeGetDefinitionState(tdBuilder.build());
        }
    }

    public class ServeDefinitionState {

        private final TorrentDef td;
        //<peerId, compId, comp>
        private final Table<Identifier, UUID, Pair<Component, TorrentConnId>> seederComponents = HashBasedTable.create();

        public ServeDefinitionState(TorrentDef td) {
            this.td = td;
        }

        public void start() {
            LOG.info("{}serve definition - start", logPrefix);
        }

        public void tearDown() {
        }

        public void startInstance(KAddress peer) {
            TorrentConnId connId = new TorrentConnId(peer.getId(), new FileIdentifier(torrentId, DEF_FILE_ID), false);
            LOG.info("{}serve definition - start connection:{}", logPrefix, connId);

            Component transferComp = create(UpldConnComp.class, new UpldConnComp.Init(connId, selfAdr, defaultDefBlock));
            connect(transferComp.getNegative(Timer.class), timerPort, Channel.TWO_WAY);
            transferNetworkChannel.addChannel(connId, transferComp.getNegative(Network.class));
            upldConnChannel.addChannel(connId, transferComp.getPositive(UpldConnPort.class));

            seederComponents.put(peer.getId(), transferComp.id(), Pair.with(transferComp, connId));
            trigger(Start.event, transferComp.control());
        }

        public void killInstance(Identifier peerId) {
            Map<UUID, Pair<Component, TorrentConnId>> aux = seederComponents.row(peerId);
            if (aux.isEmpty()) {
                return;
            }
            if (aux.size() > 1) {
                throw new RuntimeException("ups");
            }
            Pair<Component, TorrentConnId> aux2 = aux.entrySet().iterator().next().getValue();
            TorrentConnId connId = aux2.getValue1();
            Component transferComp = aux2.getValue0();
            LOG.info("{}serve definition - killing connection:{}", logPrefix, connId);
            trigger(Kill.event, transferComp.control());
        }

        public void handleKilled(Killed event) {
            Map<Identifier, Pair<Component, TorrentConnId>> aux = seederComponents.column(event.component.id());
            if (aux.isEmpty()) {
                return;
            }
            if (aux.size() > 1) {
                throw new RuntimeException("ups");
            }
            Map.Entry<Identifier, Pair<Component, TorrentConnId>> aux2 = aux.entrySet().iterator().next();
            Identifier peerId = aux2.getKey();
            TorrentConnId connId = aux2.getValue().getValue1();
            Component transferComp = aux2.getValue().getValue0();
            LOG.info("{}serve definition - killed connection:{}", logPrefix, connId);

            transferNetworkChannel.removeChannel(connId, transferComp.getNegative(Network.class));
            upldConnChannel.removeChannel(connId, transferComp.getPositive(UpldConnPort.class));
            seederComponents.remove(peerId, transferComp.id());
        }

        public void getBlocks(GetBlocks.Request req) {
            Map<Integer, BlockDetails> irregularBlocks = new TreeMap<>();
            Map<Integer, KReference<byte[]>> blockValues = new TreeMap<>();
            int lastBlockNr = td.details.getValue0() - 1;
            if (req.blocks.contains(lastBlockNr)) {
                irregularBlocks.put(lastBlockNr, td.details.getValue1());
            }
            for (Integer blockNr : req.blocks) {
                KReference<byte[]> ref = KReferenceFactory.getReference(td.blocks.get(blockNr));
                blockValues.put(blockNr, ref);
            }
            answer(req, req.success(blockValues, irregularBlocks));
        }
    }

    public static class Init extends se.sics.kompics.Init<TorrentComp> {

        public final KAddress selfAdr;
        public final Identifier torrentId;
        public final StorageProvider storageProvider;
        public List<KAddress> partners;
        public final boolean upload;
        public final Optional<TransferDetails> transferDetails;

        public Init(KAddress selfAdr, Identifier torrentId, StorageProvider storageProvider, List<KAddress> partners,
                boolean upload, Optional<TransferDetails> transferDetails) {
            this.selfAdr = selfAdr;
            this.torrentId = torrentId;
            this.storageProvider = storageProvider;
            this.upload = upload;
            this.transferDetails = transferDetails;
            this.partners = partners;
        }
    }

    private void scheduleRefreshConnections() {
        SchedulePeriodicTimeout spt = new SchedulePeriodicTimeout(REFRESH_CONN_PERIOD, REFRESH_CONN_PERIOD);
        RefreshConnectionsTimeout rt = new RefreshConnectionsTimeout(spt);
        refreshConnTid = rt.getTimeoutId();
        spt.setTimeoutEvent(rt);
        trigger(spt, timerPort);
    }

    public static class RefreshConnectionsTimeout extends Timeout {

        public RefreshConnectionsTimeout(SchedulePeriodicTimeout spt) {
            super(spt);
        }
    }
}
