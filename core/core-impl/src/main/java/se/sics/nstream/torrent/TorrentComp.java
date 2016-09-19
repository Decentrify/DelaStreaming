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
 * GNU General Public License for more defLastBlock.
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
import java.util.LinkedList;
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
import se.sics.kompics.timer.CancelPeriodicTimeout;
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
import se.sics.nstream.report.event.TransferStatus;
import se.sics.nstream.torrent.conn.ConnectionComp;
import se.sics.nstream.torrent.conn.ConnectionPort;
import se.sics.nstream.torrent.conn.event.CloseTransfer;
import se.sics.nstream.torrent.conn.event.DetailedState;
import se.sics.nstream.torrent.conn.event.OpenTransfer;
import se.sics.nstream.torrent.conn.event.Seeder;
import se.sics.nstream.torrent.connMngr.TorrentConnMngr;
import se.sics.nstream.torrent.connMngr.TorrentConnMngr.ConnResult;
import se.sics.nstream.torrent.fileMngr.TFileRead;
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
import se.sics.nstream.transfer.MyTorrent;
import se.sics.nstream.transfer.MyTorrent.Manifest;
import se.sics.nstream.transfer.MyTorrent.ManifestDef;
import se.sics.nstream.transfer.Transfer;
import se.sics.nstream.transfer.TransferMngrPort;
import se.sics.nstream.util.BlockDetails;
import se.sics.nstream.util.actuator.ComponentLoadTracking;
import se.sics.nstream.util.result.HashReadCallback;
import se.sics.nstream.util.result.ReadCallback;
import se.sics.nutil.tracking.load.QueueLoadConfig;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class TorrentComp extends ComponentDefinition {

    private static final Logger LOG = LoggerFactory.getLogger(TorrentComp.class);
    private String logPrefix;

    private static final int DEF_FILE_ID = 0;
    private static final long REFRESH_CONN_PERIOD = 10000;
    private static final int CONN_RETRY = 5;
    private static final long ADVANCE_PERIOD = 1000;

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
    private ServeFilesState serveFilesState;
    private GetFilesState getFilesState;
    //**************************************************************************
    private UUID refreshConnTid;
    private UUID advanceTid;

    public TorrentComp(Init init) {
        torrentId = init.torrentId;
        selfAdr = init.selfAdr;
        logPrefix = "<nid:" + selfAdr.getId() + ",oid:" + torrentId + ">";
        torrentConfig = new TorrentConfig();

        componentTracking = new ComponentLoadTracking("torrent", this.proxy, new QueueLoadConfig(config()));
        buildChannels();
        storageProvider(init.storageProvider);

        Optional<MyTorrent.ManifestDef> manifest = Optional.absent();
        if (init.torrentDef.isPresent()) {
            initializeTorrent(init.torrentDef.get(), true);
            manifest = Optional.of(init.torrentDef.get().manifest.getDef());
        }
        connState = new ConnectionState(manifest);
        connMngr = new TorrentConnMngr(componentTracking, init.partners);

        subscribe(handleStart, control);
        subscribe(handleKilled, control);

        subscribe(handleAdvance, timerPort);

        subscribe(handleSeederConnectSuccess, connPort);
        subscribe(handleSeederConnectTimeout, connPort);
        subscribe(handleSeederConnectSuspect, connPort);
        subscribe(handleDetailedStateSuccess, connPort);
        subscribe(handleDetailedStateTimeout, connPort);
        subscribe(handleDetailedStateNone, connPort);
        subscribe(handleOpenTransferLeecherResp, connPort);
        subscribe(handleOpenTransferLeecherTimeout, connPort);
        subscribe(handleOpenTransferSeeder, connPort);
        subscribe(handleCloseTransfer, connPort);

        subscribe(handleGetBlocks, upldConnPort);
        subscribe(handleCompletedBlocks, dwnlConnPort);
        subscribe(handleTransferDetails, transferMngrPort);
    }

    private void buildChannels() {
        ChannelIdExtractor<?, Identifier> overlayIdExtractor = new MsgOverlayIdExtractor();
        connNetworkChannel = One2NChannel.getChannel("torrentConnNetwork", networkPort, overlayIdExtractor, TorrentCompFilters.connInclusionFilter);
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

    private void initializeTorrent(MyTorrent torrentDef, boolean upload) {
        DelayedExceptionSyncHandler deh = new DelayedExceptionSyncHandler() {

            @Override
            public boolean fail(Result<Object> result) {
                throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
            }
        };
        fileMngr = new TorrentFileMngr(config(), proxy, deh, componentTracking, torrentDef, upload);
        serveDefState = new ServeDefinitionState(torrentDef);
        serveFilesState = new ServeFilesState();
        getFilesState = new GetFilesState();
    }

    Handler handleStart = new Handler<Start>() {

        @Override
        public void handle(Start event) {
            LOG.info("{}starting", logPrefix);
            if (connState != null) {
                connState.connectComp();
                connState.start();
            }
        }
    };

    @Override
    public void tearDown() {
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
    Handler handleAdvance = new Handler<AdvanceTimeout>() {
        @Override
        public void handle(AdvanceTimeout event) {
            LOG.debug("{}advance", logPrefix);
            tryAdvance();
        }
    };

    private void tryAdvance() {
        if (fileMngr == null) {
            return;
        }
        if (fileMngr.complete()) {
            cancelAdvance();
            trigger(new TransferStatus.DownloadDone(torrentId), transferStatusPort);
        }

        int fileId = connMngr.canAdvanceFile();
        if (fileId != -1) {
            TFileWrite fileWriter = fileMngr.writeTo(fileId);
            if (fileWriter.isComplete()) {
                fileMngr.complete(fileId);
                Set<Identifier> closeConn = connMngr.closeFileConnection(new FileIdentifier(torrentId, fileId));
                getFilesState.killInstances(fileId, closeConn);
                return;
            }
            int batchSize = 2;
            Pair<Integer, Optional<BlockDetails>> block = nextBlock(fileId);
            while (block != null && batchSize > 0) {
                batchSize--;
                if (!advanceConn(fileId, block.getValue0(), block.getValue1())) {
                    break;
                }
                block = nextBlock(fileId);
            }
            if (block != null) {
                fileWriter.resetBlock(block.getValue0());
            }
            if (batchSize == 0) {
                return;
            }
        }
        if (connMngr.canStartNewFile()) {
            if (fileMngr.hasPending()) {
                fileId = fileMngr.nextPending();
                LOG.debug("{}advance new file:{}", logPrefix, fileId);
                connMngr.newFileConnection(new FileIdentifier(torrentId, fileId));
                return;
            }
        }
        LOG.warn("{}nothing", logPrefix);
    }

    private Pair<Integer, Optional<BlockDetails>> nextBlock(int fileId) {
        TFileWrite fileWriter = fileMngr.writeTo(fileId);
        if (fileWriter.isComplete()) {
            fileMngr.complete(fileId);
            return null;
        }
        fileWriter.hasHashes();
        if (fileWriter.hasBlocks()) {
            //for the moment we get hashes with blocks - same management
            return fileWriter.requestBlock();
        }
        return null;
    }

    private boolean advanceConn(int fileId, int blockNr, Optional<BlockDetails> irregularBlock) {
        ConnResult result = connMngr.attemptSlot(new FileIdentifier(torrentId, fileId), blockNr, irregularBlock);
        if (result instanceof TorrentConnMngr.FailConnection) {
            return false;
        } else if (result instanceof TorrentConnMngr.UseFileConnection) {
            TorrentConnMngr.UseFileConnection openedConn = (TorrentConnMngr.UseFileConnection) result;
            getFilesState.useFileConnection(openedConn);
            return true;
        } else if (result instanceof TorrentConnMngr.NewFileConnection) {
            TorrentConnMngr.NewFileConnection newConn = (TorrentConnMngr.NewFileConnection) result;
            getFilesState.newFileConnection(newConn);
            return true;
        } else if (result instanceof TorrentConnMngr.NewPeerConnection) {
            TorrentConnMngr.NewPeerConnection newConn = (TorrentConnMngr.NewPeerConnection) result;
            getFilesState.newPeerConnection(newConn);
            return true;
        } else {
            throw new RuntimeException("ups");
        }
    }

    //**************************************************************************
    Handler handleSeederConnectSuccess = new Handler<Seeder.Success>() {
        @Override
        public void handle(Seeder.Success resp) {
            LOG.info("{}connect to:{} success", logPrefix, resp.target);
            connMngr.connected(resp.target);
            if (serveDefState == null && getDefState == null) {
                trigger(new DetailedState.Request(connMngr.randomPeer()), connPort);
            }
            if (getFilesState != null) {
                getFilesState.peerConnected(resp.target);
            }
        }
    };

    Handler handleSeederConnectTimeout = new Handler<Seeder.Timeout>() {
        @Override
        public void handle(Seeder.Timeout resp) {
            LOG.warn("{}connect timeout", logPrefix);
            throw new RuntimeException("ups");
        }
    };

    Handler handleSeederConnectSuspect = new Handler<Seeder.Suspect>() {
        @Override
        public void handle(Seeder.Suspect event) {
            LOG.warn("{}connect suspect", logPrefix);
            throw new RuntimeException("ups");
        }
    };

    Handler handleDetailedStateSuccess = new Handler<DetailedState.Success>() {
        @Override
        public void handle(DetailedState.Success event) {
            LOG.info("{}detailed state - success", logPrefix);
            if (serveDefState == null && getDefState == null) {
                KAddress peer = connMngr.randomPeer();
                TorrentConnId connId = new TorrentConnId(peer.getId(), new FileIdentifier(torrentId, DEF_FILE_ID), true);
                trigger(new OpenTransfer.LeecherRequest(peer, connId), connPort);
                getDefState = new GetDefinitionState(event.manifestDef);
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

    Handler handleOpenTransferLeecherResp = new Handler<OpenTransfer.LeecherResponse>() {
        @Override
        public void handle(OpenTransfer.LeecherResponse resp) {
            LOG.info("{}open transfer - leecher response", logPrefix);
            if (serveDefState == null && getDefState != null) {
                //def phase
                getDefState.startInstance(connMngr.randomPeer());
            }
            if (getFilesState != null) {
                getFilesState.fileConnected(resp.connId, resp.peer);
            }
        }
    };

    Handler handleOpenTransferLeecherTimeout = new Handler<OpenTransfer.LeecherTimeout>() {
        @Override
        public void handle(OpenTransfer.LeecherTimeout resp) {
            LOG.warn("{}transfer definition - leecher timeout", logPrefix);
            throw new RuntimeException("ups");
        }
    };

    Handler handleOpenTransferSeeder = new Handler<OpenTransfer.SeederRequest>() {
        @Override
        public void handle(OpenTransfer.SeederRequest req) {
            LOG.info("{}transfer definition - seeder request", logPrefix);
            if (req.connId.fileId.fileId == DEF_FILE_ID) {
                serveDefState.startInstance(req.peer);
                answer(req, req.answer(true));
            } else {
                serveFilesState.startInstance(req.connId, req.peer, MyTorrent.defaultDataBlock);
                answer(req, req.answer(true));
            }
        }
    };

    Handler handleCloseTransfer = new Handler<CloseTransfer.Indication>() {
        @Override
        public void handle(CloseTransfer.Indication event) {
            if (event.connId.leecher) {
                LOG.warn("{}close transfer - conn:{} as leecher", new Object[]{logPrefix, event.connId});
                throw new RuntimeException("ups");
            } else {
                LOG.info("{}close transfer - conn:{} as seeder", new Object[]{logPrefix, event.connId});
                serveDefState.killInstance(event.connId.targetId);
            }
        }
    };

    //**************************************************************************
    Handler handleTransferDetails = new Handler<Transfer.DownloadResponse>() {
        @Override
        public void handle(Transfer.DownloadResponse resp) {
            LOG.info("{}starting transfer", logPrefix);
            initializeTorrent(resp.torrent, false);
            scheduleAdvance();
            tryAdvance();
        }
    };
    //**************************************************************************
    Handler handleGetBlocks = new Handler<GetBlocks.Request>() {
        @Override
        public void handle(GetBlocks.Request req) {
            LOG.debug("{}conn:{} req blocks:{}", new Object[]{logPrefix, req.connId, req.blocks});
            if (req.connId.fileId.fileId == DEF_FILE_ID) {
                serveDefState.getBlocks(req);
            } else {
                new GetBlocksHandler(req).handle();
            }
        }
    };

    private class GetBlocksHandler {

        final GetBlocks.Request req;
        final Map<Integer, byte[]> hashes = new HashMap<>();
        final Map<Integer, KReference<byte[]>> blocks = new HashMap<>();

        GetBlocksHandler(GetBlocks.Request req) {
            this.req = req;
        }

        //once all defBlocks/hashes are retrived, it will answer back
        void handle() {
            final TFileRead fileReader = fileMngr.readFrom(req.connId.fileId.fileId);
            fileReader.setCacheHint(req.connId.targetId, req.cacheHint);
            for (final Integer blockNr : req.blocks) {
                fileReader.readBlock(blockNr, new ReadCallback() {

                    @Override
                    public boolean fail(Result<KReference<byte[]>> result) {
                        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
                    }

                    @Override
                    public boolean success(Result<KReference<byte[]>> result) {
                        blocks.put(blockNr, result.getValue());
                        if (done()) {
                            answer(req, req.success(hashes, blocks, fileReader.getIrregularBlocks()));
                        }
                        return true;
                    }
                });
                fileReader.readHash(blockNr, new HashReadCallback() {

                    @Override
                    public boolean fail(Result<KReference<byte[]>> result) {
                        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
                    }

                    @Override
                    public boolean success(Result<KReference<byte[]>> result) {
                        hashes.put(blockNr, result.getValue().getValue().get());
                        if (done()) {
                            answer(req, req.success(hashes, blocks, fileReader.getIrregularBlocks()));
                        }
                        return true;
                    }
                });
            }
        }

        public boolean done() {
            return hashes.size() == req.blocks.size() && blocks.size() == req.blocks.size();
        }
    }
    //**************************************************************************

    Handler handleCompletedBlocks = new Handler<CompletedBlocks>() {
        @Override
        public void handle(CompletedBlocks event) {
            LOG.info("{}conn:{} completed blocks:{} hashes:{}", new Object[]{logPrefix, event.connId, event.blocks.keySet(), event.hashes.keySet()});
            if (event.connId.fileId.fileId == DEF_FILE_ID) {
                if (getDefState == null) {
                    throw new RuntimeException("ups");
                }
                getDefState.handleBlockCompleted(event);
            } else {
                writeToFile(event.connId.fileId.fileId, event.blocks, event.hashes);
                updateConn(event.connId, event.blocks);
                tryAdvance();
            }
        }
    };

    private void writeToFile(int fileNr, Map<Integer, byte[]> blocks, Map<Integer, byte[]> hashes) {
        TFileWrite fileWriter = fileMngr.writeTo(fileNr);
        fileWriter.hashes(hashes, new HashSet<Integer>());
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
        for (Integer blockNr : blocks.keySet()) {
            connMngr.releaseSlot(connId, blockNr);
        }
    }

    //*********************************STATES***********************************
    private void completeGetDefinitionState(MyTorrent torrentDef) {
        getDefState = null;
        if (serveDefState == null) {
            throw new RuntimeException("ups");
        }
    }

    public class ConnectionState {

        private final Optional<MyTorrent.ManifestDef> manifestDef;
        private Component connComp;

        public ConnectionState(Optional<MyTorrent.ManifestDef> manifestDef) {
            this.manifestDef = manifestDef;
        }

        public void connectComp() {
            connComp = create(ConnectionComp.class, new ConnectionComp.Init(torrentId, selfAdr, manifestDef));
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

        public void handleKilled(Killed event) {
            if (connComp != null && event.component.id().equals(connComp.id())) {
                connNetworkChannel.removeChannel(torrentId, connComp.getNegative(Network.class));
            }
        }
    }

    public class GetDefinitionState {

        private final MyTorrent.Builder tdBuilder;
        //**********************************************************************
        private Component leecherComp;
        private TorrentConnId connId;

        public GetDefinitionState(ManifestDef manifestDef) {
            this.tdBuilder = new MyTorrent.Builder(manifestDef);
            trigger(new TransferStatus.DownloadStarting(torrentId), transferStatusPort);
        }

        public void startInstance(KAddress peer) {
            connId = new TorrentConnId(peer.getId(), new FileIdentifier(torrentId, DEF_FILE_ID), true);
            LOG.info("{}get definition - start connection:{}", logPrefix, connId);

            leecherComp = create(DwnlConnComp.class, new DwnlConnComp.Init(connId, selfAdr, peer, MyTorrent.defaultDefBlock, false));
            connect(leecherComp.getNegative(Timer.class), timerPort, Channel.TWO_WAY);
            transferNetworkChannel.addChannel(connId, leecherComp.getNegative(Network.class));
            dwnlConnChannel.addChannel(connId, leecherComp.getPositive(DwnlConnPort.class));
            trigger(Start.event, leecherComp.control());

            Set<Integer> blocks = new HashSet<>();
            Map<Integer, BlockDetails> irregularBlockDetails = new HashMap<>();
            int lastBlockNr = tdBuilder.manifestBuilder.nrBlocks - 1;
            for (int i = 0; i <= lastBlockNr; i++) {
                blocks.add(i);
            }
            irregularBlockDetails.put(lastBlockNr, tdBuilder.manifestBuilder.lastBlock);
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

            tdBuilder.manifestBuilder.addBlocks(event.blocks);

            if (tdBuilder.manifestBuilder.blocksComplete()) {
                interpretManifest(tdBuilder.manifestBuilder.build());
            }
        }

        private void interpretManifest(Manifest manifest) {
            trigger(new Transfer.DownloadRequest(torrentId, Result.success(manifest)), transferMngrPort);
            trigger(new CloseTransfer.Request(connId), connPort);
            killInstance();
        }

        public void handleTransferDetails(Transfer.DownloadResponse resp) {
            tdBuilder.setBase(resp.torrent.base);
            tdBuilder.setExtended(resp.torrent.extended);
            tearDown();
            completeGetDefinitionState(tdBuilder.build());
        }
    }

    public class ServeDefinitionState {

        private final MyTorrent td;
        //<peerId, compId, comp>
        private final Table<Identifier, UUID, Pair<Component, TorrentConnId>> seederComponents = HashBasedTable.create();

        public ServeDefinitionState(MyTorrent td) {
            this.td = td;
        }

        public void startInstance(KAddress peer) {
            TorrentConnId connId = new TorrentConnId(peer.getId(), new FileIdentifier(torrentId, DEF_FILE_ID), false);
            LOG.info("{}serve definition - start connection:{}", logPrefix, connId);

            Component transferComp = create(UpldConnComp.class, new UpldConnComp.Init(connId, selfAdr, td.manifest.defaultBlock, false));
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
            int lastBlockNr = td.manifest.nrBlocks - 1;
            if (req.blocks.contains(lastBlockNr)) {
                irregularBlocks.put(lastBlockNr, td.manifest.lastBlock);
            }
            for (Integer blockNr : req.blocks) {
                KReference<byte[]> ref = KReferenceFactory.getReference(td.manifest.manifestBlocks.get(blockNr));
                blockValues.put(blockNr, ref);
            }
            if (req.withHashes) {
                throw new RuntimeException("ups");
            }
            answer(req, req.success(new HashMap<Integer, byte[]>(), blockValues, irregularBlocks));
        }
    }

    public class GetFilesState {

        private final Map<UUID, TorrentConnId> compIdToLeecherId = new HashMap<>();
        private final Map<TorrentConnId, Component> leechers = new HashMap<>();
        //<peerId, >
        private final Map<Identifier, List<TorrentConnMngr.NewPeerConnection>> pendingPeerConnection = new HashMap<>();
        //<connId, >
        private final Map<TorrentConnId, List<TorrentConnMngr.NewFileConnection>> pendingFileConnection = new HashMap<>();

        public void newPeerConnection(TorrentConnMngr.NewPeerConnection peerConnect) {
            LOG.info("{}get files - file:{} waiting connect to peer:{}", new Object[]{logPrefix, peerConnect.connId.fileId, peerConnect.peer});
            List<TorrentConnMngr.NewPeerConnection> pc = pendingPeerConnection.get(peerConnect.peer.getId());
            if (pc == null) {
                pc = new LinkedList<>();
                pendingPeerConnection.put(peerConnect.peer.getId(), pc);
                LOG.info("{}get files - connecting to peer:{}", logPrefix, peerConnect.peer);
                trigger(new Seeder.Connect(peerConnect.peer), connPort);
            }
            pc.add(peerConnect);
            LOG.info("{}get files - waiting to establish peer connection:{}", logPrefix, peerConnect.peer);
        }

        public void peerConnected(KAddress peer) {
            LOG.info("{}get files - connected to peer:{}", logPrefix, peer);
            connMngr.connected(peer);
            List<TorrentConnMngr.NewPeerConnection> pc = pendingPeerConnection.remove(peer.getId());
            if (pc == null) {
                throw new RuntimeException("ups");
            }
            for (TorrentConnMngr.NewPeerConnection conn : pc) {
                newFileConnection(conn.advance());
            }
        }

        public void newFileConnection(TorrentConnMngr.NewFileConnection conn) {
            List<TorrentConnMngr.NewFileConnection> fc = pendingFileConnection.get(conn.connId);
            if (fc == null) {
                fc = new LinkedList<>();
                pendingFileConnection.put(conn.connId, fc);
                LOG.info("{}get files - establishing file connection:{}", logPrefix, conn.connId);
                trigger(conn.getMsg(), connPort);
            }
            fc.add(conn);
            LOG.info("{}get files - waiting to establish file connection:{}", logPrefix, conn.connId);
        }

        public void fileConnected(TorrentConnId connId, KAddress peer) {
            LOG.info("{}get files - established file connection:{}", logPrefix, connId);
            List<TorrentConnMngr.NewFileConnection> fcs = pendingFileConnection.remove(connId);
            if (fcs == null) {
                throw new RuntimeException("ups");
            }

            Component leecherComp = create(DwnlConnComp.class, new DwnlConnComp.Init(connId, selfAdr, peer, MyTorrent.defaultDataBlock, true));
            connect(leecherComp.getNegative(Timer.class), timerPort, Channel.TWO_WAY);
            transferNetworkChannel.addChannel(connId, leecherComp.getNegative(Network.class));
            dwnlConnChannel.addChannel(connId, leecherComp.getPositive(DwnlConnPort.class));
            trigger(Start.event, leecherComp.control());
            leechers.put(connId, leecherComp);
            compIdToLeecherId.put(leecherComp.id(), connId);

            connMngr.connectPeerFile(connId);
            for (TorrentConnMngr.NewFileConnection fc : fcs) {
                useFileConnection(fc.advance());
            }
        }

        public void useFileConnection(TorrentConnMngr.UseFileConnection conn) {
            LOG.debug("{}get files - conn:{} block:{}", new Object[]{logPrefix, conn.connId, conn.blockNr});
            connMngr.useSlot(conn);
            trigger(conn.getMsg(), dwnlConnPort);
        }

        public void killInstances(int fileId, Set<Identifier> peerIds) {
            for (Identifier peerId : peerIds) {
                TorrentConnId connId = new TorrentConnId(peerId, new FileIdentifier(torrentId, fileId), true);
                killInstance(connId);
            }
        }

        private void killInstance(TorrentConnId connId) {
            Component leecherComp = leechers.get(connId);
            if (leecherComp == null) {
                throw new RuntimeException("ups");
            }
            LOG.info("{}get files - killing leecher connection:{}", logPrefix, connId);
            trigger(Kill.event, leecherComp.control());
        }

        public void handleKilled(Killed event) {
            TorrentConnId connId = compIdToLeecherId.remove(event.component.id());
            if (connId == null) {
                return;
            }
            Component leecherComp = leechers.remove(connId);
            if (leecherComp == null) {
                throw new RuntimeException("ups");
            }
            LOG.info("{}get files - killed leecher connection:{}", logPrefix, connId);

            transferNetworkChannel.removeChannel(connId, leecherComp.getNegative(Network.class));
            dwnlConnChannel.removeChannel(connId, leecherComp.getPositive(DwnlConnPort.class));
        }
    }

    public class ServeFilesState {

        private final Map<UUID, TorrentConnId> compIdToSeederId = new HashMap<>();
        private final Map<TorrentConnId, Component> seeders = new HashMap<>();

        public void startInstance(TorrentConnId connId, KAddress peer, BlockDetails defaultBlock) {
            LOG.info("{}get files - start seeder connection:{}", logPrefix, connId);
            Component seederComp = create(UpldConnComp.class, new UpldConnComp.Init(connId, selfAdr, defaultBlock, true));
            connect(seederComp.getNegative(Timer.class), timerPort, Channel.TWO_WAY);
            transferNetworkChannel.addChannel(connId, seederComp.getNegative(Network.class));
            upldConnChannel.addChannel(connId, seederComp.getPositive(UpldConnPort.class));
            trigger(Start.event, seederComp.control());

            seeders.put(connId, seederComp);
            compIdToSeederId.put(seederComp.id(), connId);
        }

        private void killInstance(TorrentConnId connId) {
            Component seederComp = seeders.get(connId);
            if (seederComp == null) {
                throw new RuntimeException("ups");
            }
            LOG.info("{}get files - killing seeder connection:{}", logPrefix, connId);
            trigger(Kill.event, seederComp.control());
        }

        public void handleKilled(Killed event) {
            TorrentConnId connId = compIdToSeederId.remove(event.component.id());
            if (connId == null) {
                return;
            }
            Component seederComp = seeders.remove(connId);
            if (seederComp == null) {
                throw new RuntimeException("ups");
            }
            LOG.info("{}get files - killed seeder connection:{}", logPrefix, connId);

            transferNetworkChannel.removeChannel(connId, seederComp.getNegative(Network.class));
            upldConnChannel.removeChannel(connId, seederComp.getPositive(UpldConnPort.class));
        }
    }

    public static class Init extends se.sics.kompics.Init<TorrentComp> {

        public final KAddress selfAdr;
        public final Identifier torrentId;
        public final StorageProvider storageProvider;
        public List<KAddress> partners;
        public final boolean upload;
        public final Optional<MyTorrent> torrentDef;

        public Init(KAddress selfAdr, Identifier torrentId, StorageProvider storageProvider, List<KAddress> partners,
                boolean upload, Optional<MyTorrent> torrentDef) {
            this.selfAdr = selfAdr;
            this.torrentId = torrentId;
            this.storageProvider = storageProvider;
            this.upload = upload;
            this.torrentDef = torrentDef;
            this.partners = partners;
        }
    }

    private void scheduleAdvance() {
        SchedulePeriodicTimeout spt = new SchedulePeriodicTimeout(ADVANCE_PERIOD, ADVANCE_PERIOD);
        AdvanceTimeout tt = new AdvanceTimeout(spt);
        spt.setTimeoutEvent(tt);
        advanceTid = tt.getTimeoutId();
        trigger(spt, timerPort);
    }

    private void cancelAdvance() {
        CancelPeriodicTimeout cpd = new CancelPeriodicTimeout(advanceTid);
        trigger(cpd, timerPort);
        advanceTid = null;
    }

    public static class AdvanceTimeout extends Timeout {

        public AdvanceTimeout(SchedulePeriodicTimeout spt) {
            super(spt);
        }
    }
}
