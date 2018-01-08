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
package se.sics.nstream.torrent.transfer;

import com.google.common.base.Optional;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;
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
import se.sics.kompics.Positive;
import se.sics.kompics.Start;
import se.sics.kompics.network.Network;
import se.sics.kompics.timer.CancelPeriodicTimeout;
import se.sics.kompics.timer.SchedulePeriodicTimeout;
import se.sics.kompics.timer.Timeout;
import se.sics.kompics.timer.Timer;
import se.sics.kompics.util.Identifier;
import se.sics.ktoolbox.util.identifiable.overlay.OverlayId;
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
import se.sics.nstream.ConnId;
import se.sics.nstream.FileId;
import se.sics.nstream.StreamId;
import se.sics.nstream.TorrentIds;
import se.sics.nstream.storage.durable.DStoragePort;
import se.sics.nstream.storage.durable.util.MyStream;
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
import se.sics.nstream.torrent.tracking.TorrentTrackingPort;
import se.sics.nstream.torrent.tracking.event.TorrentTracking;
import se.sics.nstream.torrent.transfer.dwnl.event.CompletedBlocks;
import se.sics.nstream.torrent.transfer.dwnl.event.DownloadBlocks;
import se.sics.nstream.torrent.transfer.event.ctrl.GetRawTorrent;
import se.sics.nstream.torrent.transfer.event.ctrl.SetupTransfer;
import se.sics.nstream.torrent.transfer.tracking.TransferReportPort;
import se.sics.nstream.torrent.transfer.tracking.TransferTrackingComp;
import se.sics.nstream.torrent.transfer.tracking.TransferTrackingPort;
import se.sics.nstream.torrent.transfer.tracking.TransferTrackingReport;
import se.sics.nstream.torrent.transfer.upld.event.GetBlocks;
import se.sics.nstream.torrent.util.EventTorrentConnIdExtractor;
import se.sics.nstream.torrent.util.MsgTorrentConnIdExtractor;
import se.sics.nstream.transfer.MyTorrent;
import se.sics.nstream.transfer.MyTorrent.Manifest;
import se.sics.nstream.transfer.MyTorrent.ManifestDef;
import se.sics.nstream.util.BlockDetails;
import se.sics.nstream.util.actuator.ComponentLoadTracking;
import se.sics.nstream.util.result.HashReadCallback;
import se.sics.nstream.util.result.ReadCallback;
import se.sics.nutil.tracking.load.QueueLoadConfig;
import se.sics.silkold.resourcemngr.PrepareResources;
import se.sics.silkold.resourcemngr.ResourceMngrPort;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class TransferComp extends ComponentDefinition {

    private static final Logger LOG = LoggerFactory.getLogger(TransferComp.class);
    private String logPrefix;

    private static final int DEF_FILE_NR = 0;
    private static final long ADVANCE_PERIOD = 1000;

    private final TorrentConfig torrentConfig;
    private final OverlayId torrentId;
    private final KAddress selfAdr;
    //********************************CONNECT_TO********************************
    Positive<Network> networkPort = requires(Network.class);
    Positive<Timer> timerPort = requires(Timer.class);
    Positive<ResourceMngrPort> resourceMngrPort = requires(ResourceMngrPort.class);
    Negative<TransferCtrlPort> transferPort = provides(TransferCtrlPort.class);
    Positive<DStoragePort> storagePort = requires(DStoragePort.class);
    Negative<TorrentTrackingPort> statusPort = provides(TorrentTrackingPort.class);
    //*********************************INTERNAL*********************************
    //transfer ports
    Positive<TransferReportPort> transferReportPort = requires(TransferReportPort.class);
    Positive<DwnlConnPort> dwnlConnPort = requires(DwnlConnPort.class);
    Positive<UpldConnPort> upldConnPort = requires(UpldConnPort.class);
    //conn ports
    Positive<ConnectionPort> connPort = requires(ConnectionPort.class);
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
    private Component transferTrackingComp;
    //**************************************************************************
    private UUID advanceTid;
     //**************************************************************************
    private GetRawTorrent.Request rawTorrentReq;
    private SetupTransfer.Request setupTransferReq;

    public TransferComp(Init init) {
        torrentId = init.torrentId;
        selfAdr = init.selfAdr;
        logPrefix = "<nid:" + selfAdr.getId() + ",oid:" + torrentId + ">";
        torrentConfig = new TorrentConfig();

        componentTracking = new ComponentLoadTracking("torrent", this.proxy, new QueueLoadConfig(config()));
        buildChannels();
        connMngr = new TorrentConnMngr(componentTracking, init.partners);

        subscribe(handleStart, control);
        subscribe(handleKilled, control);

        subscribe(handleGetRawTorrent, transferPort);
        subscribe(handleSetupTransfer, transferPort);
        subscribe(handleAdvance, timerPort);
        subscribe(handleResourcesPrepared, resourceMngrPort);
        subscribe(handleTransferReport, transferReportPort);

        subscribe(handleSeederConnectSuccess, connPort);
        subscribe(handleSeederConnectTimeout, connPort);
        subscribe(handleSeederConnectSuspect, connPort);
        subscribe(handleDetailedState, connPort);
        subscribe(handleOpenTransferLeecherResp, connPort);
        subscribe(handleOpenTransferLeecherTimeout, connPort);
        subscribe(handleOpenTransferSeeder, connPort);
        subscribe(handleCloseTransfer, connPort);

        subscribe(handleGetBlocks, upldConnPort);
        subscribe(handleCompletedBlocks, dwnlConnPort);
    }

    private void buildChannels() {
        ChannelIdExtractor<?, Identifier> overlayIdExtractor = new MsgOverlayIdExtractor();
        connNetworkChannel = One2NChannel.getChannel("torrentConnNetwork", networkPort, overlayIdExtractor, TransferCompFilters.connInclusionFilter);
        ChannelIdExtractor<?, Identifier> connIdExtractor = new MsgTorrentConnIdExtractor(new SourceHostIdExtractor());
        transferNetworkChannel = One2NChannel.getChannel("torrentPeerFileNetwork", networkPort, connIdExtractor, TransferCompFilters.transferInclusionFilter);
        dwnlConnChannel = One2NChannel.getChannel("torrentDwnlConn", (Negative) dwnlConnPort.getPair(), new EventTorrentConnIdExtractor());
        upldConnChannel = One2NChannel.getChannel("torrentUpldConn", (Negative) upldConnPort.getPair(), new EventTorrentConnIdExtractor());
    }

    Handler handleStart = new Handler<Start>() {

        @Override
        public void handle(Start event) {
            LOG.info("{}starting", logPrefix);
            connState = new ConnectionState();
            connState.connectComp();
            connState.start();
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
    Handler handleGetRawTorrent = new Handler<GetRawTorrent.Request>() {
        @Override
        public void handle(GetRawTorrent.Request req) {
            rawTorrentReq = req;
            if (connMngr.hasConnCandidates()) {
                trigger(new Seeder.Connect(connMngr.getConnCandidate(), torrentId), connPort);
            } else {
                answer(req, req.success(Result.timeout(new IllegalArgumentException("no peers to download manifest def"))));
            }
        }
    };

    Handler handleSeederConnectSuccess = new Handler<Seeder.Success>() {
        @Override
        public void handle(Seeder.Success resp) {
            LOG.info("{}connect to:{} success", logPrefix, resp.target);
            connMngr.connected(resp.target);
            if (getFilesState != null) {
                getFilesState.peerConnected(resp.target);
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

    Handler handleDetailedState = new Handler<DetailedState.Deliver>() {
        @Override
        public void handle(DetailedState.Deliver event) {
            if (!event.manifestDef.isSuccess()) {
                LOG.warn("{}manifest def - failed", logPrefix);
                answer(rawTorrentReq, rawTorrentReq.success(event.manifestDef));
                return;
            }
            LOG.info("{}detailed state - success", logPrefix);

            KAddress peer = connMngr.randomPeer();
            FileId fileId = TorrentIds.fileId(torrentId, DEF_FILE_NR);
            ConnId connId = TorrentIds.connId(fileId, peer.getId(), true);
            trigger(new OpenTransfer.LeecherRequest(peer, connId), connPort);
            getDefState = new GetDefinitionState(event.manifestDef.getValue());
        }
    };
    
    Handler handleSetupTransfer = new Handler<SetupTransfer.Request>() {
        @Override
        public void handle(SetupTransfer.Request req) {
            LOG.info("{}transfer - setting up", logPrefix);
            setupTransferReq = req;
            serveDefState = new ServeDefinitionState(req.torrent);
            if (getDefState == null) {
                trigger(new DetailedState.Set(req.torrent.manifest.getDef()), connPort);
            }
            trigger(new PrepareResources.Request(torrentId, serveDefState.td), resourceMngrPort);
        }
    };

    Handler handleResourcesPrepared = new Handler<PrepareResources.Success>() {
        @Override
        public void handle(PrepareResources.Success resp) {
            LOG.info("{}resources prepared", logPrefix);
            serveFilesState = new ServeFilesState();
            getFilesState = new GetFilesState();

            initializeTorrent(serveDefState.td, resp.streamsInfo);
            trigger(Start.event, transferTrackingComp.control());

            answer(setupTransferReq, setupTransferReq.success(Result.success(true)));
            trigger(new TorrentTracking.TransferSetUp(torrentId, fileMngr.report()), statusPort);
            scheduleAdvance();
            tryAdvance();
        }
    };

    private void initializeTorrent(MyTorrent torrent, Map<StreamId, Long> streamsInfo) {
        DelayedExceptionSyncHandler deh = new DelayedExceptionSyncHandler() {

            @Override
            public boolean fail(Result<Object> result) {
                throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
            }
        };
        fileMngr = TorrentFileMngr.create(config(), proxy, deh, componentTracking, torrent, streamsInfo);
        fileMngr.start();
        
        //transfer report
        transferTrackingComp = create(TransferTrackingComp.class, new TransferTrackingComp.Init(torrentId));
        connect(transferTrackingComp.getNegative(Timer.class), timerPort, Channel.TWO_WAY);
        connect(transferTrackingComp.getPositive(TransferReportPort.class), transferReportPort.getPair(), Channel.TWO_WAY);
    }

    //**************************************************************************
    Handler handleTransferReport = new Handler<TransferTrackingReport>() {
        @Override
        public void handle(TransferTrackingReport event) {
            LOG.info("{}transfer report", logPrefix);
            TorrentTracking.Indication report = new TorrentTracking.Indication(fileMngr.report(), event.downloadReport);
            trigger(report, statusPort);
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
            trigger(new TorrentTracking.DownloadDone(torrentId, fileMngr.report()), statusPort);
        }

        FileId fileId = connMngr.canAdvanceFile();
        if (fileId != null) {
            TFileWrite fileWriter = fileMngr.writeTo(fileId);
            if (fileWriter.isComplete()) {
                LOG.info("{}file:{} completed", new Object[]{logPrefix, fileId});
                fileMngr.complete(fileId);
                Set<Identifier> closeConn = connMngr.closeFileConnection(fileId);
                getFilesState.killInstances(fileId, closeConn);
                return;
            }
            int batchSize = 2;
            Pair<Integer, Optional<BlockDetails>> block = nextBlock(fileWriter);
            while (block != null && batchSize > 0) {
                batchSize--;
                if (!advanceConn(fileId, block.getValue0(), block.getValue1())) {
                    break;
                }
                block = nextBlock(fileWriter);
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
                Pair<FileId, Map<StreamId, MyStream>> resource = fileMngr.nextPending();
                fileId = resource.getValue0();
                LOG.debug("{}advance new file:{}", logPrefix, fileId);
                connMngr.newFileConnection(fileId);
                return;
            }
        }
        LOG.info("{}nothing", logPrefix);
    }

    private Pair<Integer, Optional<BlockDetails>> nextBlock(TFileWrite fileWriter) {
        fileWriter.hasHashes();
        if (fileWriter.hasBlocks()) {
            //for the moment we get hashes with blocks - same management
            return fileWriter.requestBlock();
        }
        return null;
    }

    private boolean advanceConn(FileId fileId, int blockNr, Optional<BlockDetails> irregularBlock) {
        ConnResult result = connMngr.attemptSlot(fileId, blockNr, irregularBlock);
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
    Handler handleOpenTransferLeecherResp = new Handler<OpenTransfer.LeecherResponse>() {
        @Override
        public void handle(OpenTransfer.LeecherResponse resp) {
            LOG.info("{}open transfer - leecher response", logPrefix);
            if (resp.connId.fileId.fileNr == DEF_FILE_NR) {
                //def phase
                getDefState.startInstance(connMngr.randomPeer());
            } else {
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
            if (req.connId.fileId.fileNr == DEF_FILE_NR) {
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
                serveDefState.killInstance(event.connId.peerId);
            }
        }
    };

    //**************************************************************************
    Handler handleGetBlocks = new Handler<GetBlocks.Request>() {
        @Override
        public void handle(GetBlocks.Request req) {
            LOG.debug("{}conn:{} req blocks:{}", new Object[]{logPrefix, req.connId, req.blocks});
            if (req.connId.fileId.fileNr == DEF_FILE_NR) {
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
            final TFileRead fileReader = fileMngr.readFrom(req.connId.fileId);
            fileReader.setCacheHint(req.connId.peerId, req.cacheHint);
            for (final Integer blockNr : req.blocks) {
                fileReader.readBlock(blockNr, new ReadCallback() {

                    @Override
                    public boolean fail(Result<KReference<byte[]>> result) {
                        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
                    }

                    @Override
                    public boolean success(Result<KReference<byte[]>> result) {
                        //retain block - to be relased in requester(UpldConnComp), when it is done with it
                        result.getValue().retain();
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
            if (event.connId.fileId.fileNr == DEF_FILE_NR) {
                if (getDefState == null) {
                    throw new RuntimeException("ups");
                }
                getDefState.handleBlockCompleted(event);
            } else {
                writeToFile(event.connId.fileId, event.blocks, event.hashes);
                updateConn(event.connId, event.blocks);
                tryAdvance();
            }
        }
    };

    private void writeToFile(FileId fileId, Map<Integer, byte[]> blocks, Map<Integer, byte[]> hashes) {
        TFileWrite fileWriter = fileMngr.writeTo(fileId);
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

    private void updateConn(ConnId connId, Map<Integer, byte[]> blocks) {
        for (Integer blockNr : blocks.keySet()) {
            connMngr.releaseSlot(connId, blockNr);

        }
    }

    //*********************************STATES***********************************
    public class ConnectionState {

        private Component connComp;

        public void connectComp() {
            connComp = create(ConnectionComp.class, new ConnectionComp.Init(torrentId, selfAdr));
            connect(connComp.getNegative(Timer.class), timerPort, Channel.TWO_WAY);
            connNetworkChannel.addChannel(torrentId, connComp.getNegative(Network.class));
            connect(connComp.getPositive(ConnectionPort.class), connPort.getPair(), Channel.TWO_WAY);
        }

        public void start() {
            trigger(Start.event, connComp.control());

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
        private ConnId connId;

        public GetDefinitionState(ManifestDef manifestDef) {
            this.tdBuilder = new MyTorrent.Builder(manifestDef);
        }

        public void startInstance(KAddress peer) {
            FileId fileId = TorrentIds.fileId(torrentId, DEF_FILE_NR);
            connId = TorrentIds.connId(fileId, peer.getId(), true);
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
            trigger(new TorrentTracking.DownloadedManifest(torrentId, Result.success(manifest)), statusPort);
            answer(rawTorrentReq, rawTorrentReq.success(Result.success(manifest)));
            trigger(new CloseTransfer.Request(connId), connPort);
            killInstance();
        }
    }

    public class ServeDefinitionState {

        private final MyTorrent td;
        //<peerId, compId, comp>
        private final Table<Identifier, UUID, Pair<Component, ConnId>> seederComponents = HashBasedTable.create();

        public ServeDefinitionState(MyTorrent td) {
            this.td = td;
        }

        public void startInstance(KAddress peer) {
            FileId fileId = TorrentIds.fileId(torrentId, DEF_FILE_NR);
            ConnId connId = TorrentIds.connId(fileId, peer.getId(), false);
            LOG.info("{}serve definition - start connection:{}", logPrefix, connId);

            Component transferComp = create(UpldConnComp.class, new UpldConnComp.Init(connId, selfAdr, td.manifest.defaultBlock, false));
            connect(transferComp.getNegative(Timer.class), timerPort, Channel.TWO_WAY);
            transferNetworkChannel.addChannel(connId, transferComp.getNegative(Network.class));
            upldConnChannel.addChannel(connId, transferComp.getPositive(UpldConnPort.class));

            seederComponents.put(peer.getId(), transferComp.id(), Pair.with(transferComp, connId));
            trigger(Start.event, transferComp.control());
        }

        public void killInstance(Identifier peerId) {
            Map<UUID, Pair<Component, ConnId>> aux = seederComponents.row(peerId);
            if (aux.isEmpty()) {
                return;
            }
            if (aux.size() > 1) {
                throw new RuntimeException("ups");
            }
            Pair<Component, ConnId> aux2 = aux.entrySet().iterator().next().getValue();
            ConnId connId = aux2.getValue1();
            Component transferComp = aux2.getValue0();
            LOG.info("{}serve definition - killing connection:{}", logPrefix, connId);
            trigger(Kill.event, transferComp.control());
        }

        public void handleKilled(Killed event) {
            Map<Identifier, Pair<Component, ConnId>> aux = seederComponents.column(event.component.id());
            if (aux.isEmpty()) {
                return;
            }
            if (aux.size() > 1) {
                throw new RuntimeException("ups");
            }
            Map.Entry<Identifier, Pair<Component, ConnId>> aux2 = aux.entrySet().iterator().next();
            Identifier peerId = aux2.getKey();
            ConnId connId = aux2.getValue().getValue1();
            Component transferComp = aux2.getValue().getValue0();
            LOG.info("{}serve definition - killed connection:{}", logPrefix, connId);

            transferNetworkChannel.removeChannel(connId, transferComp.getNegative(Network.class));
            upldConnChannel.removeChannel(connId, transferComp.getPositive(UpldConnPort.class));
            seederComponents.remove(peerId, event.component.id());
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

        private final Map<UUID, ConnId> compIdToLeecherId = new HashMap<>();
        private final Map<ConnId, Component> leechers = new HashMap<>();
        //<peerId, >
        private final Map<Identifier, List<TorrentConnMngr.NewPeerConnection>> pendingPeerConnection = new HashMap<>();
        //<connId, >
        private final Map<ConnId, List<TorrentConnMngr.NewFileConnection>> pendingFileConnection = new HashMap<>();

        public void newPeerConnection(TorrentConnMngr.NewPeerConnection peerConnect) {
            LOG.info("{}get files - file:{} waiting connect to peer:{}", new Object[]{logPrefix, peerConnect.connId.fileId, peerConnect.peer});
            List<TorrentConnMngr.NewPeerConnection> pc = pendingPeerConnection.get(peerConnect.peer.getId());
            if (pc == null) {
                pc = new LinkedList<>();
                pendingPeerConnection.put(peerConnect.peer.getId(), pc);
                LOG.info("{}get files - connecting to peer:{}", logPrefix, peerConnect.peer);
                trigger(new Seeder.Connect(peerConnect.peer, torrentId), connPort);
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

        public void fileConnected(ConnId connId, KAddress peer) {
            LOG.info("{}get files - established file connection:{}", logPrefix, connId);
            List<TorrentConnMngr.NewFileConnection> fcs = pendingFileConnection.remove(connId);
            if (fcs == null) {
                throw new RuntimeException("ups");
            }

            Component leecherComp = create(DwnlConnComp.class, new DwnlConnComp.Init(connId, selfAdr, peer, MyTorrent.defaultDataBlock, true));
            connect(leecherComp.getNegative(Timer.class), timerPort, Channel.TWO_WAY);
            connect(leecherComp.getPositive(TransferTrackingPort.class), transferTrackingComp.getNegative(TransferTrackingPort.class), Channel.TWO_WAY);
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

        public void killInstances(FileId fileId, Set<Identifier> peerIds) {
            for (Identifier peerId : peerIds) {
                ConnId connId = TorrentIds.connId(fileId, peerId, true);
                killInstance(connId);
            }
        }

        private void killInstance(ConnId connId) {
            Component leecherComp = leechers.get(connId);
            if (leecherComp == null) {
                throw new RuntimeException("ups");
            }
            LOG.info("{}get files - killing leecher connection:{}", logPrefix, connId);
            trigger(Kill.event, leecherComp.control());
        }

        public void handleKilled(Killed event) {
            ConnId connId = compIdToLeecherId.remove(event.component.id());
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

        private final Map<UUID, ConnId> compIdToSeederId = new HashMap<>();
        private final Map<ConnId, Component> seeders = new HashMap<>();

        public void startInstance(ConnId connId, KAddress peer, BlockDetails defaultBlock) {
            LOG.info("{}get files - start seeder connection:{}", logPrefix, connId);
            Component seederComp = create(UpldConnComp.class, new UpldConnComp.Init(connId, selfAdr, defaultBlock, true));
            connect(seederComp.getNegative(Timer.class), timerPort, Channel.TWO_WAY);
            transferNetworkChannel.addChannel(connId, seederComp.getNegative(Network.class));
            upldConnChannel.addChannel(connId, seederComp.getPositive(UpldConnPort.class));
            trigger(Start.event, seederComp.control());

            seeders.put(connId, seederComp);
            compIdToSeederId.put(seederComp.id(), connId);
        }

        private void killInstance(ConnId connId) {
            Component seederComp = seeders.get(connId);
            if (seederComp == null) {
                throw new RuntimeException("ups");
            }
            LOG.info("{}get files - killing seeder connection:{}", logPrefix, connId);
            trigger(Kill.event, seederComp.control());
        }

        public void handleKilled(Killed event) {
            ConnId connId = compIdToSeederId.remove(event.component.id());
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

    public static class Init extends se.sics.kompics.Init<TransferComp> {

        public final KAddress selfAdr;
        public final OverlayId torrentId;
        public List<KAddress> partners;

        public Init(KAddress selfAdr, OverlayId torrentId, List<KAddress> partners) {
            this.selfAdr = selfAdr;
            this.torrentId = torrentId;
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
