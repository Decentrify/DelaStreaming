/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package se.sics.gvod.stream.system.hops;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import org.javatuples.Pair;
import org.javatuples.Triplet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.caracaldb.MessageRegistrator;
import se.sics.gvod.core.util.TorrentDetails;
import se.sics.gvod.network.GVoDSerializerSetup;
import se.sics.gvod.stream.StreamHostComp;
import se.sics.gvod.stream.report.ReportPort;
import se.sics.gvod.stream.report.SummaryEvent;
import se.sics.kompics.Channel;
import se.sics.kompics.Component;
import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.Handler;
import se.sics.kompics.Init;
import se.sics.kompics.Positive;
import se.sics.kompics.Start;
import se.sics.kompics.network.Network;
import se.sics.kompics.network.netty.NettyInit;
import se.sics.kompics.network.netty.NettyNetwork;
import se.sics.kompics.timer.Timer;
import se.sics.kompics.timer.java.JavaTimer;
import se.sics.ktoolbox.hops.managedStore.storage.HopsFactory;
import se.sics.ktoolbox.util.identifiable.Identifier;
import se.sics.ktoolbox.util.managedStore.core.FileMngr;
import se.sics.ktoolbox.util.managedStore.core.HashMngr;
import se.sics.ktoolbox.util.managedStore.core.TransferMngr;
import se.sics.ktoolbox.util.managedStore.core.impl.LBAOTransferMngr;
import se.sics.ktoolbox.util.managedStore.core.impl.SimpleTransferMngr;
import se.sics.ktoolbox.util.managedStore.core.util.HashUtil;
import se.sics.ktoolbox.util.managedStore.core.util.Torrent;
import se.sics.ktoolbox.util.network.KAddress;
import se.sics.ktoolbox.util.setup.BasicSerializerSetup;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class Receiver extends ComponentDefinition {

    private static final Logger LOG = LoggerFactory.getLogger(Receiver.class);
    private String logPrefix = "";

    private Component networkComp;
    private Component timerComp;
    private Component streamComp;

    Positive<ReportPort> reportPort = requires(ReportPort.class);

    private final ExperimentKCWrapper experimentConfig;

    private final KAddress selfAdr;

    public Receiver() {
        experimentConfig = new ExperimentKCWrapper(config());
        selfAdr = experimentConfig.receiverAdr;
        LOG.debug("{}starting...", logPrefix);

        registerSerializers();
        registerPortTracking();

        subscribe(handleStart, control);
        subscribe(handleSummary, reportPort);
    }

    private void registerSerializers() {
        MessageRegistrator.register();
        int currentId = 151;
        currentId = BasicSerializerSetup.registerBasicSerializers(currentId);
        currentId = GVoDSerializerSetup.registerSerializers(currentId);
    }

    private void registerPortTracking() {
    }

    Handler handleStart = new Handler<Start>() {
        @Override
        public void handle(Start event) {
            LOG.info("{}starting...", logPrefix);
            timerComp = create(JavaTimer.class, Init.NONE);
            networkComp = create(NettyNetwork.class, new NettyInit(selfAdr));
            trigger(Start.event, timerComp.control());
            trigger(Start.event, networkComp.control());

            connect();
            trigger(Start.event, streamComp.control());
        }
    };

    Handler handleSummary = new Handler<SummaryEvent>() {
        @Override
        public void handle(SummaryEvent event) {
            LOG.info("{}transfer of:{} completed in:{}", new Object[]{logPrefix, event.transferSize, event.transferTime});
        }
    };

    private void disconnect() {
        disconnect(streamComp.getPositive(ReportPort.class), reportPort.getPair());
    }

    private void connect() {

        StreamHostComp.ExtPort extPorts = new StreamHostComp.ExtPort(timerComp.getPositive(Timer.class), networkComp.getPositive(Network.class));
        TorrentDetails torrentDetails = new TorrentDetails() {
            private final Identifier overlayId = experimentConfig.torrentId;

            @Override
            public Identifier getOverlayId() {
                return overlayId;
            }

            @Override
            public boolean download() {
                return true;
            }

            @Override
            public Torrent getTorrent() {
                throw new RuntimeException("logic error");
            }

            @Override
            public Triplet<FileMngr, HashMngr, TransferMngr> torrentMngrs(Torrent torrent) {
                String hopsURL = experimentConfig.hopsURL;
                String filePath = experimentConfig.hopsOutDir + File.separator + experimentConfig.torrentName;
                long fileSize = torrent.fileInfo.size;
                String hashAlg = torrent.torrentInfo.hashAlg;
                int pieceSize = torrent.torrentInfo.pieceSize;
                int blockSize = torrent.torrentInfo.piecesPerBlock * pieceSize;
                int hashSize = HashUtil.getHashSize(torrent.torrentInfo.hashAlg);

                Pair<FileMngr, HashMngr> fileHashMngr = HopsFactory.getIncomplete(hopsURL, filePath, fileSize, hashAlg, blockSize, pieceSize);
                return fileHashMngr.add((TransferMngr)new LBAOTransferMngr(torrent, fileHashMngr.getValue1(), fileHashMngr.getValue0(), 10));
            }
        };
        List<KAddress> partners = new ArrayList<KAddress>();
        partners.add(experimentConfig.senderAdr);
        streamComp = create(StreamHostComp.class, new StreamHostComp.Init(extPorts, selfAdr, torrentDetails, partners));
        connect(streamComp.getPositive(ReportPort.class), reportPort.getPair(), Channel.TWO_WAY);
    }
}
