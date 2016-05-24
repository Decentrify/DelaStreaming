/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package se.sics.gvod.stream.system.hops;

import java.io.File;
import java.util.ArrayList;
import org.javatuples.Pair;
import org.javatuples.Triplet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.caracaldb.Address;
import se.sics.caracaldb.MessageRegistrator;
import se.sics.gvod.core.util.TorrentDetails;
import se.sics.gvod.network.GVoDSerializerSetup;
import se.sics.gvod.stream.StreamHostComp;
import se.sics.kompics.Component;
import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.Handler;
import se.sics.kompics.Init;
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
import se.sics.ktoolbox.util.managedStore.core.impl.TransferMngr;
import se.sics.ktoolbox.util.managedStore.core.util.FileInfo;
import se.sics.ktoolbox.util.managedStore.core.util.HashUtil;
import se.sics.ktoolbox.util.managedStore.core.util.Torrent;
import se.sics.ktoolbox.util.managedStore.core.util.TorrentInfo;
import se.sics.ktoolbox.util.network.KAddress;
import se.sics.ktoolbox.util.setup.BasicSerializerSetup;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class Sender extends ComponentDefinition {

    private static final Logger LOG = LoggerFactory.getLogger(Sender.class);
    private String logPrefix = "";

    private Component networkComp;
    private Component timerComp;
    private Component streamComp;

    private final ExperimentKCWrapper experimentConfig;

    private final KAddress selfAdr;

    public Sender() {
        experimentConfig = new ExperimentKCWrapper(config());
        selfAdr = experimentConfig.senderAdr;
        LOG.debug("{}starting...", logPrefix);

        registerSerializers();
        registerPortTracking();
        subscribe(handleStart, control);
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
            connect();
            trigger(Start.event, timerComp.control());
            trigger(Start.event, networkComp.control());
            trigger(Start.event, streamComp.control());
        }
    };

    private void connect() {
        timerComp = create(JavaTimer.class, Init.NONE);
        networkComp = create(NettyNetwork.class, new NettyInit(selfAdr));

        StreamHostComp.ExtPort extPorts = new StreamHostComp.ExtPort(timerComp.getPositive(Timer.class), networkComp.getPositive(Network.class));
        TorrentDetails torrentDetails;
        torrentDetails = new TorrentDetails() {
            private final String torrentName = experimentConfig.torrentName;
            private final String filePath = experimentConfig.hopsInDir + File.separator + experimentConfig.torrentName;
            private final Torrent torrent;
            private final Triplet<FileMngr, HashMngr, TransferMngr> mngrs;

            {
                String hopsURL = experimentConfig.hopsURL;
                int pieceSize = 1024;
                int piecesPerBlock = 1024;
                int blockSize = pieceSize * piecesPerBlock;

                String hashAlg = HashUtil.getAlgName(HashUtil.SHA);

                Pair<FileMngr, HashMngr> fileHashMngr = HopsFactory.getComplete(hopsURL, filePath, hashAlg, blockSize, pieceSize);
                mngrs = fileHashMngr.add((TransferMngr) null);
                long fileSize = mngrs.getValue0().length();
                torrent = new Torrent(experimentConfig.torrentId, FileInfo.newFile(torrentName, fileSize), new TorrentInfo(pieceSize, piecesPerBlock, hashAlg, -1));
            }

            @Override
            public Identifier getOverlayId() {
                return torrent.overlayId;
            }

            @Override
            public boolean download() {
                return false;
            }

            @Override
            public Torrent getTorrent() {
                return torrent;
            }

            @Override
            public Triplet<FileMngr, HashMngr, TransferMngr> torrentMngrs(Torrent torrent) {
                return mngrs;
            }
        };
        streamComp = create(StreamHostComp.class, new StreamHostComp.Init(extPorts, selfAdr, torrentDetails, new ArrayList<KAddress>()));
    }
}
