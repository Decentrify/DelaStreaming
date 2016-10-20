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
package se.sics.gvod.stream.system;

import java.io.File;
import java.io.IOException;
import org.javatuples.Triplet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.caracaldb.MessageRegistrator;
import se.sics.gvod.core.aggregation.VodCoreAggregation;
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
import se.sics.ktoolbox.util.aggregation.BasicAggregation;
import se.sics.ktoolbox.util.config.impl.SystemKCWrapper;
import se.sics.ktoolbox.util.identifiable.Identifier;
import se.sics.ktoolbox.util.managedStore.core.FileMngr;
import se.sics.ktoolbox.util.managedStore.core.HashMngr;
import se.sics.ktoolbox.util.managedStore.core.TransferMngr;
import se.sics.ktoolbox.util.managedStore.core.impl.StorageMngrFactory;
import se.sics.ktoolbox.util.managedStore.core.impl.SimpleTransferMngr;
import se.sics.ktoolbox.util.managedStore.core.util.FileInfo;
import se.sics.ktoolbox.util.managedStore.core.util.HashUtil;
import se.sics.ktoolbox.util.managedStore.core.util.Torrent;
import se.sics.ktoolbox.util.managedStore.core.util.TorrentInfo;
import se.sics.ktoolbox.util.setup.BasicSerializerSetup;

/**
 * @author Alex Ormenisan <aaor@sics.se>
 */
public class StreamHostLauncher extends ComponentDefinition {

    private static final Logger LOG = LoggerFactory.getLogger(StreamHostLauncher.class);
    private String logPrefix = "";

    private Component networkComp;
    private Component timerComp;
    private Component streamComp;

    private final SystemKCWrapper systemConfig;
    private final StreamHostKCWrapper hostConfig;

    public StreamHostLauncher() {
        systemConfig = new SystemKCWrapper(config());
        hostConfig = new StreamHostKCWrapper(config());
        LOG.debug("{}starting...", logPrefix);

        registerSerializers();
        registerPortTracking();
        subscribe(handleStart, control);
    }

    private void registerSerializers() {
        MessageRegistrator.register();
        int currentId = 128;
        currentId = BasicSerializerSetup.registerBasicSerializers(currentId);
        currentId = GVoDSerializerSetup.registerSerializers(currentId);
    }

    private void registerPortTracking() {
        BasicAggregation.registerPorts();
        VodCoreAggregation.registerPorts();
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
        networkComp = create(NettyNetwork.class, new NettyInit(hostConfig.selfAdr));
        
        StreamHostComp.ExtPort extPorts = new StreamHostComp.ExtPort(timerComp.getPositive(Timer.class), networkComp.getPositive(Network.class));
        TorrentDetails torrentDetails;
        if (!hostConfig.download) {
            torrentDetails = new TorrentDetails() {
                private final boolean download = hostConfig.download;
                private final String filePath = hostConfig.filePath;
                private final String hashPath = hostConfig.hashPath;
                private final Torrent torrent;
                private final Triplet<FileMngr, HashMngr, TransferMngr> mngrs;

                {
                    try {
                        File dataFile = new File(filePath);
                        if(!dataFile.exists()) {
                            throw new RuntimeException("missing data file");
                        }
                        long fileSize = dataFile.length();
                        String torrentName = dataFile.getName();
                        int pieceSize = 1024;
                        int piecesPerBlock = 1024;
                        int blockSize = pieceSize * piecesPerBlock;

                        File hashFile = new File(hashPath);
                        if(!hashFile.exists()) {
                            throw new RuntimeException("missing hash file");
                        }
                        String hashAlg = HashUtil.getAlgName(HashUtil.SHA);
                        int hashSize = HashUtil.getHashSize(hashAlg);
                        long hashFileSize = hashFile.length();
                        
                        torrent = new Torrent(hostConfig.torrentId, FileInfo.newFile(torrentName, fileSize), new TorrentInfo(1024, 1024, hashAlg, hashFileSize));
                        FileMngr fileMngr = StorageMngrFactory.completeMMFileMngr(filePath, fileSize, blockSize, pieceSize);
                        HashMngr hashMngr = StorageMngrFactory.completeMMHashMngr(hashPath, hashAlg, hashFileSize, hashSize);
                        mngrs = Triplet.with(fileMngr, hashMngr, null);
                    } catch (IOException ex) {
                        throw new RuntimeException(ex);
                    }
                }

                @Override
                public Identifier getOverlayId() {
                    return torrent.overlayId;
                }

                @Override
                public boolean download() {
                    return download;
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
        } else {
            torrentDetails = new TorrentDetails() {
                private final Identifier overlayId = hostConfig.torrentId;
                private final boolean download = hostConfig.download;
                private final String resourcePath = hostConfig.filePath;

                @Override
                public Identifier getOverlayId() {
                    return overlayId;
                }

                @Override
                public boolean download() {
                    return download;
                }

                @Override
                public Torrent getTorrent() {
                    throw new RuntimeException("logic error");
                }

                @Override
                public Triplet<FileMngr, HashMngr, TransferMngr> torrentMngrs(Torrent torrent) {
                    try {
                        int blockSize = torrent.torrentInfo.piecesPerBlock * torrent.torrentInfo.pieceSize;
                        int hashSize = HashUtil.getHashSize(torrent.torrentInfo.hashAlg);

                        FileMngr fileMngr = StorageMngrFactory.incompleteMMFileMngr(resourcePath, torrent.fileInfo.size, blockSize, torrent.torrentInfo.pieceSize);
                        HashMngr hashMngr = StorageMngrFactory.incompleteMMHashMngr(resourcePath + ".hash", torrent.torrentInfo.hashAlg, torrent.torrentInfo.hashFileSize, hashSize);
                        return Triplet.with(fileMngr, hashMngr, (TransferMngr)new SimpleTransferMngr(torrent, hashMngr, fileMngr));
                    } catch (IOException ex) {
                        throw new RuntimeException(ex);
                    }
                }
            };
        }
        streamComp = create(StreamHostComp.class, new StreamHostComp.Init(extPorts, hostConfig.selfAdr, torrentDetails, hostConfig.partners));
    }
}
