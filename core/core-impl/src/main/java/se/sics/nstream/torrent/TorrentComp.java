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
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.javatuples.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.kompics.ClassMatchedHandler;
import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.Handler;
import se.sics.kompics.Negative;
import se.sics.kompics.PortType;
import se.sics.kompics.Positive;
import se.sics.kompics.Start;
import se.sics.kompics.network.MessageNotify;
import se.sics.kompics.network.Network;
import se.sics.kompics.timer.CancelPeriodicTimeout;
import se.sics.kompics.timer.SchedulePeriodicTimeout;
import se.sics.kompics.timer.ScheduleTimeout;
import se.sics.kompics.timer.Timeout;
import se.sics.kompics.timer.Timer;
import se.sics.ktoolbox.util.identifiable.Identifier;
import se.sics.ktoolbox.util.identifiable.basic.UUIDIdentifier;
import se.sics.ktoolbox.util.network.KAddress;
import se.sics.ktoolbox.util.network.KContentMsg;
import se.sics.ktoolbox.util.network.KHeader;
import se.sics.ktoolbox.util.tracking.load.QueueLoadConfig;
import se.sics.ledbat.core.LedbatConfig;
import se.sics.ledbat.ncore.msg.LedbatMsg;
import se.sics.nstream.StreamEvent;
import se.sics.nstream.report.TransferStatusPort;
import se.sics.nstream.report.event.ReportTimeout;
import se.sics.nstream.report.event.TransferStatus;
import se.sics.nstream.torrent.event.HashGet;
import se.sics.nstream.torrent.event.TorrentGet;
import se.sics.nstream.torrent.event.TorrentTimeout;
import se.sics.nstream.transfer.Transfer;
import se.sics.nstream.transfer.TransferMngrPort;
import se.sics.nstream.util.TransferDetails;
import se.sics.nstream.util.actuator.ComponentLoadTracking;

/**
 *
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class TorrentComp extends ComponentDefinition {

    private static final Logger LOG = LoggerFactory.getLogger(TorrentComp.class);
    private String logPrefix;

    //**************************************************************************
    Positive<TransferMngrPort> transferMngrPort = requires(TransferMngrPort.class);
    Positive<Network> networkPort = requires(Network.class);
    Positive<Timer> timerPort = requires(Timer.class);
    Negative<TransferStatusPort> transferStatusPort = provides(TransferStatusPort.class);
    List<Positive> requiredPorts = new ArrayList<>();
    //**************************************************************************
    private final TorrentConfig torrentConfig;
    //**************************************************************************
    private ComponentLoadTracking componentTracking;
    private TransferFSM transfer;
    private SimpleConnMngr router;
    //**************************************************************************
    private UUID periodicReport;
    private UUID loadCheck;

    public TorrentComp(Init init) {
        logPrefix = "<" + init.selfAdr.getId() + ">";
        torrentConfig = new TorrentConfig();
        router = new SimpleConnMngr(new ConnMngrConfig(config()), new LedbatConfig(config()), init.partners);
        componentTracking = new ComponentLoadTracking("torrent", this.proxy, new QueueLoadConfig(config()));
        storageProvider(init.storageProvider);
        transferState(init);

        subscribe(handleStart, control);
        subscribe(handleReportTimeout, timerPort);

        subscribe(handleTorrentReq, networkPort);
        subscribe(handleTorrentResp, networkPort);
        subscribe(handleTorrentTimeout, timerPort);
        subscribe(handleAdvanceDownload, timerPort);
        subscribe(handleHashTimeout, timerPort);
        subscribe(handlePieceTimeout, timerPort);
        subscribe(handleLedbatReq, networkPort);
        subscribe(handleLedbatResp, networkPort);

        subscribe(handleNettyCheck, networkPort);
        subscribe(handleExtendedTorrentResp, transferMngrPort);
        subscribe(handleTransferStatusReq, transferStatusPort);
    }

    private void transferState(Init init) {
        TransferFSM.CommonState cs = new TransferFSM.CommonState(config(), torrentConfig, this.proxy, init.selfAdr, init.overlayId, router, componentTracking);
        if (init.upload) {
            byte[] torrentByte = init.transferDetails.get().getValue0();
            TransferDetails transferDetails = init.transferDetails.get().getValue1();
            transfer = new TransferFSM.UploadState(cs, torrentByte, transferDetails);
        } else {
            transfer = new TransferFSM.InitState(cs);
        }
    }

    private void storageProvider(StorageProvider storageProvider) {
        for (Class<PortType> r : storageProvider.requiresPorts()) {
            requiredPorts.add(requires(r));
        }
    }

    Handler handleStart = new Handler<Start>() {

        @Override
        public void handle(Start event) {
            componentTracking.startTracking();
            transfer.start();
            scheduleReport();
        }
    };

    @Override
    public void tearDown() {
        CancelPeriodicTimeout ct = new CancelPeriodicTimeout(periodicReport);
        trigger(ct, timerPort);

        transfer.close();
    }

    Handler handleReportTimeout = new Handler<ReportTimeout>() {
        @Override
        public void handle(ReportTimeout event) {
            transfer.report();
        }
    };

    Handler handleTransferStatusReq = new Handler<TransferStatus.Request>() {
        @Override
        public void handle(TransferStatus.Request event) {
            transfer.handleTransferStatusReq(event);
        }
    };

    Handler handleNettyCheck = new Handler<MessageNotify.Resp>() {
        @Override
        public void handle(MessageNotify.Resp resp) {
            transfer.handleNotify(resp);
        }
    };

    ClassMatchedHandler handleTorrentReq
            = new ClassMatchedHandler<TorrentGet.Request, KContentMsg<KAddress, KHeader<KAddress>, TorrentGet.Request>>() {
                @Override
                public void handle(TorrentGet.Request content, KContentMsg<KAddress, KHeader<KAddress>, TorrentGet.Request> container) {
                    transfer.handleTorrentReq(container, content);
                    transfer = transfer.next();
                }
            };

    ClassMatchedHandler handleTorrentResp
            = new ClassMatchedHandler<TorrentGet.Response, KContentMsg<KAddress, KHeader<KAddress>, TorrentGet.Response>>() {

                @Override
                public void handle(TorrentGet.Response content, KContentMsg<KAddress, KHeader<KAddress>, TorrentGet.Response> container) {
                    transfer.handleTorrentResp(container, content);
                    transfer = transfer.next();
                }
            };

    Handler handleTorrentTimeout = new Handler<TorrentTimeout.Metadata>() {
        @Override
        public void handle(TorrentTimeout.Metadata timeout) {
            transfer.handleTorrentTimeout(timeout);
            transfer = transfer.next();
        }
    };

    Handler handleAdvanceDownload = new Handler<TorrentTimeout.AdvanceDownload>() {
        @Override
        public void handle(TorrentTimeout.AdvanceDownload timeout) {
            transfer.handleAdvanceDownload(timeout);
            transfer = transfer.next();
        }
    };

    Handler handleExtendedTorrentResp = new Handler<Transfer.DownloadResponse>() {
        @Override
        public void handle(Transfer.DownloadResponse resp) {
            transfer.handleExtendedTorrentResp(resp);
            transfer = transfer.next();
        }
    };

    ClassMatchedHandler handleLedbatReq
            = new ClassMatchedHandler<LedbatMsg.Request, KContentMsg<KAddress, KHeader<KAddress>, LedbatMsg.Request>>() {
                @Override
                public void handle(LedbatMsg.Request content, KContentMsg container) {
                    if (content.payload instanceof HashGet.Request) {
                        transfer.handleHashReq(container, content);
                        transfer = transfer.next();
                    } else {
                        transfer.handlePieceReq(container, content);
                        transfer = transfer.next();
                    }
                }
            };

    ClassMatchedHandler handleLedbatResp
            = new ClassMatchedHandler<LedbatMsg.Response, KContentMsg<KAddress, KHeader<KAddress>, LedbatMsg.Response>>() {

                @Override
                public void handle(LedbatMsg.Response content, KContentMsg container) {
                    if (content.payload instanceof HashGet.Response) {
                        transfer.handleHashResp(container, content);
                        transfer = transfer.next();
                    } else {
                        transfer.handlePieceResp(container, content);
                        transfer = transfer.next();
                    }

                }
            };

    Handler handleHashTimeout = new Handler<TorrentTimeout.Hash>() {
        @Override
        public void handle(TorrentTimeout.Hash timeout) {
            transfer.handleHashTimeout(timeout);
            transfer = transfer.next();
        }
    };

//    ClassMatchedHandler handlePieceReq
//            = new ClassMatchedHandler<LedbatMsg.Request<PieceGet.Request>, KContentMsg<KAddress, KHeader<KAddress>, LedbatMsg.Request<PieceGet.Request>>>() {
//                @Override
//                public void handle(LedbatMsg.Request<PieceGet.Request> content, KContentMsg<KAddress, KHeader<KAddress>, LedbatMsg.Request<PieceGet.Request>> container) {
//                    transfer.handlePieceReq(container, content);
//                    transfer = transfer.next();
//                }
//            };
//
//    ClassMatchedHandler handlePieceResp
//            = new ClassMatchedHandler<LedbatMsg.Response<PieceGet.Response>, KContentMsg<KAddress, KHeader<KAddress>, LedbatMsg.Response<PieceGet.Response>>>() {
//
//                @Override
//                public void handle(LedbatMsg.Response<PieceGet.Response> content, KContentMsg<KAddress, KHeader<KAddress>, LedbatMsg.Response<PieceGet.Response>> container) {
//                    transfer.handlePieceResp(container, content);
//                    transfer = transfer.next();
//                }
//            };

    Handler handlePieceTimeout = new Handler<TorrentTimeout.Piece>() {
        @Override
        public void handle(TorrentTimeout.Piece timeout) {
            transfer.handlePieceTimeout(timeout);
            transfer = transfer.next();
        }
    };

    public static class Init extends se.sics.kompics.Init<TorrentComp> {

        public final KAddress selfAdr;
        public final Identifier overlayId;
        public final StorageProvider storageProvider;
        public List<KAddress> partners;
        public final boolean upload;
        public final Optional<Pair<byte[], TransferDetails>> transferDetails;

        public Init(KAddress selfAdr, Identifier overlayId, StorageProvider storageProvider, List<KAddress> partners,
                boolean upload, Optional<Pair<byte[], TransferDetails>> transferDetails) {
            this.selfAdr = selfAdr;
            this.overlayId = overlayId;
            this.storageProvider = storageProvider;
            this.partners = partners;
            this.upload = upload;
            this.transferDetails = transferDetails;
        }
    }

    private void scheduleReport() {
        SchedulePeriodicTimeout spt = new SchedulePeriodicTimeout(torrentConfig.reportDelay, torrentConfig.reportDelay);
        ReportTimeout rt = new ReportTimeout(spt);
        periodicReport = rt.getTimeoutId();
        spt.setTimeoutEvent(rt);
        trigger(spt, timerPort);
    }

    public static class LoadCheckTimeout extends Timeout implements StreamEvent {

        public final long createdAt;
        public final long delay;

        public LoadCheckTimeout(ScheduleTimeout st, long delay) {
            super(st);
            this.createdAt = System.currentTimeMillis();
            this.delay = delay;
        }

        @Override
        public Identifier getId() {
            return new UUIDIdentifier(getTimeoutId());
        }
    }
}
