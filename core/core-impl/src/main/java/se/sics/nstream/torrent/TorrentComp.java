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
import se.sics.ktoolbox.util.network.basic.BasicContentMsg;
import se.sics.ktoolbox.util.network.basic.BasicHeader;
import se.sics.nstream.StreamEvent;
import se.sics.nstream.report.TransferStatusPort;
import se.sics.nstream.report.event.DownloadStatus;
import se.sics.nstream.report.event.ReportTimeout;
import se.sics.nstream.torrent.event.HashGet;
import se.sics.nstream.torrent.event.PieceGet;
import se.sics.nstream.torrent.event.TorrentGet;
import se.sics.nstream.torrent.event.TorrentTimeout;
import se.sics.nstream.transfer.Transfer;
import se.sics.nstream.transfer.TransferMngrPort;
import se.sics.nstream.util.TransferDetails;
import se.sics.nstream.util.actuator.ComponentLoad;
import se.sics.nstream.util.actuator.DownloadStates;

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
    private ComponentLoad componentLoad;
    private TransferFSM transfer;
    private RoundRobinConnMngr router;
    //**************************************************************************
    private UUID periodicReport;
    private UUID loadCheck;

    public TorrentComp(Init init) {
        logPrefix = "<" + init.selfAdr.getId() + ">";
        torrentConfig = new TorrentConfig();
        router = new RoundRobinConnMngr(init.partners);
        componentLoad = new ComponentLoad();
        storageProvider(init.storageProvider);
        transferState(init);

        subscribe(handleStart, control);
        subscribe(handleReportTimeout, timerPort);

        subscribe(handleTorrentReq, networkPort);
        subscribe(handleTorrentResp, networkPort);
        subscribe(handleTorrentTimeout, timerPort);
        subscribe(handleAdvanceDownload, timerPort);
        subscribe(handleHashReq, networkPort);
        subscribe(handleHashResp, networkPort);
        subscribe(handleHashTimeout, timerPort);
        subscribe(handlePieceReq, networkPort);
        subscribe(handlePieceResp, networkPort);
        subscribe(handlePieceTimeout, timerPort);

        subscribe(handleLoadCheck, timerPort);
        subscribe(handleNetworkLoadCheck, networkPort);
        subscribe(handleNettyCheck, networkPort);
        subscribe(handleExtendedTorrentResp, transferMngrPort);
        subscribe(handleTransferStatusReq, transferStatusPort);
    }

    private void transferState(Init init) {
        TransferFSM.CommonState cs = new TransferFSM.CommonState(config(), torrentConfig, this.proxy, init.selfAdr, init.overlayId, router, componentLoad);
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
            transfer.start();
            scheduleReport();
            scheduleLoadCheck();
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
            LOG.info("{}report router:{}", logPrefix, router.report());
            LOG.info("{}report comp load:{}", logPrefix, componentLoad.report().toString());
        }
    };

    Handler handleTransferStatusReq  = new Handler<DownloadStatus.Request>() {
        @Override
        public void handle(DownloadStatus.Request event) {
            transfer.handleTransferStatusReq(event);
        }
    };

    Handler handleLoadCheck = new Handler<LoadCheckTimeout>() {
        @Override
        public void handle(LoadCheckTimeout timeout) {
            sendNetworkCheck();
        }
    };

    private void sendNetworkCheck() {
        KHeader header = new BasicHeader(null, null, null);
        KContentMsg msg = new BasicContentMsg(header, new LoadCheck());
        trigger(msg, networkPort.getPair());
    }

    Handler handleNettyCheck = new Handler<MessageNotify.Resp>() {
        @Override
        public void handle(MessageNotify.Resp resp) {
            transfer.handleNotify(resp);
        }
    };

    ClassMatchedHandler handleNetworkLoadCheck
            = new ClassMatchedHandler<LoadCheck, KContentMsg<KAddress, KHeader<KAddress>, LoadCheck>>() {
                @Override
                public void handle(LoadCheck content, KContentMsg<KAddress, KHeader<KAddress>, LoadCheck> container) {
                    long now = System.currentTimeMillis();
                    long queueDelay = now - content.createdAt;
                    componentLoad.adjustState(queueDelay);
                    if (router.changed()) {
                        componentLoad.outsideChange();
                    }
                    if (componentLoad.state().equals(DownloadStates.SLOW_DOWN)) {
                        router.slowDown();
                    }
                    LOG.info("{}component state:{} router:{}", new Object[]{logPrefix, componentLoad.state(), router.totalSlots()});
                    scheduleLoadCheck();
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

    ClassMatchedHandler handleHashReq
            = new ClassMatchedHandler<HashGet.Request, KContentMsg<KAddress, KHeader<KAddress>, HashGet.Request>>() {
                @Override
                public void handle(HashGet.Request content, KContentMsg<KAddress, KHeader<KAddress>, HashGet.Request> container) {
                    transfer.handleHashReq(container, content);
                    transfer = transfer.next();
                }
            };

    ClassMatchedHandler handleHashResp
            = new ClassMatchedHandler<HashGet.Response, KContentMsg<KAddress, KHeader<KAddress>, HashGet.Response>>() {

                @Override
                public void handle(HashGet.Response content, KContentMsg<KAddress, KHeader<KAddress>, HashGet.Response> container) {
                    transfer.handleHashResp(container, content);
                    transfer = transfer.next();
                }
            };

    Handler handleHashTimeout = new Handler<TorrentTimeout.Hash>() {
        @Override
        public void handle(TorrentTimeout.Hash timeout) {
            transfer.handleHashTimeout(timeout);
            transfer = transfer.next();
        }
    };

    ClassMatchedHandler handlePieceReq
            = new ClassMatchedHandler<PieceGet.Request, KContentMsg<KAddress, KHeader<KAddress>, PieceGet.Request>>() {
                @Override
                public void handle(PieceGet.Request content, KContentMsg<KAddress, KHeader<KAddress>, PieceGet.Request> container) {
                    transfer.handlePieceReq(container, content);
                    transfer = transfer.next();
                }
            };

    ClassMatchedHandler handlePieceResp
            = new ClassMatchedHandler<PieceGet.Response, KContentMsg<KAddress, KHeader<KAddress>, PieceGet.Response>>() {

                @Override
                public void handle(PieceGet.Response content, KContentMsg<KAddress, KHeader<KAddress>, PieceGet.Response> container) {
                    transfer.handlePieceResp(container, content);
                    transfer = transfer.next();
                }
            };

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

    private void scheduleLoadCheck() {
        ScheduleTimeout st = new ScheduleTimeout(componentLoad.checkPeriod());
        LoadCheckTimeout lct = new LoadCheckTimeout(st, componentLoad.checkPeriod());
        st.setTimeoutEvent(lct);
        trigger(st, timerPort);
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

    public static class LoadCheck implements StreamEvent {

        public final Identifier eventId;
        public final long createdAt;

        public LoadCheck() {
            this.eventId = UUIDIdentifier.randomId();
            this.createdAt = System.currentTimeMillis();
        }

        @Override
        public Identifier getId() {
            return eventId;
        }

    }
}
