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
import se.sics.kompics.PortType;
import se.sics.kompics.Positive;
import se.sics.kompics.Start;
import se.sics.kompics.network.Network;
import se.sics.kompics.timer.CancelPeriodicTimeout;
import se.sics.kompics.timer.SchedulePeriodicTimeout;
import se.sics.kompics.timer.Timer;
import se.sics.ktoolbox.util.identifiable.Identifier;
import se.sics.ktoolbox.util.network.KAddress;
import se.sics.ktoolbox.util.network.KContentMsg;
import se.sics.ktoolbox.util.network.KHeader;
import se.sics.nstream.report.ReportTimeout;
import se.sics.nstream.transfer.Transfer;
import se.sics.nstream.transfer.TransferMngrPort;
import se.sics.nstream.transfer.event.TransferTorrent;
import se.sics.nstream.util.TransferDetails;

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
    List<Positive> requiredPorts = new ArrayList<>();
    //**************************************************************************
    private final TorrentConfig torrentConfig;
    //**************************************************************************
    private TransferFSM transfer;
    private Router router;
    //**************************************************************************
    private UUID periodicReport;

    public TorrentComp(Init init) {
        torrentConfig = new TorrentConfig();
        router = new RoundRobinRouter(init.partners);
        storageProvider(init.storageProvider);
        transferState(init);
        
        subscribe(handleStart, control);
        subscribe(handleTorrentReq, networkPort);
        subscribe(handleTorrentResp, networkPort);
        subscribe(handleReportTimeout, timerPort);
        subscribe(handleTorrentTimeout, timerPort);
        subscribe(handleExtendedTorrentResp, transferMngrPort);
    }

    private void transferState(Init init) {
        TransferFSM.CommonState cs = new TransferFSM.CommonState(config(), torrentConfig, this.proxy, init.selfAdr, init.overlayId, router);
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
        }
    };
    
     private void scheduleReportTimeout() {
         SchedulePeriodicTimeout spt = new SchedulePeriodicTimeout(torrentConfig.reportDelay, torrentConfig.reportDelay);
            ReportTimeout rt = new ReportTimeout(spt);
            periodicReport = rt.getTimeoutId();
            trigger(rt, timerPort);
            spt.setTimeoutEvent(rt);
        }

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
    
    ClassMatchedHandler handleTorrentReq
            = new ClassMatchedHandler<TransferTorrent.Request, KContentMsg<KAddress, KHeader<KAddress>, TransferTorrent.Request>>() {
                @Override
                public void handle(TransferTorrent.Request content, KContentMsg<KAddress, KHeader<KAddress>, TransferTorrent.Request> container) {
                    transfer.handleTorrentReq(container, content);
                    transfer = transfer.next();
                }
            };

    ClassMatchedHandler handleTorrentResp
            = new ClassMatchedHandler<TransferTorrent.Response, KContentMsg<KAddress, KHeader<KAddress>, TransferTorrent.Response>>() {

                @Override
                public void handle(TransferTorrent.Response content, KContentMsg<KAddress, KHeader<KAddress>, TransferTorrent.Response> container) {
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

    Handler handleExtendedTorrentResp = new Handler<Transfer.DownloadResponse>() {
        @Override
        public void handle(Transfer.DownloadResponse resp) {
            transfer.handleExtendedTorrentResp(resp);
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
}
