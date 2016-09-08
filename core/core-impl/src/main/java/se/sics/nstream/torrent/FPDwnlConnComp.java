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

import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.Handler;
import se.sics.kompics.Positive;
import se.sics.kompics.Start;
import se.sics.kompics.network.Network;
import se.sics.kompics.timer.CancelPeriodicTimeout;
import se.sics.kompics.timer.SchedulePeriodicTimeout;
import se.sics.kompics.timer.Timer;
import se.sics.ktoolbox.util.identifiable.Identifier;
import se.sics.ktoolbox.util.network.KAddress;
import se.sics.ktoolbox.util.tracking.load.NetworkQueueLoadProxy;
import se.sics.ktoolbox.util.tracking.load.QueueLoadConfig;
import se.sics.ledbat.core.AppCongestionWindow;
import se.sics.ledbat.core.LedbatConfig;
import se.sics.nstream.torrent.event.TorrentTimeout;

/**
 * A FPDwnlConnComp per torrent per peer per file per dwnl (there is an
 * equivalent comp for upld)
 *
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class FPDwnlConnComp extends ComponentDefinition {

    private static final Logger LOG = LoggerFactory.getLogger(FPDwnlConnComp.class);
    private String logPrefix;

    private static final long ADVANCE_DOWNLOAD = 1000;
    //**************************************************************************
    Positive<Network> networkPort = requires(Network.class);
    Positive<Timer> timerPort = requires(Timer.class);
    //**************************************************************************
    private final Identifier overlayId;
    private final KAddress self;
    private final KAddress target;
    //**************************************************************************
    private final NetworkQueueLoadProxy networkQueueLoad;
    private final AppCongestionWindow cwnd;
    private final LinkedList<
    //**************************************************************************
    private UUID advanceDownloadTid;

    public FPDwnlConnComp(Init init) {
        overlayId = init.overlayId;
        self = init.self;
        target = init.target;
        logPrefix = "<nid:" + self.getId() + ",oid:" + overlayId + ",tid:" + target.getId() + ">";

        networkQueueLoad = new NetworkQueueLoadProxy(logPrefix, proxy, new QueueLoadConfig(config()));
        cwnd = new AppCongestionWindow(new LedbatConfig(config()), overlayId);

        subscribe(handleStart, control);
        subscribe(handleAdvanceDownload, timerPort);
    }

    Handler handleStart = new Handler<Start>() {
        @Override
        public void handle(Start event) {
            LOG.info("{}starting...", logPrefix);
            networkQueueLoad.startTracking();
            scheduleAdvanceDownload();
        }
    };

    @Override
    public void tearDown() {
        cancelAdvanceDownload();
    }
    
    Handler handleAdvanceDownload = new Handler<TorrentTimeout.AdvanceDownload>() {
        @Override
        public void handle(TorrentTimeout.AdvanceDownload event) {
            LOG.trace("{}advance download", logPrefix);
        }
    };

    private void cancelAdvanceDownload() {
        CancelPeriodicTimeout cpd = new CancelPeriodicTimeout(advanceDownloadTid);
        trigger(cpd, timerPort);
        advanceDownloadTid = null;
    }

    private SchedulePeriodicTimeout scheduleAdvanceDownload() {
        SchedulePeriodicTimeout spt = new SchedulePeriodicTimeout(TransferConfig.advanceDownloadPeriod, TransferConfig.advanceDownloadPeriod);
        TorrentTimeout.AdvanceDownload tt = new TorrentTimeout.AdvanceDownload(spt);
        spt.setTimeoutEvent(tt);
        advanceDownloadTid = tt.getTimeoutId();
        return spt;
    }

    public static class Init extends se.sics.kompics.Init<FPDwnlConnComp> {

        public final Identifier overlayId;
        public final KAddress self;
        public final KAddress target;

        public Init(Identifier overlayId, KAddress self, KAddress target) {
            this.overlayId = overlayId;
            this.self = self;
            this.target = target;
        }
    }

}
