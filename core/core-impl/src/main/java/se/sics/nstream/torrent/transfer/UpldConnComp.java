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
package se.sics.nstream.torrent.transfer;

import com.google.common.collect.Sets;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import org.javatuples.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.kompics.ClassMatchedHandler;
import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.Handler;
import se.sics.kompics.Negative;
import se.sics.kompics.Positive;
import se.sics.kompics.Start;
import se.sics.kompics.network.Network;
import se.sics.kompics.timer.CancelPeriodicTimeout;
import se.sics.kompics.timer.SchedulePeriodicTimeout;
import se.sics.kompics.timer.Timeout;
import se.sics.kompics.timer.Timer;
import se.sics.ktoolbox.util.identifiable.Identifiable;
import se.sics.ktoolbox.util.network.KAddress;
import se.sics.ktoolbox.util.network.KContentMsg;
import se.sics.ktoolbox.util.network.KHeader;
import se.sics.ktoolbox.util.reference.KReference;
import se.sics.ktoolbox.util.reference.KReferenceException;
import se.sics.ledbat.ncore.msg.LedbatMsg;
import se.sics.nstream.torrent.transfer.msg.CacheHint;
import se.sics.nstream.torrent.transfer.msg.DownloadHash;
import se.sics.nstream.torrent.transfer.msg.DownloadPiece;
import se.sics.nstream.torrent.transfer.upld.event.GetBlocks;
import se.sics.nstream.torrent.transfer.upld.event.UpldConnReport;
import se.sics.nstream.torrent.util.TorrentConnId;
import se.sics.nstream.util.BlockDetails;
import se.sics.nstream.util.BlockHelper;
import se.sics.nstream.util.range.KPiece;
import se.sics.nstream.util.range.RangeKReference;
import se.sics.nutil.tracking.load.NetworkQueueLoadProxy;
import se.sics.nutil.tracking.load.QueueLoadConfig;

/**
 *
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class UpldConnComp extends ComponentDefinition {

    private static final Logger LOG = LoggerFactory.getLogger(UpldConnComp.class);
    private String logPrefix;

    private static final long REPORT_PERIOD = 1000;
    //**************************************************************************
    Negative<UpldConnPort> connPort = provides(UpldConnPort.class);
    Positive<Network> networkPort = requires(Network.class);
    Positive<Timer> timerPort = requires(Timer.class);
    //**************************************************************************
    private final TorrentConnId connId;
    private final KAddress self;
    private final BlockDetails defaultBlock;
    private final boolean withHashes;
    //**************************************************************************
    private final NetworkQueueLoadProxy networkQueueLoad;
    private final Map<Integer, BlockDetails> irregularBlocks = new HashMap<>();
    private final Map<Integer, KReference<byte[]>> servedBlocks = new HashMap<>();
    private final Map<Integer, byte[]> servedHashes = new HashMap<>();
    private KContentMsg<?, ?, CacheHint.Request> pendingCacheReq;
    //**************************************************************************
    private UUID reportTid;

    public UpldConnComp(Init init) {
        connId = init.connId;
        self = init.self;
        defaultBlock = init.defaultBlock;
        withHashes = init.withHashes;
        logPrefix = connId.toString();

        networkQueueLoad = new NetworkQueueLoadProxy(logPrefix, proxy, new QueueLoadConfig(config()));
        subscribe(handleStart, control);
        subscribe(handleReport, timerPort);
        subscribe(handleCache, networkPort);
        subscribe(handleGetBlocks, connPort);
        subscribe(handleLedbat, networkPort);
    }

    Handler handleStart = new Handler<Start>() {
        @Override
        public void handle(Start event) {
            LOG.info("{}starting", logPrefix);
            networkQueueLoad.start();
            scheduleReport();
        }
    };

    @Override
    public void tearDown() {
        networkQueueLoad.tearDown();
        cancelReport();
        for (KReference<byte[]> block : servedBlocks.values()) {
            silentRelease(block);
        }
    }

    Handler handleReport = new Handler<ReportTimeout>() {
        @Override
        public void handle(ReportTimeout event) {
            double queueAdjustment = networkQueueLoad.adjustment();
            Pair<Integer, Integer> queueDelay = networkQueueLoad.queueDelay();
            trigger(new UpldConnReport(connId, queueDelay, queueAdjustment), connPort);
        }
    };

    //**************************************************************************
    ClassMatchedHandler handleCache
            = new ClassMatchedHandler<CacheHint.Request, KContentMsg<KAddress, KHeader<KAddress>, CacheHint.Request>>() {

                @Override
                public void handle(CacheHint.Request content, KContentMsg<KAddress, KHeader<KAddress>, CacheHint.Request> context) {
                    LOG.trace("{}received:{}", new Object[]{logPrefix, content});
                    if (pendingCacheReq == null) {
                        LOG.info("{}cache:{} req - ts:{} blocks:{}", 
                                new Object[]{logPrefix, content.getId(), content.requestCache.lStamp, content.requestCache.blocks});
                        pendingCacheReq = context;
                        Set<Integer> newCache = Sets.difference(content.requestCache.blocks, servedBlocks.keySet());
                        Set<Integer> delCache = new HashSet<>(Sets.difference(servedBlocks.keySet(), content.requestCache.blocks));

                        if (!newCache.isEmpty()) {
                            trigger(new GetBlocks.Request(connId, newCache, withHashes, content.requestCache), connPort);
                        } else {
                            answerCacheHint();
                        }
                        //release references that were retained when given to us
                        for (Integer blockNr : delCache) {
                            KReference<byte[]> block = servedBlocks.remove(blockNr);
                            servedHashes.remove(blockNr);
                            irregularBlocks.remove(blockNr);
                            silentRelease(block);
                        }
                    }
                }
            };

    Handler handleGetBlocks = new Handler<GetBlocks.Response>() {
        @Override
        public void handle(GetBlocks.Response resp) {
            //references are already retained by whoever gives them to us
            LOG.info("{}serving blocks:{} hashes:{}", new Object[]{logPrefix, resp.blocks.keySet(), resp.hashes.keySet()});
            servedBlocks.putAll(resp.blocks);
            servedHashes.putAll(resp.hashes);
            irregularBlocks.putAll(resp.irregularBlocks);
            answerCacheHint();
        }
    };

    private void answerCacheHint() {
        answerMsg(pendingCacheReq, pendingCacheReq.getContent().success());
        pendingCacheReq = null;
    }
    //**************************************************************************
    ClassMatchedHandler handleLedbat
            = new ClassMatchedHandler<LedbatMsg.Request, KContentMsg<KAddress, KHeader<KAddress>, LedbatMsg.Request>>() {
                @Override
                public void handle(LedbatMsg.Request content, KContentMsg<KAddress, KHeader<KAddress>, LedbatMsg.Request> context) {
                    Object baseContent = content.getWrappedContent();
                    if (baseContent instanceof DownloadPiece.Request) {
                        handlePiece(context, content);
                    } else if (baseContent instanceof DownloadHash.Request) {
                        handleHashes(context, content);
                    } else {
                        LOG.error("{}received:{}", logPrefix, content);
                        throw new RuntimeException("ups");
                    }
                }
            };

    public void handlePiece(KContentMsg msg, LedbatMsg.Request<DownloadPiece.Request> content) {
        int blockNr = content.getWrappedContent().piece.getValue0();
        BlockDetails blockDetails = irregularBlocks.containsKey(blockNr) ? irregularBlocks.get(blockNr) : defaultBlock;
        KReference<byte[]> block = servedBlocks.get(blockNr);
        if (block == null) {
            //TODO Alex
            return;
//            throw new RuntimeException("bad cache-block logic");
        }
        //retain block here - release in serializer
        if (!block.retain()) {
            throw new RuntimeException("bad ref logic");
        }
        KPiece pieceRange = BlockHelper.getPieceRange(content.getWrappedContent().piece, blockDetails, defaultBlock);
        RangeKReference piece = RangeKReference.createInstance(block, BlockHelper.getBlockPos(blockNr, defaultBlock), pieceRange);
        LedbatMsg.Response ledbatContent = content.answer(content.getWrappedContent().success(piece));
        answerMsg(msg, ledbatContent);
    }

    public void handleHashes(KContentMsg msg, LedbatMsg.Request<DownloadHash.Request> content) {
        Map<Integer, byte[]> hashValues = new TreeMap<>();
        for (Integer hashNr : content.getWrappedContent().hashes) {
            byte[] hashVal = servedHashes.get(hashNr);
            if (hashVal == null) {
                LOG.warn("{}no hash for:{} - not serving incomplete", logPrefix, hashNr);
                return;
            }
            hashValues.put(hashNr, hashVal);
        }
        LedbatMsg.Response ledbatContent = content.answer(content.getWrappedContent().success(hashValues));
        answerMsg(msg, ledbatContent);
    }
    //**************************************************************************

    private void answerMsg(KContentMsg original, Identifiable respContent) {
        LOG.trace("{}answering with:{}", logPrefix, respContent);
        trigger(original.answer(respContent), networkPort);
    }

    private void silentRelease(KReference<byte[]> ref) {
        try {
            ref.release();
        } catch (KReferenceException ex) {
            throw new RuntimeException(ex);
        }
    }

    public static class Init extends se.sics.kompics.Init<UpldConnComp> {

        public final TorrentConnId connId;
        public final KAddress self;
        public final BlockDetails defaultBlock;
        public final boolean withHashes;

        public Init(TorrentConnId connId, KAddress self, BlockDetails defaultBlock, boolean withHashes) {
            this.connId = connId;
            this.self = self;
            this.defaultBlock = defaultBlock;
            this.withHashes = withHashes;
        }
    }

    private void scheduleReport() {
        SchedulePeriodicTimeout spt = new SchedulePeriodicTimeout(REPORT_PERIOD, REPORT_PERIOD);
        ReportTimeout rt = new ReportTimeout(spt);
        spt.setTimeoutEvent(rt);
        reportTid = rt.getTimeoutId();
        trigger(spt, timerPort);
    }

    private void cancelReport() {
        CancelPeriodicTimeout cpd = new CancelPeriodicTimeout(reportTid);
        trigger(cpd, timerPort);
        reportTid = null;
    }

    public static class ReportTimeout extends Timeout {

        public ReportTimeout(SchedulePeriodicTimeout spt) {
            super(spt);
        }
    }
}
