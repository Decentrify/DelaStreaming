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
package se.sics.gvod.stream.congestion;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.nstream.StreamEvent;
import se.sics.gvod.stream.congestion.event.external.PLedbatConnection;
import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.Handler;
import se.sics.kompics.Negative;
import se.sics.kompics.Positive;
import se.sics.kompics.Start;
import se.sics.kompics.network.Network;
import se.sics.kompics.timer.CancelTimeout;
import se.sics.kompics.timer.SchedulePeriodicTimeout;
import se.sics.kompics.timer.Timeout;
import se.sics.kompics.timer.Timer;
import se.sics.ktoolbox.util.identifiable.Identifier;
import se.sics.ktoolbox.util.identifiable.basic.UUIDIdentifier;
import se.sics.ktoolbox.util.network.KAddress;
import se.sics.ktoolbox.util.network.KContentMsg;
import se.sics.ktoolbox.util.network.KHeader;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class PullLedbatComp extends ComponentDefinition {

    private static final Logger LOG = LoggerFactory.getLogger(PullLedbat.class);
    private String logPrefix = "";

    //****************************CONNECTIONS***********************************
    //***********************EXTERNAL_CONNECT_TO********************************
    Negative<PLedbatPort> ledbatPort = provides(PLedbatPort.class);
    Negative<Network> providedNetPort = provides(Network.class);
    Positive<Network> requiredNetPort = requires(Network.class);
    Positive<Timer> timerPort = requires(Timer.class);
    //**************************EXTERNAL_STATE**********************************
    private final KAddress selfAdr;
    private final long baseSeed;
    //**************************INTERNAL_STATE**********************************
    private final Map<Identifier, PullLedbat> connections = new HashMap<>();
    private UUID roundTId;

    public PullLedbatComp(Init init) {
        selfAdr = init.selfAdr;
        logPrefix = "<nid:" + selfAdr.getId() + ">";
        LOG.info("{}initializing...", logPrefix);
        
        baseSeed = init.seed;

        subscribe(handleStart, control);
        subscribe(handleRound, timerPort);
        subscribe(handleTrackConnection, ledbatPort);
        subscribe(handleUntrackConnection, ledbatPort);
        subscribe(handleOutgoing, providedNetPort);
        subscribe(handleIncoming, requiredNetPort);
    }

    Handler handleStart = new Handler<Start>() {
        @Override
        public void handle(Start event) {
            LOG.info("{}starting...", logPrefix);
            scheduleRound();
        }
    };

    private void answerNetwork(KContentMsg msg, Object content) {
        KContentMsg resp = msg.answer(content);
        LOG.trace("{}sending:{}", logPrefix, resp);
        trigger(resp, requiredNetPort);
    }

    Handler handleRound = new Handler<RoundTimeout>() {
        @Override
        public void handle(RoundTimeout event) {
            LOG.debug("{}round", logPrefix);
            for (Map.Entry<Identifier, PullLedbat> pl : connections.entrySet()) {
                String report = pl.getValue().round();
                LOG.debug("{}report {}:{}", new Object[]{logPrefix, pl.getKey(), report});
            }
        }
    };

    Handler handleTrackConnection = new Handler<PLedbatConnection.TrackRequest>() {
        @Override
        public void handle(PLedbatConnection.TrackRequest req) {
            LOG.trace("{}received:{}", logPrefix, req);
            PullLedbat conn = connections.get(req.target.getId());
            if (conn == null) {
                conn = new PullLedbat(req, new Random(baseSeed + req.target.getId().partition(Integer.MAX_VALUE)));
                connections.put(req.target.getId(), conn);
            } else {
                throw new RuntimeException("missing logic - not yet dealing with multi conn");
            }
        }
    };

    Handler handleUntrackConnection = new Handler<PLedbatConnection.Untrack>() {
        @Override
        public void handle(PLedbatConnection.Untrack event) {
            LOG.trace("{}received:{}", logPrefix, event);
            connections.remove(event.req.target.getId());
        }
    };

    Handler handleOutgoing = new Handler<KContentMsg>() {
        @Override
        public void handle(KContentMsg event) {
            LOG.trace("{}outgoing:{}", logPrefix, event);
            if (event.getContent() instanceof PLedbatMsg.Request) {
                trigger(event, requiredNetPort);
            } else if (event.getContent() instanceof PLedbatMsg.Response) {
                KContentMsg<KAddress, KHeader<KAddress>, PLedbatMsg.Response> msg = (KContentMsg<KAddress, KHeader<KAddress>, PLedbatMsg.Response>) event;
                PullLedbatComp.this.outgoing(msg.getContent(), msg);
            } else {
                trigger(event, requiredNetPort);
            }
        }
    };

    private void outgoing(PLedbatMsg.Response content, KContentMsg<KAddress, KHeader<KAddress>, PLedbatMsg.Response> container) {
        long outgoingTimestamp = System.currentTimeMillis();
        content.setSendingTime(outgoingTimestamp);
        trigger(container, requiredNetPort);
    }

    Handler handleIncoming = new Handler<KContentMsg>() {
        @Override
        public void handle(KContentMsg event) {
            LOG.trace("{}incoming:{}", logPrefix, event);
            if (event.getContent() instanceof PLedbatMsg.Request) {
                trigger(event, providedNetPort);
            } else if (event.getContent() instanceof PLedbatMsg.Response) {
                KContentMsg<KAddress, KHeader<KAddress>, PLedbatMsg.Response> msg = (KContentMsg<KAddress, KHeader<KAddress>, PLedbatMsg.Response>) event;
                PullLedbatComp.this.incoming(msg.getContent(), msg);
            } else {
                trigger(event, providedNetPort);
            }
        }
    };

    private void incoming(PLedbatMsg.Response content, KContentMsg<KAddress, KHeader<KAddress>, PLedbatMsg.Response> container) {
        KAddress target = container.getHeader().getSource();
        PullLedbat conn = connections.get(target.getId());
        if (conn == null) {
            LOG.warn("{}no ledbat tracking for target:{}", logPrefix, target);
            return;
        }
        long incomingTimestamp = System.currentTimeMillis();
        content.setReceivedTime(incomingTimestamp);
        conn.incoming(content, proxy);
        trigger(container, providedNetPort);
    }

    public static class Init extends se.sics.kompics.Init<PullLedbatComp> {
        public final KAddress selfAdr;
        public final long seed;
        
        public Init(KAddress selfAdr, long seed) {
            this.selfAdr = selfAdr;
            this.seed = seed;
        }
    }

    public void cancelTimeout(UUID tId) {
        CancelTimeout ct = new CancelTimeout(tId);
        trigger(ct, timerPort);
    }

    public void scheduleRound() {
        SchedulePeriodicTimeout spt = new SchedulePeriodicTimeout(PullLedbat.BASE_HISTORY_ROUND_TIME, PullLedbat.BASE_HISTORY_ROUND_TIME);
        Timeout t = new RoundTimeout(spt);
        spt.setTimeoutEvent(t);
        trigger(spt, timerPort);
        roundTId = t.getTimeoutId();
    }

    public static class RoundTimeout extends Timeout implements StreamEvent {

        public RoundTimeout(SchedulePeriodicTimeout spt) {
            super(spt);
        }

        @Override
        public Identifier getId() {
            return new UUIDIdentifier(getTimeoutId());
        }

        @Override
        public String toString() {
            return "RoundTimeout<" + getId() + ">";
        }
    }
}
