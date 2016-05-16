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
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.gvod.stream.StreamEvent;
import se.sics.gvod.stream.congestion.event.external.PLedbatConnection;
import se.sics.kompics.ClassMatchedHandler;
import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.Handler;
import se.sics.kompics.Negative;
import se.sics.kompics.Positive;
import se.sics.kompics.Start;
import se.sics.kompics.network.Network;
import se.sics.kompics.timer.CancelTimeout;
import se.sics.kompics.timer.ScheduleTimeout;
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

    private static final Logger LOG = LoggerFactory.getLogger(PullLedbatComp.class);
    private String logPrefix = "";

    //****************************CONNECTIONS***********************************
    //***********************EXTERNAL_CONNECT_TO********************************
    Negative<PLedbatPort> ledbatPort = provides(PLedbatPort.class);
    Negative<Network> providedNetPort = provides(Network.class);
    Positive<Network> requiredNetPort = requires(Network.class);
    Positive<Timer> timerPort = requires(Timer.class);
    //**************************INTERNAL_STATE**********************************
    private final long defaultMsgTimeout = 2000;
    private final Map<Identifier, Connection> connections = new HashMap<>();
    //<msgId, timeoutId>
    private final Map<Identifier, UUID> pendingMsg = new HashMap<>();

    public PullLedbatComp(Init init) {
        LOG.info("{}initiaiting...", logPrefix);

        subscribe(handleStart, control);
        subscribe(handleTrackConnection, ledbatPort);
        subscribe(handleUntrackConnection, ledbatPort);
        subscribe(handleOutgoingRequest, providedNetPort);
        subscribe(handleOutgoingResponse, providedNetPort);
        subscribe(handleIncomingResponse, requiredNetPort);
    }

    Handler handleStart = new Handler<Start>() {
        @Override
        public void handle(Start event) {
            LOG.info("{}starting...", logPrefix);
        }
    };

    private void answerNetwork(KContentMsg msg, Object content) {
        KContentMsg resp = msg.answer(content);
        LOG.trace("{}sending:{}", logPrefix, resp);
        trigger(resp, requiredNetPort);
    }

    Handler handleTrackConnection = new Handler<PLedbatConnection.TrackRequest>() {
        @Override
        public void handle(PLedbatConnection.TrackRequest req) {
            LOG.trace("{}received:{}", logPrefix, req);
            Connection conn = connections.get(req.target.getId());
            if (conn == null) {
                conn = new Connection(req);
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

    ClassMatchedHandler handleOutgoingRequest
            = new ClassMatchedHandler<PLedbatMsg.Request, KContentMsg<KAddress, KHeader<KAddress>, PLedbatMsg.Request>>() {
                @Override
                public void handle(PLedbatMsg.Request content, KContentMsg<KAddress, KHeader<KAddress>, PLedbatMsg.Request> container) {
                    LOG.trace("{}outgoing:{}", logPrefix, container);
                    KAddress target = container.getHeader().getSource();
                    for (Identifier pendingResp : content.pendingResp()) {
                        pendingMsg.put(pendingResp, schedulePendingAck(target, pendingResp));
                    }
                    trigger(container, requiredNetPort);
                }
            };

    ClassMatchedHandler handleOutgoingResponse
            = new ClassMatchedHandler<PLedbatMsg.Response, KContentMsg<KAddress, KHeader<KAddress>, PLedbatMsg.Response>>() {
                @Override
                public void handle(PLedbatMsg.Response content, KContentMsg<KAddress, KHeader<KAddress>, PLedbatMsg.Response> container) {
                    LOG.trace("{}outgoing:{}", logPrefix, container);
                    content.setSendingTime(System.currentTimeMillis());
                    trigger(container, requiredNetPort);
                }
            };

    ClassMatchedHandler handleIncomingResponse
            = new ClassMatchedHandler<PLedbatMsg.Response, KContentMsg<KAddress, KHeader<KAddress>, PLedbatMsg.Response>>() {
                @Override
                public void handle(PLedbatMsg.Response content, KContentMsg<KAddress, KHeader<KAddress>, PLedbatMsg.Response> container) {
                    LOG.trace("{}incoming:{}", logPrefix, container);
                    UUID tId = pendingMsg.remove(content.getId());
                    if (tId == null) {
                        LOG.debug("{}late:{}", logPrefix, container);
                        return;
                    } else {
                        cancelTimeout(tId);
                    }
                    KAddress target = container.getHeader().getSource();
                    Connection conn = connections.get(target.getId());
                    if (conn == null) {
                        LOG.warn("{}no ledbat tracking for target:{}", logPrefix, target);
                        return;
                    }
                    conn.incoming(content.getSendingTime(), proxy);
                    trigger(container, providedNetPort);
                }
            };

    Handler handleMsgTimeout = new Handler<MsgTimeout>() {
        @Override
        public void handle(MsgTimeout timeout) {
            LOG.debug("{}src:{}timeout", logPrefix, timeout.target);
            UUID tId = pendingMsg.remove(timeout.msgId);
            if (tId == null) {
                Connection conn = connections.get(timeout.target.getId());
                if (conn == null) {
                    LOG.warn("{}no ledbat tracking for target:{}", logPrefix, timeout.target);
                    return;
                }
                conn.timeout(proxy);
            } else {
                LOG.debug("{}late:{}", logPrefix, timeout);
                return;
            }
        }
    };

    public static class Init extends se.sics.kompics.Init<PullLedbatComp> {

    }

    public void cancelTimeout(UUID tId) {
        CancelTimeout ct = new CancelTimeout(tId);
        trigger(ct, timerPort);
    }

    public UUID schedulePendingAck(KAddress target, Identifier msgId) {
        ScheduleTimeout st = new ScheduleTimeout(defaultMsgTimeout);
        Timeout t = new MsgTimeout(st, target, msgId);
        st.setTimeoutEvent(t);
        trigger(st, timerPort);
        return t.getTimeoutId();
    }

    public class MsgTimeout extends Timeout implements StreamEvent {

        public final KAddress target;
        public final Identifier msgId;

        public MsgTimeout(ScheduleTimeout st, KAddress target, Identifier msgId) {
            super(st);
            this.target = target;
            this.msgId = msgId;
        }

        @Override
        public Identifier getId() {
            return new UUIDIdentifier(getTimeoutId());
        }

        @Override
        public String toString() {
            return "MsgTimeout<" + getId() + ">";
        }
    }
}
