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
package se.sics.gvod.cc;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.UUID;
import org.javatuples.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.gvod.cc.event.CCAddOverlay;
import se.sics.gvod.cc.event.CCJoinOverlay;
import se.sics.gvod.cc.op.CCAddOverlayOp;
import se.sics.gvod.cc.op.CCJoinOverlayOp;
import se.sics.gvod.cc.opMngr.Operation;
import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.Handler;
import se.sics.kompics.Init;
import se.sics.kompics.KompicsEvent;
import se.sics.kompics.Negative;
import se.sics.kompics.Positive;
import se.sics.kompics.Start;
import se.sics.kompics.Stop;
import se.sics.kompics.timer.CancelPeriodicTimeout;
import se.sics.kompics.timer.SchedulePeriodicTimeout;
import se.sics.kompics.timer.Timeout;
import se.sics.kompics.timer.Timer;
import se.sics.ktoolbox.cc.bootstrap.CCOperationPort;
import se.sics.ktoolbox.cc.operation.event.CCOpRequest;
import se.sics.ktoolbox.cc.operation.event.CCOpResponse;
import se.sics.ktoolbox.cc.operation.event.CCOpTimeout;
import se.sics.ktoolbox.util.identifiable.Identifier;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class VoDCaracalClientComp extends ComponentDefinition {

    private static final Logger LOG = LoggerFactory.getLogger(VoDCaracalClientComp.class);
    private String logPrefix = "";

    private Negative<VoDCaracalClientPort> vodCCPort = provides(VoDCaracalClientPort.class);
    private Positive<CCOperationPort> ccPort = requires(CCOperationPort.class);
    private Positive<Timer> timer = requires(Timer.class);

    private final VoDCaracalClientConfig ccConfig;
    private final byte[] schemaId;

    private UUID sanityCheckTId;

    private final Map<Identifier, Operation> activeOps = new HashMap<>();
    //<opReq, <opId, retries>>
    private final Map<Identifier, Pair<Identifier, Integer>> pendingMsgs = new HashMap<>(); 

    public VoDCaracalClientComp(VoDCaracalClientInit init) {
        LOG.info("{}initiating...", logPrefix);

        this.ccConfig = init.ccConfig;
        this.schemaId = init.schemaId;

        this.sanityCheckTId = null;

        subscribe(handleStart, control);
        subscribe(handleStop, control);
        subscribe(handleSanityCheck, timer);
        subscribe(handleAddOverlay, vodCCPort);
        subscribe(handleJoinOverlay, vodCCPort);
        subscribe(handleCCResponse, ccPort);
        subscribe(handleCCTimeout, ccPort);
    }

    //**************************************************************************
    Handler handleStart = new Handler<Start>() {
        @Override
        public void handle(Start e) {
            LOG.info("{}starting", logPrefix);
            scheduleSanityCheck();
        }
    };

    Handler handleStop = new Handler<Stop>() {
        @Override
        public void handle(Stop e) {
            LOG.info("{}stopping", logPrefix);
            cancelSanityCheck();
        }
    };

    Handler handleSanityCheck = new Handler<SanityCheckTimeout>() {
        @Override
        public void handle(SanityCheckTimeout event) {
            LOG.info("{}memory usage - activeOps:{}, pendingMsgs:{}",
                    new Object[]{logPrefix, activeOps.size(), pendingMsgs.size()});
        }
    };

    private void scheduleSanityCheck() {
        if (sanityCheckTId != null) {
            LOG.warn("{}double starting sanityCheck", logPrefix);
            return;
        }
        SchedulePeriodicTimeout spt = new SchedulePeriodicTimeout(ccConfig.sanityCheckPeriod, ccConfig.sanityCheckPeriod);
        SanityCheckTimeout sc = new SanityCheckTimeout(spt);
        spt.setTimeoutEvent(sc);
        sanityCheckTId = sc.getTimeoutId();
        trigger(spt, timer);
    }

    private void cancelSanityCheck() {
        if (sanityCheckTId == null) {
            LOG.warn("{}double stopping sanityCheck", logPrefix);
            return;
        }
        CancelPeriodicTimeout cpt = new CancelPeriodicTimeout(sanityCheckTId);
        sanityCheckTId = null;
        trigger(cpt, timer);
    }

    public class SanityCheckTimeout extends Timeout {

        public SanityCheckTimeout(SchedulePeriodicTimeout request) {
            super(request);
        }

        @Override
        public String toString() {
            return "SANITYCHECK_TIMEOUT";
        }
    }
    //**************************************************************************
    Handler handleAddOverlay = new Handler<CCAddOverlay.Request>() {
        @Override
        public void handle(CCAddOverlay.Request req) {
            LOG.trace("{} received:{}", logPrefix, req);
            Operation op = new CCAddOverlayOp(schemaId, req);
            startOp(op);
        }
    };

    Handler handleJoinOverlay = new Handler<CCJoinOverlay.Request>() {
        @Override
        public void handle(CCJoinOverlay.Request req) {
            LOG.trace("{} received:{}", logPrefix, req);
            Operation op = new CCJoinOverlayOp(schemaId, req);
            startOp(op);
        }
    };

    Handler handleCCResponse = new Handler<CCOpResponse>() {
        @Override
        public void handle(CCOpResponse e) {
            LOG.trace("{}received:{}", logPrefix, e.opResp);
            Pair<Identifier, Integer> msgI = pendingMsgs.remove(e.getId());
            if (msgI == null) {
                LOG.warn("{}weird late response:{}", logPrefix, e.getId());
                return;
            }
            Operation op = activeOps.get(msgI.getValue0());
            if (op.handleEvent(e).equals(Operation.HandleStatus.NOT_HANDLED)) {
                LOG.warn("{}weird unhandled:{} for op:{}", new Object[]{logPrefix, e.getId(), msgI.getValue0()});
                return;
            }
            processOp(op);
        }
    };

    Handler handleCCTimeout = new Handler<CCOpTimeout>() {
        @Override
        public void handle(CCOpTimeout e) {
            LOG.trace("{}timeout:{}", logPrefix, e.opReq);
            Pair<Identifier, Integer> msgI = pendingMsgs.remove(e.getId());
            if (msgI == null) {
                LOG.warn("{}weird timeout:{}", logPrefix, msgI.getValue0());
                return;
            }
            if(msgI.getValue1() == 0) {
                LOG.info("{}op:{} timed out", logPrefix, e.getId());
                Operation op = activeOps.get(msgI.getValue0());
                if(op == null) {
                    LOG.warn("{}weird timeout:{}", logPrefix, msgI.getValue0());
                    return;
                }
                op.timeout(e.getId());
                processOp(op);
                return;
            }
            pendingMsgs.put(e.getId(), Pair.with(msgI.getValue0(), msgI.getValue1()-1));
            LOG.trace("{}sending:{}", logPrefix, e.opReq);
            trigger(e.opReq, ccPort);
        }
    };

    private void startOp(Operation op) {
        activeOps.put(op.getId(), op);
        op.start();
        processOp(op);
    }

    private void processOp(Operation op) {
        switch (op.getStatus()) {
            case ONGOING:
                for (Map.Entry<CCOpRequest, Boolean> e : op.sendingQueue().entrySet()) {
                    LOG.trace("{}sending:{}", logPrefix, e.getKey());
                    trigger(e.getKey(), ccPort);
                    if (e.getValue()) {
                        pendingMsgs.put(e.getKey().getId(), Pair.with(op.getId(), ccConfig.retries));
                    }
                }
            break;
            case DONE:
                for (Map.Entry<CCOpRequest, Boolean> e : op.sendingQueue().entrySet()) {
                    LOG.trace("{}sending:{}", logPrefix, e.getKey());
                    trigger(e.getKey(), ccPort);
                    if (e.getValue()) {
                        LOG.warn("{}operation done, should not send replyable message:{} at this state", logPrefix, e.getKey().opReq);
                    }
                }
                cleanOp(op.getId());
                KompicsEvent resp = op.getResult();
                LOG.trace("{}sending:{}", logPrefix, resp);
                trigger(resp, vodCCPort);
        }
    }

    private void cleanOp(Identifier opId) {
        activeOps.remove(opId);
        Iterator<Map.Entry<Identifier, Pair<Identifier, Integer>>> msgIt = pendingMsgs.entrySet().iterator();
        while (msgIt.hasNext()) {
            Map.Entry<Identifier, Pair<Identifier, Integer>> pendingMsg = msgIt.next();
            if (pendingMsg.getValue().getValue0().equals(opId)) {
                msgIt.remove();
            }
        }
    }

    public static class VoDCaracalClientInit extends Init<VoDCaracalClientComp> {

        public final byte[] schemaId;
        public final VoDCaracalClientConfig ccConfig;

        public VoDCaracalClientInit(VoDCaracalClientConfig ccConfig, byte[] schemaId) {
            this.schemaId = schemaId;
            this.ccConfig = ccConfig;
        }
    }
}
