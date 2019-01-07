/*
 * Copyright (C) 2009 Swedish Institute of Computer Science (SICS) Copyright (C)
 * 2009 Royal Institute of Technology (KTH)
 *
 * KompicsToolbox is free software; you can redistribute it and/or
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
package se.sics.dela.workers.ctrl;

import java.util.Optional;
import se.sics.dela.network.conn.NetConnPort;
import se.sics.dela.workers.ctrl.util.ReceiverTaskComp;
import se.sics.dela.workers.ctrl.util.SenderTaskComp;
import se.sics.dela.workers.ctrl.util.WorkerStackType;
import se.sics.dela.workers.task.DelaWorkTask;
import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.Handler;
import se.sics.kompics.Positive;
import se.sics.kompics.Start;
import se.sics.kompics.timer.Timer;
import se.sics.kompics.util.Identifier;
import se.sics.ktoolbox.nutil.conn.workers.WorkCtrlCenterEvents;
import se.sics.ktoolbox.nutil.conn.workers.WorkCtrlCenterPort;
import se.sics.ktoolbox.nutil.nxcomp.NxMngrEvents;
import se.sics.ktoolbox.nutil.nxcomp.NxMngrPort;
import se.sics.ktoolbox.nutil.nxcomp.NxStackId;
import se.sics.ktoolbox.nutil.nxcomp.NxStackInit;
import se.sics.ktoolbox.nutil.timer.TimerProxy;
import se.sics.ktoolbox.nutil.timer.TimerProxyImpl;
import se.sics.ktoolbox.util.identifiable.BasicIdentifiers;
import se.sics.ktoolbox.util.identifiable.IdentifierFactory;
import se.sics.ktoolbox.util.identifiable.IdentifierRegistryV2;
import se.sics.ktoolbox.util.identifiable.basic.SimpleByteIdFactory;
import se.sics.ktoolbox.util.network.KAddress;

/**
 *
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class WorkCtrlDriverComp extends ComponentDefinition {

  Positive<NxMngrPort> taskMngr = requires(NxMngrPort.class);
  Positive<NetConnPort> netConnMngr = requires(NetConnPort.class);
  Positive<WorkCtrlCenterPort> appPort = requires(WorkCtrlCenterPort.class);
  Positive<Timer> timerPort = requires(Timer.class);
  TimerProxy timer;

  private final KAddress selfAdr;

  private final IdentifierFactory eventIds;
  private final Identifier receiverStackId;
  private final Identifier senderStackId;

  public WorkCtrlDriverComp(Init init) {
    this.selfAdr = init.selfAdr;
    this.timer = new TimerProxyImpl().setup(proxy, logger);
    
    eventIds = IdentifierRegistryV2.instance(BasicIdentifiers.Values.EVENT, Optional.of(1234l));
    SimpleByteIdFactory stackIds = new SimpleByteIdFactory(Optional.empty(), 0);
    receiverStackId = WorkerStackType.RECEIVER.getId(stackIds);
    senderStackId = WorkerStackType.SENDER.getId(stackIds);
    
    subscribe(handleStart, control);
    subscribe(handleNewTask, appPort);
    subscribe(handleTaskStarted, taskMngr);
  }

  Handler handleStart = new Handler<Start>() {
    @Override
    public void handle(Start event) {
    }
  };

  Handler handleNewTask = new Handler<WorkCtrlCenterEvents.NewTask>() {
    @Override
    public void handle(WorkCtrlCenterEvents.NewTask req) {
      logger.info("task:{}", req.task);
      NxMngrEvents.CreateReq createReq;
      if (req.task instanceof DelaWorkTask.LReceiver) {
        DelaWorkTask.LReceiver task = (DelaWorkTask.LReceiver) req.task;
        ReceiverTaskComp.Init init = new ReceiverTaskComp.Init(selfAdr);
        NxStackInit initWrapper = new NxStackInit.OneComp<>(init);
        Identifier stackId = new NxStackId(receiverStackId, eventIds.randomId());
        createReq = new NxMngrEvents.CreateReq(eventIds.randomId(), stackId, initWrapper);
      } else if (req.task instanceof DelaWorkTask.LSender) {
        DelaWorkTask.LSender task = (DelaWorkTask.LSender) req.task;
        SenderTaskComp.Init init = new SenderTaskComp.Init(selfAdr, task.receiver);
        NxStackInit initWrapper = new NxStackInit.OneComp<>(init);
        Identifier stackId = new NxStackId(senderStackId, eventIds.randomId());
        createReq = new NxMngrEvents.CreateReq(eventIds.randomId(), stackId, initWrapper);
      } else {
        throw new RuntimeException("unknown task");
      }
      logger.info("task:{} starting", createReq.eventId);
      trigger(createReq, taskMngr);
    }
  };
  
  Handler handleTaskStarted = new Handler<NxMngrEvents.CreateAck>() {
    @Override
    public void handle(NxMngrEvents.CreateAck event) {
      logger.info("task:{} started", event.req.eventId);
    }
  };

  public static class Init extends se.sics.kompics.Init<WorkCtrlDriverComp> {

    public final KAddress selfAdr;

    public Init(KAddress self) {
      this.selfAdr = self;
    }
  }
}
