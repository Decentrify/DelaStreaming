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

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import se.sics.dela.network.conn.NetConnComp;
import se.sics.dela.network.conn.NetConnEvents;
import se.sics.dela.network.conn.NetConnPort;
import se.sics.dela.network.util.ChannelId;
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
import se.sics.ktoolbox.util.identifiable.basic.PairIdentifier;
import se.sics.ktoolbox.util.identifiable.basic.SimpleByteIdFactory;
import se.sics.ktoolbox.util.network.KAddress;

/**
 *
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class WorkCtrlDriverComp extends ComponentDefinition {

  Positive<NxMngrPort> nxStackMngr = requires(NxMngrPort.class);
  Positive<NetConnPort> netConnMngr = requires(NetConnPort.class);
  Positive<WorkCtrlCenterPort> appPort = requires(WorkCtrlCenterPort.class);
  Positive<Timer> timerPort = requires(Timer.class);
  TimerProxy timer;

  private final KAddress selfAdr;

  private final IdentifierFactory eventIds;
  private final Identifier receiverStackId;
  private final Identifier senderStackId;
  private final Identifier netConnStackId;

  //<taskId, task>
  private final Map<Identifier, Task> tasks = new HashMap<>();
  //<createReqId, taskId>
  private final Map<Identifier, Identifier> pendingNxCreateAck = new HashMap<>();
  //<createReqId, taskId>
  private final Map<Identifier, Identifier> pendingLedbatAck = new HashMap<>();

  public WorkCtrlDriverComp(Init init) {
    this.selfAdr = init.selfAdr;
    this.timer = new TimerProxyImpl().setup(proxy, logger);

    eventIds = IdentifierRegistryV2.instance(BasicIdentifiers.Values.EVENT, Optional.of(1234l));
    SimpleByteIdFactory stackIds = new SimpleByteIdFactory(Optional.empty(), 0);
    receiverStackId = WorkerStackType.RECEIVER.getId(stackIds);
    senderStackId = WorkerStackType.SENDER.getId(stackIds);
    netConnStackId = WorkerStackType.NET_CONN.getId(stackIds);

    subscribe(handleStart, control);
    subscribe(handleNewTask, appPort);
    subscribe(handleTaskCtrlStarted, nxStackMngr);
    subscribe(handleLedbatSenderStarted, netConnMngr);
    subscribe(handleLedbatReceiverStarted, netConnMngr);
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
      if (req.task instanceof DelaWorkTask.LReceiver) {
        ReceiverTask task = new ReceiverTask((DelaWorkTask.LReceiver) req.task);
        tasks.put(task.task.taskId, task);
        task.start();
      } else if (req.task instanceof DelaWorkTask.LSender) {
        SenderTask task = new SenderTask((DelaWorkTask.LSender) req.task);
        tasks.put(task.task.taskId, task);
        task.start();
      } else {
        throw new RuntimeException("unknown task");
      }
    }
  };

  Handler handleTaskCtrlStarted = new Handler<NxMngrEvents.CreateAck>() {
    @Override
    public void handle(NxMngrEvents.CreateAck ack) {
      logger.info("{} started", ack.getId());
      Identifier taskId = pendingNxCreateAck.remove(ack.getId());
      if (taskId == null) {
        throw new RuntimeException("no task");
      }
      Task task = tasks.get(taskId);
      task.nxCreated(ack);
    }
  };

  Handler handleLedbatSenderStarted = new Handler<NetConnEvents.LedbatSenderCreateAck>() {
    @Override
    public void handle(NetConnEvents.LedbatSenderCreateAck ack) {
      ledbatStarted(ack.getId());
    }
  };

  Handler handleLedbatReceiverStarted = new Handler<NetConnEvents.LedbatReceiverCreateAck>() {
    @Override
    public void handle(NetConnEvents.LedbatReceiverCreateAck ack) {
      ledbatStarted(ack.getId());
    }
  };

  private void ledbatStarted(Identifier reqId) {
    logger.info("{} started", reqId);
    Identifier taskId = pendingLedbatAck.remove(reqId);
    if (taskId == null) {
      throw new RuntimeException("no task");
    }
    Task task = tasks.get(taskId);
    task.ledbatStarted();
  }

  interface Task {

    public void start();

    public void nxCreated(NxMngrEvents.CreateAck ack);

    public void ledbatStarted();
  }

  class SenderTask implements Task {

    final DelaWorkTask.LSender task;
    NxMngrEvents.CreateReq netConnCreateReq;
    boolean netConnCreateAck = false;
    NxMngrEvents.CreateReq senderCtrlCreateReq;
    boolean senderCtrlCreateAck = false;
    NetConnEvents.LedbatSenderCreate ledbatSenderCreateReq;
    boolean ledbatSenderCreateAck = false;

    final ChannelId channelId;

    public SenderTask(DelaWorkTask.LSender task) {
      this.task = task;
      Identifier ctrlId = new NxStackId(senderStackId, task.dataId);
      Identifier sender = new NxStackId(netConnStackId, selfAdr.getId());
      Identifier receiver = new NxStackId(netConnStackId, task.receiver.getId());
      channelId = new ChannelId(ctrlId, sender, receiver);
    }

    //
    @Override
    public void start() {
      createNetConnComp();
    }

    @Override
    public void nxCreated(NxMngrEvents.CreateAck ack) {
      if (ack.getId().equals(netConnCreateReq.getId())) {
        netConnCreateAck = true;
        createLedbatSender();
      } else if (ack.getId().equals(senderCtrlCreateReq.getId())) {
        senderCtrlCreateAck = true;
      }
      checkIfReady();
    }

    @Override
    public void ledbatStarted() {
      ledbatSenderCreateAck = true;
      createSenderCtrlComp();
    }

    void createNetConnComp() {
      NetConnComp.Init init = new NetConnComp.Init(selfAdr, task.receiver);
      NxStackInit initWrapper = new NxStackInit.OneComp<>(init);
      netConnCreateReq = new NxMngrEvents.CreateReq(eventIds.randomId(), channelId.receiverId(), initWrapper);
      logger.info("netconn:{} starting", netConnCreateReq.eventId);
      trigger(netConnCreateReq, nxStackMngr);
      pendingNxCreateAck.put(netConnCreateReq.getId(), task.taskId);
    }

    void createSenderCtrlComp() {
      Identifier rivuletId = new PairIdentifier(selfAdr.getId(), task.receiver.getId());
      SenderTaskComp.Init init = new SenderTaskComp.Init(selfAdr, task.receiver, task.dataId, rivuletId);
      NxStackInit initWrapper = new NxStackInit.OneComp<>(init);
      senderCtrlCreateReq = new NxMngrEvents.CreateReq(eventIds.randomId(), channelId.dataId(), initWrapper);
      logger.info("sender:{} starting", senderCtrlCreateReq.eventId);
      trigger(senderCtrlCreateReq, nxStackMngr);
      pendingNxCreateAck.put(senderCtrlCreateReq.getId(), task.taskId);
    }

    void createLedbatSender() {
      ledbatSenderCreateReq = new NetConnEvents.LedbatSenderCreate(eventIds.randomId(), channelId);
      logger.info("ledbat sender:{} starting", ledbatSenderCreateReq.getId());
      trigger(ledbatSenderCreateReq, netConnMngr);
      pendingLedbatAck.put(ledbatSenderCreateReq.getId(), task.taskId);
    }

    private void checkIfReady() {
      if (netConnCreateAck && ledbatSenderCreateAck && senderCtrlCreateAck) {
        logger.info("ready");
      }
    }
  }

  class ReceiverTask implements Task {

    final DelaWorkTask.LReceiver task;
    NxMngrEvents.CreateReq netConnCreateReq;
    boolean netConnCreateAck = false;
    NxMngrEvents.CreateReq receiverCtrlCreateReq;
    boolean receiverCtrlCreateAck = false;
    NetConnEvents.LedbatReceiverCreate ledbatReceiverCreateReq;
    boolean ledbatReceiverCreateAck = false;

    final ChannelId channelId;

    public ReceiverTask(DelaWorkTask.LReceiver task) {
      this.task = task;
      Identifier ctrlId = new NxStackId(receiverStackId, task.dataId);
      Identifier sender = new NxStackId(netConnStackId, task.sender.getId());
      Identifier receiver = new NxStackId(netConnStackId, selfAdr.getId());
      channelId = new ChannelId(ctrlId, sender, receiver);
    }

    @Override
    public void start() {
      createNetConnComp();
    }

    @Override
    public void nxCreated(NxMngrEvents.CreateAck ack) {
      if (ack.getId().equals(netConnCreateReq.getId())) {
        netConnCreateAck = true;
        createLedbatSender();
      } else if (ack.getId().equals(receiverCtrlCreateReq.getId())) {
        receiverCtrlCreateAck = true;
      }
      checkIfReady();
    }

    @Override
    public void ledbatStarted() {
      ledbatReceiverCreateAck = true;
      createReceiverCtrlComp();
    }

    void createNetConnComp() {
      NetConnComp.Init init = new NetConnComp.Init(selfAdr, task.sender);
      NxStackInit initWrapper = new NxStackInit.OneComp<>(init);
      netConnCreateReq = new NxMngrEvents.CreateReq(eventIds.randomId(), channelId.senderId(), initWrapper);
      logger.info("netconn:{} starting", netConnCreateReq.eventId);
      trigger(netConnCreateReq, nxStackMngr);
      pendingNxCreateAck.put(netConnCreateReq.getId(), task.taskId);
    }

    void createReceiverCtrlComp() {
      Identifier rivuletId = new PairIdentifier(task.sender.getId(), selfAdr.getId());
      ReceiverTaskComp.Init init = new ReceiverTaskComp.Init(selfAdr, task.sender, task.dataId, rivuletId);
      NxStackInit initWrapper = new NxStackInit.OneComp<>(init);
      receiverCtrlCreateReq = new NxMngrEvents.CreateReq(eventIds.randomId(), channelId.dataId(), initWrapper);
      logger.info("receiver:{} starting", receiverCtrlCreateReq.eventId);
      trigger(receiverCtrlCreateReq, nxStackMngr);
      pendingNxCreateAck.put(receiverCtrlCreateReq.getId(), task.taskId);
    }

    void createLedbatSender() {
      ledbatReceiverCreateReq = new NetConnEvents.LedbatReceiverCreate(eventIds.randomId(), channelId);
      logger.info("ledbat receiver:{} starting", ledbatReceiverCreateReq.getId());
      trigger(ledbatReceiverCreateReq, netConnMngr);
      pendingLedbatAck.put(ledbatReceiverCreateReq.getId(), task.taskId);
    }

    private void checkIfReady() {
      if (netConnCreateAck && ledbatReceiverCreateAck && receiverCtrlCreateAck) {
        logger.info("ready");
      }
    }
  }

  public static class Init extends se.sics.kompics.Init<WorkCtrlDriverComp> {

    public final KAddress selfAdr;

    public Init(KAddress self) {
      this.selfAdr = self;
    }
  }
}
