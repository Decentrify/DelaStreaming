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

import java.net.InetAddress;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;
import se.sics.dela.network.conn.NetConnComp;
import se.sics.dela.network.conn.NetConnEvents;
import se.sics.dela.network.conn.NetConnPartnerIdExtractorV2;
import se.sics.dela.network.conn.NetConnPort;
import se.sics.dela.network.ledbat.LedbatEvent;
import se.sics.dela.network.ledbat.LedbatMsg;
import se.sics.dela.network.ledbat.LedbatReceiverPort;
import se.sics.dela.network.ledbat.LedbatRivuletIdExtractorV2;
import se.sics.dela.network.ledbat.LedbatSenderPort;
import se.sics.dela.workers.ctrl.util.NxStackEventIdExtractors;
import se.sics.dela.workers.ctrl.util.NxStackMsgIdExtractors;
import se.sics.dela.workers.ctrl.util.NxStackTypeIdExtractor;
import se.sics.dela.workers.ctrl.util.ReceiverTaskComp;
import se.sics.dela.workers.ctrl.util.SenderTaskComp;
import se.sics.dela.workers.ctrl.util.WorkerStackType;
import se.sics.kompics.Channel;
import se.sics.kompics.Component;
import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.Handler;
import se.sics.kompics.Init;
import se.sics.kompics.PortType;
import se.sics.kompics.Start;
import se.sics.kompics.timer.Timer;
import se.sics.kompics.timer.java.JavaTimer;
import se.sics.kompics.util.Identifier;
import se.sics.ktoolbox.netmngr.NetworkConfig;
import se.sics.ktoolbox.netmngr.driver.NxNetProxy;
import se.sics.ktoolbox.nutil.conn.ConnConfig;
import se.sics.ktoolbox.nutil.conn.workers.WorkCtrlCenterComp;
import se.sics.ktoolbox.nutil.conn.workers.WorkCtrlCenterPort;
import se.sics.ktoolbox.nutil.network.portsv2.EventIdExtractorV2;
import se.sics.ktoolbox.nutil.network.portsv2.EventTypeExtractorsV2;
import se.sics.ktoolbox.nutil.network.portsv2.MsgIdExtractorV2;
import se.sics.ktoolbox.nutil.network.portsv2.One2NEventChannelV2;
import se.sics.ktoolbox.nutil.nxcomp.NxMngrEvents;
import se.sics.ktoolbox.nutil.nxcomp.NxMngrPort;
import se.sics.ktoolbox.nutil.nxcomp.v2.NxMngrCompV2;
import se.sics.ktoolbox.nutil.nxcomp.v2.NxStackDefinitionV2;
import se.sics.ktoolbox.nutil.timer.TimerProxyImpl;
import se.sics.ktoolbox.util.config.impl.SystemConfig;
import se.sics.ktoolbox.util.identifiable.BasicIdentifiers;
import se.sics.ktoolbox.util.identifiable.IdentifierFactory;
import se.sics.ktoolbox.util.identifiable.IdentifierRegistryV2;
import se.sics.ktoolbox.util.identifiable.basic.SimpleByteIdFactory;
import se.sics.ktoolbox.util.network.KAddress;
import se.sics.ktoolbox.util.network.basic.BasicAddress;
import se.sics.ktoolbox.util.trysf.Try;
import se.sics.ktoolbox.util.trysf.TryHelper;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class WorkCtrlHostComp extends ComponentDefinition {

  private Component timerComp;
  private NxNetProxy nxNetProxy;
  private Component ctrlCenter;
  private Component ctrlDriver;
  private Component senderTasks;
  private Component receiverTasks;
  private Component netConnMngr;
  private One2NEventChannelV2 nxStackChannel;

  private SystemConfig systemConfig;
  private NetworkConfig networkConfig;
  private WorkCtrlConfig ctrlConfig;
  private KAddress selfAdr;

  private final IdentifierFactory eventIds;
  private final SimpleByteIdFactory nxStackIds;

  public WorkCtrlHostComp() {
    eventIds = IdentifierRegistryV2.instance(BasicIdentifiers.Values.EVENT, Optional.of(1234l));
    nxStackIds = new SimpleByteIdFactory(Optional.empty(), 0);
    subscribe(handleStart, control);
  }

  Handler handleStart = new Handler<Start>() {
    @Override
    public void handle(Start event) {
      readConfig();
      timerComp = create(JavaTimer.class, Init.NONE);
      nxNetProxy = new NxNetProxy().setup(proxy, logger, networkConfig, eventIds, privateIpDetected());

      trigger(Start.event, timerComp.control());
      nxNetProxy.start();
    }
  };

  private void readConfig() {
    new Try.Success(true)
      .flatMap(TryHelper.tryFSucc0(() -> SystemConfig.instance(config())))
      .flatMap(TryHelper.tryCTSucc1((SystemConfig conf) -> systemConfig = conf))
      .flatMap(TryHelper.tryFSucc0(() -> NetworkConfig.instance(config())))
      .flatMap(TryHelper.tryCTSucc1((NetworkConfig conf) -> networkConfig = conf))
      .flatMap(TryHelper.tryFSucc0(() -> WorkCtrlConfig.instance(config())))
      .flatMap(TryHelper.tryCTSucc1((WorkCtrlConfig conf) -> ctrlConfig = conf))
      .recoverWith(TryHelper.tryCTFail((Throwable cause) -> {
        throw new RuntimeException(cause);
      }));
  }

  private Consumer<InetAddress> privateIpDetected() {
    return (privateIp) -> {
      selfAdr = new BasicAddress(privateIp, systemConfig.port, systemConfig.id);
      nxNetProxy.bind(selfAdr, networkReady());
    };
  }

  private Consumer<Boolean> networkReady() {
    return (_ignore) -> {
      ConnConfig connConfig = new ConnConfig(ctrlConfig.updatePeriod);
      ctrlCenter = create(WorkCtrlCenterComp.class, new WorkCtrlCenterComp.Init(selfAdr,
        ctrlConfig.overlayId, ctrlConfig.batchId, ctrlConfig.baseId, connConfig, ctrlConfig.mngrAdr));
      nxNetProxy.connect(ctrlCenter);
      connect(timerComp.getPositive(Timer.class), ctrlCenter.getNegative(Timer.class), Channel.TWO_WAY);

      ctrlDriver = create(WorkCtrlDriverComp.class, new WorkCtrlDriverComp.Init(selfAdr));
      connect(timerComp.getPositive(Timer.class), ctrlDriver.getNegative(Timer.class), Channel.TWO_WAY);
      connect(ctrlCenter.getPositive(WorkCtrlCenterPort.class), ctrlDriver.getNegative(WorkCtrlCenterPort.class),
        Channel.TWO_WAY);

      createNxStackChannel();
      createNetConnMngr();
      createTaskMngrReceiver();
      createTaskMngrSender();
      trigger(Start.event, ctrlCenter.control());
      trigger(Start.event, ctrlDriver.control());
      trigger(Start.event, netConnMngr.control());
      trigger(Start.event, receiverTasks.control());
      trigger(Start.event, senderTasks.control());
    };
  }

  private void createNxStackChannel() {
    Map<String, EventIdExtractorV2> channelSelectors = new HashMap<>();
    channelSelectors.put(NxMngrEvents.EVENT_TYPE, new NxStackTypeIdExtractor());
    nxStackChannel = One2NEventChannelV2.getChannel("worker nx stacks channel", logger,
      ctrlDriver.getNegative(NxMngrPort.class), new EventTypeExtractorsV2.Base(), channelSelectors);
  }

  
  private void createNetConnMngr() {
    Identifier netConnStackId = WorkerStackType.NET_CONN.getId(nxStackIds);
    
    List<Class<PortType>> negativePorts = new LinkedList<>();
    List<Map<String, EventIdExtractorV2>> negativeEvents = new LinkedList<>();
    List<Class<PortType>> positivePorts = new LinkedList<>();
    List<Map<String, EventIdExtractorV2>> positiveEvents = new LinkedList<>();
    
    Map<String, MsgIdExtractorV2> positiveNetworkMsgs = new HashMap<>();
    positiveNetworkMsgs.put(LedbatMsg.MSG_TYPE, new NxStackMsgIdExtractors.Source(netConnStackId));
    NxStackDefinitionV2.NetworkPort posNetPort = new NxStackDefinitionV2.NetworkPort(positiveNetworkMsgs);
    
    positivePorts.add((Class) Timer.class);
    Map<String, EventIdExtractorV2> timerEvents = new HashMap<>();
    timerEvents.put(TimerProxyImpl.Timeout.EVENT_TYPE, new NxStackEventIdExtractors.TimerProxy(netConnStackId));
    positiveEvents.add(timerEvents);
    
    negativePorts.add((Class) NetConnPort.class);
    Map<String, EventIdExtractorV2> netConnEvents = new HashMap<>();
    netConnEvents.put(NetConnEvents.EVENT_TYPE, new NetConnPartnerIdExtractorV2());
    negativeEvents.add(netConnEvents);
    
    negativePorts.add((Class) LedbatSenderPort.class);
    Map<String, EventIdExtractorV2> ledbatSenderEvents = new HashMap<>();
    ledbatSenderEvents.put(LedbatEvent.EVENT_TYPE, new LedbatRivuletIdExtractorV2());
    negativeEvents.add(ledbatSenderEvents);
    
    negativePorts.add((Class) LedbatReceiverPort.class);
    Map<String, EventIdExtractorV2> ledbatReceiverEvents = new HashMap<>();
    ledbatReceiverEvents.put(LedbatEvent.EVENT_TYPE, new LedbatRivuletIdExtractorV2());
    negativeEvents.add(ledbatReceiverEvents);
    
    NxStackDefinitionV2 stackDefintion = new NxStackDefinitionV2.OneComp<>(NetConnComp.class);
    netConnMngr = create(NxMngrCompV2.class, new NxMngrCompV2.Init("net conn", stackDefintion, negativePorts, negativeEvents,
      positivePorts, positiveEvents, Optional.of(posNetPort)));
    nxNetProxy.connect(netConnMngr);
    connect(netConnMngr.getNegative(Timer.class), timerComp.getPositive(Timer.class), Channel.TWO_WAY);
    nxStackChannel.addChannel(WorkerStackType.NET_CONN.getId(nxStackIds), netConnMngr.getPositive(NxMngrPort.class));
    connect(ctrlDriver.getNegative(NetConnPort.class), netConnMngr.getPositive(NetConnPort.class), Channel.TWO_WAY);
  }
  
  private void createTaskMngrReceiver() {
    Identifier ledbatReceiverStackId = WorkerStackType.RECEIVER.getId(nxStackIds);
    
    List<Class<PortType>> negativePorts = new LinkedList<>();
    List<Map<String, EventIdExtractorV2>> negativeIdExtractors = new LinkedList<>();
    List<Class<PortType>> positivePorts = new LinkedList<>();
    List<Map<String, EventIdExtractorV2>> positiveIdExtractors = new LinkedList<>();
    
    positivePorts.add((Class) LedbatReceiverPort.class);
    Map<String, EventIdExtractorV2> ledbatReceiverEvents = new HashMap<>();
    ledbatReceiverEvents.put(LedbatEvent.EVENT_TYPE, new LedbatRivuletIdExtractorV2());
    positiveIdExtractors.add(ledbatReceiverEvents);
    
    positivePorts.add((Class) Timer.class);
    Map<String, EventIdExtractorV2> timerEvents = new HashMap<>();
    timerEvents.put(TimerProxyImpl.Timeout.EVENT_TYPE, new NxStackEventIdExtractors.TimerProxy(ledbatReceiverStackId));
    positiveIdExtractors.add(timerEvents);
    
    NxStackDefinitionV2 stackDefintion = new NxStackDefinitionV2.OneComp<>(ReceiverTaskComp.class);
    receiverTasks = create(NxMngrCompV2.class, new NxMngrCompV2.Init("task mngr receiver", stackDefintion, negativePorts, negativeIdExtractors,
      positivePorts, positiveIdExtractors, Optional.empty()));
    connect(receiverTasks.getNegative(Timer.class), timerComp.getPositive(Timer.class), Channel.TWO_WAY);
    nxStackChannel.addChannel(WorkerStackType.RECEIVER.getId(nxStackIds), receiverTasks.getPositive(NxMngrPort.class));
    connect(receiverTasks.getNegative(LedbatReceiverPort.class), netConnMngr.getPositive(LedbatReceiverPort.class), 
      Channel.TWO_WAY);
  }

  private void createTaskMngrSender() {
    Identifier ledbatSenderStackId = WorkerStackType.SENDER.getId(nxStackIds);
    
    List<Class<PortType>> negativePorts = new LinkedList<>();
    List<Map<String, EventIdExtractorV2>> negativeIdExtractors = new LinkedList<>();
    List<Class<PortType>> positivePorts = new LinkedList<>();
    List<Map<String, EventIdExtractorV2>> positiveIdExtractors = new LinkedList<>();
    
    positivePorts.add((Class) LedbatSenderPort.class);
    Map<String, EventIdExtractorV2> ledbatSenderEvents = new HashMap<>();
    ledbatSenderEvents.put(LedbatEvent.EVENT_TYPE, new LedbatRivuletIdExtractorV2());
    positiveIdExtractors.add(ledbatSenderEvents);
    
    positivePorts.add((Class) Timer.class);
    Map<String, EventIdExtractorV2> timerEvents = new HashMap<>();
    timerEvents.put(TimerProxyImpl.Timeout.EVENT_TYPE, new NxStackEventIdExtractors.TimerProxy(ledbatSenderStackId));
    positiveIdExtractors.add(timerEvents);
    
    NxStackDefinitionV2 stackDefintion = new NxStackDefinitionV2.OneComp<>(SenderTaskComp.class);
    senderTasks = create(NxMngrCompV2.class, new NxMngrCompV2.Init("task mngr sender", stackDefintion, 
      negativePorts, negativeIdExtractors, positivePorts, positiveIdExtractors, Optional.empty()));
    connect(senderTasks.getNegative(Timer.class), timerComp.getPositive(Timer.class), Channel.TWO_WAY);
    nxStackChannel.addChannel(WorkerStackType.SENDER.getId(nxStackIds), senderTasks.getPositive(NxMngrPort.class));
    connect(senderTasks.getNegative(LedbatSenderPort.class), netConnMngr.getPositive(LedbatSenderPort.class), 
      Channel.TWO_WAY);
  }
}
