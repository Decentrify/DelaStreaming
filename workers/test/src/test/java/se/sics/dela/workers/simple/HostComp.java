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
package se.sics.dela.workers.simple;

import com.typesafe.config.ConfigFactory;
import java.io.File;
import java.util.HashMap;
import java.util.Map;
import se.sics.dela.workers.ctrl.WorkCtrlConfig;
import se.sics.dela.workers.ctrl.WorkCtrlDriverComp;
import se.sics.dela.workers.mngr.WorkMngrConfig;
import se.sics.dela.workers.mngr.WorkMngrDriverComp;
import se.sics.dela.workers.util.NetworkEmulator;
import se.sics.kompics.Channel;
import se.sics.kompics.Component;
import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.Handler;
import se.sics.kompics.Start;
import se.sics.kompics.config.Config;
import se.sics.kompics.config.TypesafeConfig;
import se.sics.kompics.network.Network;
import se.sics.kompics.timer.Timer;
import se.sics.kompics.timer.java.JavaTimer;
import se.sics.ktoolbox.nutil.conn.ConnConfig;
import se.sics.ktoolbox.nutil.conn.ConnIds;
import se.sics.ktoolbox.nutil.conn.ConnIds.InstanceId;
import se.sics.ktoolbox.nutil.conn.ConnMsgs;
import se.sics.ktoolbox.nutil.conn.workers.WorkCtrlCenterComp;
import se.sics.ktoolbox.nutil.conn.workers.WorkCtrlCenterPort;
import se.sics.ktoolbox.nutil.conn.workers.WorkMngrCenterComp;
import se.sics.ktoolbox.nutil.conn.workers.WorkMngrCenterPort;
import se.sics.ktoolbox.nutil.conn.workers.WorkMsgs;
import se.sics.ktoolbox.nutil.network.portsv2.MsgIdExtractorV2;
import se.sics.ktoolbox.nutil.network.portsv2.MsgIdExtractorsV2;
import se.sics.ktoolbox.nutil.network.portsv2.MsgTypeExtractorsV2;
import se.sics.ktoolbox.nutil.network.portsv2.OutgoingOne2NMsgChannelV2;
import se.sics.ktoolbox.util.identifiable.IdentifierRegistryV2;
import se.sics.ktoolbox.util.network.KAddress;
import se.sics.ktoolbox.util.trysf.Try;
import se.sics.ktoolbox.util.trysf.TryHelper;

/**
 *
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class HostComp extends ComponentDefinition {

  private final Init init;
  private Component timerComp;
  private Component networkComp;
  private Component workMngrComp;
  private Component workCtrlComp1;
  private Component workCtrlComp2;
  private Component workMngrDriverComp;
  private Component workCtrlDriverComp1;
  private Component workCtrlDriverComp2;
  
  private OutgoingOne2NMsgChannelV2 networkChannel;

  public HostComp(Init init) {
    this.init = init;
    subscribe(handleStart, control);
  }

  Handler handleStart = new Handler<Start>() {
    @Override
    public void handle(Start event) {
      timerComp = create(JavaTimer.class, Init.NONE);
      networkComp = create(NetworkEmulator.class, Init.NONE);
      Map<String, MsgIdExtractorV2> channelSelectors = new HashMap<>();
      channelSelectors.put(ConnMsgs.MSG_TYPE, new MsgIdExtractorsV2.Destination<>());
      channelSelectors.put(WorkMsgs.MSG_TYPE, new MsgIdExtractorsV2.Destination<>());
      networkChannel = OutgoingOne2NMsgChannelV2.getChannel("test-host-channel", logger,
        networkComp.getPositive(Network.class), new MsgTypeExtractorsV2.Base(), channelSelectors);

      connectWorkMngr();
      connectWorkCtrl1();
      connectWorkCtrl2();
      
      trigger(Start.event, timerComp.control());
      trigger(Start.event, networkComp.control());
      startWorkMngr();
      startWorkCtrl1();
      startWorkCtrl2();
    }
  };

  private WorkMngrConfig readWorkMngrConfig() {
    Config config = TypesafeConfig.load(ConfigFactory.parseFile(new File(init.workMngrConfigFile)));
    
    Try<WorkMngrConfig> aux = WorkMngrConfig.instance(config);
    if(aux.isSuccess()) {
      logger.info("mngr config:{}", aux.get());
      return aux.get();
    } else {
      throw new RuntimeException(TryHelper.tryError(aux));
    }
  }

  private void connectWorkMngr() {
    WorkMngrConfig workMngrConfig = readWorkMngrConfig();
    ConnConfig connConfig = new ConnConfig(workMngrConfig.updatePeriod);
    workMngrComp = create(WorkMngrCenterComp.class, new WorkMngrCenterComp.Init(init.workMngrAdr,
      workMngrConfig.overlayId, workMngrConfig.batchId, workMngrConfig.baseId, connConfig));
    networkChannel.addChannel(init.workMngrAdr.getId(), workMngrComp.getNegative(Network.class));
    connect(timerComp.getPositive(Timer.class), workMngrComp.getNegative(Timer.class), Channel.TWO_WAY);

    workMngrDriverComp = create(WorkMngrDriverComp.class, new WorkMngrDriverComp.Init(init.workMngrAdr));
    connect(workMngrComp.getPositive(WorkMngrCenterPort.class), workMngrDriverComp.getNegative(WorkMngrCenterPort.class),
      Channel.TWO_WAY);
  }
  
  private void startWorkMngr() {
    trigger(Start.event, workMngrComp.control());
    trigger(Start.event, workMngrDriverComp.control());
  }
  
  private WorkCtrlConfig readWorkCtrlConfig1() {
    Config config = TypesafeConfig.load(ConfigFactory.parseFile(new File(init.workCtrlConfigFile1)));
    
    Try<WorkCtrlConfig> aux = WorkCtrlConfig.instance(config);
    if(aux.isSuccess()) {
      logger.info("ctrl1 config:{}", aux.get());
      return aux.get();
    } else {
      throw new RuntimeException(TryHelper.tryError(aux));
    }
  }

  private void connectWorkCtrl1() {
    WorkCtrlConfig workMngrConfig = readWorkCtrlConfig1();
    ConnConfig connConfig = new ConnConfig(workMngrConfig.updatePeriod);
    workCtrlComp1 = create(WorkCtrlCenterComp.class, new WorkCtrlCenterComp.Init(init.workCtrlAdr1,
      workMngrConfig.overlayId, workMngrConfig.batchId, workMngrConfig.baseId, connConfig, init.workMngrAdr));
    networkChannel.addChannel(init.workCtrlAdr1.getId(), workCtrlComp1.getNegative(Network.class));
    connect(timerComp.getPositive(Timer.class), workCtrlComp1.getNegative(Timer.class), Channel.TWO_WAY);

    workCtrlDriverComp1 = create(WorkCtrlDriverComp.class, new WorkCtrlDriverComp.Init(init.workCtrlAdr1));
    connect(workCtrlComp1.getPositive(WorkCtrlCenterPort.class), workCtrlDriverComp1.getNegative(WorkCtrlCenterPort.class),
      Channel.TWO_WAY);
  }
  
  private void startWorkCtrl1() {
    trigger(Start.event, workCtrlComp1.control());
    trigger(Start.event, workCtrlDriverComp1.control());
  }
  
  private WorkCtrlConfig readWorkCtrlConfig2() {
    Config config = TypesafeConfig.load(ConfigFactory.parseFile(new File(init.workCtrlConfigFile2)));
    
    Try<WorkCtrlConfig> aux = WorkCtrlConfig.instance(config);
    if(aux.isSuccess()) {
      logger.info("ctrl2 config:{}", aux.get());
      return aux.get();
    } else {
      throw new RuntimeException(TryHelper.tryError(aux));
    }
  }

  private void connectWorkCtrl2() {
    WorkCtrlConfig workMngrConfig = readWorkCtrlConfig2();
    ConnConfig connConfig = new ConnConfig(workMngrConfig.updatePeriod);
    workCtrlComp2 = create(WorkCtrlCenterComp.class, new WorkCtrlCenterComp.Init(init.workCtrlAdr2,
      workMngrConfig.overlayId, workMngrConfig.batchId, workMngrConfig.baseId, connConfig, init.workMngrAdr));
    networkChannel.addChannel(init.workCtrlAdr2.getId(), workCtrlComp2.getNegative(Network.class));
    connect(timerComp.getPositive(Timer.class), workCtrlComp2.getNegative(Timer.class), Channel.TWO_WAY);

    workCtrlDriverComp2 = create(WorkCtrlDriverComp.class, new WorkCtrlDriverComp.Init(init.workCtrlAdr2));
    connect(workCtrlComp2.getPositive(WorkCtrlCenterPort.class), workCtrlDriverComp2.getNegative(WorkCtrlCenterPort.class),
      Channel.TWO_WAY);
  }
  
  private void startWorkCtrl2() {
    trigger(Start.event, workCtrlComp2.control());
    trigger(Start.event, workCtrlDriverComp2.control());
  }

  public static class Init extends se.sics.kompics.Init<HostComp> {

    public final String workMngrConfigFile;
    public final String workCtrlConfigFile1;
    public final String workCtrlConfigFile2;
    public final KAddress workMngrAdr;
    public final KAddress workCtrlAdr1;
    public final KAddress workCtrlAdr2;

    public Init(String workMngrConfigFile, String workCtrlConfigFile1, String workCtrlConfigFile2, 
      KAddress workMngrAdr, KAddress workCtrlAdr1, KAddress workCtrlAdr2) {
      this.workMngrConfigFile = workMngrConfigFile;
      this.workCtrlConfigFile1 = workCtrlConfigFile1;
      this.workCtrlConfigFile2 = workCtrlConfigFile2;
      this.workMngrAdr = workMngrAdr;
      this.workCtrlAdr1 = workCtrlAdr1;
      this.workCtrlAdr2 = workCtrlAdr2;
    }
  }
}
