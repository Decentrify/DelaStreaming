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
import java.util.Optional;
import java.util.function.Consumer;
import se.sics.kompics.Channel;
import se.sics.kompics.Component;
import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.Handler;
import se.sics.kompics.Init;
import se.sics.kompics.Start;
import se.sics.kompics.timer.Timer;
import se.sics.kompics.timer.java.JavaTimer;
import se.sics.ktoolbox.netmngr.NetworkConfig;
import se.sics.ktoolbox.netmngr.driver.NxNetProxy;
import se.sics.ktoolbox.nutil.conn.ConnConfig;
import se.sics.ktoolbox.nutil.conn.workers.WorkCtrlCenterComp;
import se.sics.ktoolbox.nutil.conn.workers.WorkCtrlCenterPort;
import se.sics.ktoolbox.util.config.impl.SystemConfig;
import se.sics.ktoolbox.util.identifiable.BasicIdentifiers;
import se.sics.ktoolbox.util.identifiable.IdentifierFactory;
import se.sics.ktoolbox.util.identifiable.IdentifierRegistryV2;
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

  private SystemConfig systemConfig;
  private NetworkConfig networkConfig;
  private WorkCtrlConfig ctrlConfig;
  private KAddress selfAdr;

  private IdentifierFactory eventIds;
  public WorkCtrlHostComp() {
    eventIds = IdentifierRegistryV2.instance(BasicIdentifiers.Values.EVENT, Optional.of(1234l));
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
      
      trigger(Start.event, ctrlCenter.control());
      trigger(Start.event, ctrlDriver.control());
    };
  }
}
