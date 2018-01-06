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
package se.sics.silk.r2conn;

import se.sics.kompics.Channel;
import se.sics.kompics.Component;
import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.Handler;
import se.sics.kompics.Negative;
import se.sics.kompics.Positive;
import se.sics.kompics.Start;
import se.sics.kompics.fsm.FSMInternalState;
import se.sics.kompics.fsm.FSMStateName;
import se.sics.kompics.network.Network;
import se.sics.kompics.timer.Timer;
import se.sics.kompics.util.Identifier;
import se.sics.ktoolbox.util.network.KAddress;
import se.sics.silk.mocktimer.MockTimerComp;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class R2MngrWrapperComp extends ComponentDefinition {

  Positive<Network> network = requires(Network.class);
  Negative<MockTimerComp.Port> timerTrigger = provides(MockTimerComp.Port.class);
  Negative<R2ConnSeederPort> seeders = provides(R2ConnSeederPort.class);
  Negative<R2ConnLeecherPort> leechers = provides(R2ConnLeecherPort.class);

  Component r2MngrComp;
  Component timerComp;
  KAddress selfAdr;

  public R2MngrWrapperComp(Init init) {
    this.selfAdr = init.selfAdr;
    subscribe(handleStart, control);
    subscribe(handleTrigger, timerTrigger);
  }

  Handler handleStart = new Handler<Start>() {
    @Override
    public void handle(Start event) {
      R2ConnComp.Init init = new R2ConnComp.Init(selfAdr);
      r2MngrComp = create(R2ConnComp.class, init);
      timerComp = create(MockTimerComp.class, Init.NONE);
      connect(r2MngrComp.getNegative(Network.class), network, Channel.TWO_WAY);
      connect(r2MngrComp.getNegative(Timer.class), timerComp.getPositive(Timer.class), Channel.TWO_WAY);
      connect(r2MngrComp.getPositive(R2ConnSeederPort.class), seeders, Channel.TWO_WAY);
      connect(r2MngrComp.getPositive(R2ConnLeecherPort.class), leechers, Channel.TWO_WAY);
      trigger(Start.event, r2MngrComp.control());
      trigger(Start.event, timerComp.control());
    }
  };
  
  Handler handleTrigger = new Handler<MockTimerComp.TriggerTimeout>() {
    @Override
    public void handle(MockTimerComp.TriggerTimeout event) {
      trigger(event, timerComp.getPositive(MockTimerComp.Port.class));
    }
  };

  //******************************************TESTING HELPERS***********************************************************
  public FSMInternalState getConnSeederIS(Identifier seederId) {
    return ((R2ConnComp) r2MngrComp.getComponent()).getConnSeederIS(seederId);
  }

  public FSMStateName getConnSeederState(Identifier seederId) {
    return ((R2ConnComp) r2MngrComp.getComponent()).getConnSeederState(seederId);
  }

  public boolean activeSeederFSM(Identifier baseId) {
    return ((R2ConnComp) r2MngrComp.getComponent()).activeSeederFSM(baseId);
  }
  
  public FSMStateName getConnLeecherState(Identifier seederId) {
    return ((R2ConnComp) r2MngrComp.getComponent()).getConnLeecherState(seederId);
  }
  
  public boolean activeLeecherFSM(Identifier baseId) {
    return ((R2ConnComp) r2MngrComp.getComponent()).activeLeecherFSM(baseId);
  }
  
  public static class Init extends se.sics.kompics.Init<R2MngrWrapperComp> {

    public final KAddress selfAdr;

    public Init(KAddress selfAdr) {
      this.selfAdr = selfAdr;
    }
  }
}
