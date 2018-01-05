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
package se.sics.silk.r2mngr;

import se.sics.kompics.Channel;
import se.sics.kompics.Component;
import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.Handler;
import se.sics.kompics.KompicsEvent;
import se.sics.kompics.Negative;
import se.sics.kompics.PortType;
import se.sics.kompics.Positive;
import se.sics.kompics.Start;
import se.sics.kompics.fsm.FSMInternalState;
import se.sics.kompics.fsm.FSMStateName;
import se.sics.kompics.network.Network;
import se.sics.kompics.timer.Timer;
import se.sics.kompics.util.Identifier;
import se.sics.ktoolbox.util.network.KAddress;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class R2MngrWrapperComp extends ComponentDefinition {

  Negative<ConnSeederPort> conn = provides(ConnSeederPort.class);
  Positive<Network> network = requires(Network.class);
  Negative<Port> timerTrigger = provides(Port.class);

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
      R2MngrComp.Init init = new R2MngrComp.Init(selfAdr);
      r2MngrComp = create(R2MngrComp.class, init);
      timerComp = create(R2MngrMockTimerComp.class, Init.NONE);
      connect(r2MngrComp.getPositive(ConnSeederPort.class), conn, Channel.TWO_WAY);
      connect(r2MngrComp.getNegative(Network.class), network, Channel.TWO_WAY);
      connect(r2MngrComp.getNegative(Timer.class), timerComp.getPositive(Timer.class), Channel.TWO_WAY);
      trigger(Start.event, r2MngrComp.control());
      trigger(Start.event, timerComp.control());
    }
  };
  
  Handler handleTrigger = new Handler<R2MngrWrapperComp.TriggerTimeout>() {
    @Override
    public void handle(R2MngrWrapperComp.TriggerTimeout event) {
      trigger(event, timerComp.getPositive(Port.class));
    }
  };

  //******************************************TESTING HELPERS***********************************************************
  FSMInternalState getConnSeederIS(Identifier seederId) {
    return ((R2MngrComp) r2MngrComp.getComponent()).getConnSeederIS(seederId);
  }

  FSMStateName getConnSeederState(Identifier seederId) {
    return ((R2MngrComp) r2MngrComp.getComponent()).getConnSeederState(seederId);
  }

  public boolean activeSeederFSM(Identifier baseId) {
    return ((R2MngrComp) r2MngrComp.getComponent()).activeSeederFSM(baseId);
  }

  public static class Init extends se.sics.kompics.Init<R2MngrWrapperComp> {

    public final KAddress selfAdr;

    public Init(KAddress selfAdr) {
      this.selfAdr = selfAdr;
    }
  }
  
  public static class Port extends PortType {
    {
      request(TriggerTimeout.class);
    }
  }
  
  public static class TriggerTimeout implements KompicsEvent {
  }
}
