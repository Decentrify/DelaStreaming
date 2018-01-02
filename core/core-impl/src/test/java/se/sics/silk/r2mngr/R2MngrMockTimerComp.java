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

import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.Handler;
import se.sics.kompics.Negative;
import se.sics.kompics.timer.CancelPeriodicTimeout;
import se.sics.kompics.timer.SchedulePeriodicTimeout;
import se.sics.kompics.timer.Timeout;
import se.sics.kompics.timer.Timer;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class R2MngrMockTimerComp extends ComponentDefinition {
  Negative<Timer> timer = provides(Timer.class);
  Negative<R2MngrWrapperComp.Port> timerTrigger = provides(R2MngrWrapperComp.Port.class);
  Timeout timeoutEvent;
  
  public R2MngrMockTimerComp() {
    subscribe(handlePeriodicSchedule, timer);
    subscribe(handleCancelPeriodicSchedule, timer);
    subscribe(handleTrigger, timerTrigger);
  }
  Handler handlePeriodicSchedule = new Handler<SchedulePeriodicTimeout>() {
    @Override
    public void handle(SchedulePeriodicTimeout timeout) {
      timeoutEvent = timeout.getTimeoutEvent();
    }
  };
  
  Handler handleCancelPeriodicSchedule = new Handler<CancelPeriodicTimeout>() {
    @Override
    public void handle(CancelPeriodicTimeout timeout) {
      timeoutEvent = null;
    }
  };

  Handler handleTrigger = new Handler<R2MngrWrapperComp.TriggerTimeout>() {
    @Override
    public void handle(R2MngrWrapperComp.TriggerTimeout event) {
      trigger(timeoutEvent, timer);
    }
  };
}
