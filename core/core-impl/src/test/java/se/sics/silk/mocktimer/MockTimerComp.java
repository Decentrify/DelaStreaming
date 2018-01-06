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
package se.sics.silk.mocktimer;

import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.Handler;
import se.sics.kompics.KompicsEvent;
import se.sics.kompics.Negative;
import se.sics.kompics.PortType;
import se.sics.kompics.timer.CancelPeriodicTimeout;
import se.sics.kompics.timer.SchedulePeriodicTimeout;
import se.sics.kompics.timer.Timeout;
import se.sics.kompics.timer.Timer;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class MockTimerComp extends ComponentDefinition {
  Negative<Timer> timer = provides(Timer.class);
  Negative<Port> timerTrigger = provides(Port.class);
  Timeout timeoutEvent;
  
  public MockTimerComp() {
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
    }
  };

  Handler handleTrigger = new Handler<TriggerTimeout>() {
    @Override
    public void handle(TriggerTimeout event) {
      trigger(timeoutEvent, timer);
    }
  };
  
  public static class Port extends PortType {
    {
      request(TriggerTimeout.class);
    }
  }
  
  public static class TriggerTimeout implements KompicsEvent {
  }
}
