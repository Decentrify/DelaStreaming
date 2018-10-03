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
package se.sics.dela.util;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.function.Consumer;
import se.sics.kompics.ComponentProxy;
import se.sics.kompics.Handler;
import se.sics.kompics.Positive;
import se.sics.kompics.timer.CancelPeriodicTimeout;
import se.sics.kompics.timer.SchedulePeriodicTimeout;
import se.sics.kompics.timer.ScheduleTimeout;
import se.sics.kompics.timer.Timer;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class TimerProxyImpl implements TimerProxy {

  private Positive<Timer> timer;
  private ComponentProxy proxy;
  private Map<UUID, Consumer<Boolean>> periodicCallbacks = new HashMap<>();

  public TimerProxyImpl() {
  }

  @Override
  public TimerProxyImpl setup(ComponentProxy proxy) {
    this.proxy = proxy;
    proxy.getNegative(Timer.class).getPair();
    proxy.subscribe(handleTimeout, timer);
    return this;
  }

  @Override
  public UUID schedulePeriodicTimer(long delay, long period, Consumer<Boolean> callback) {
    SchedulePeriodicTimeout spt = new SchedulePeriodicTimeout(delay, period);
    Timeout t = new Timeout(spt);
    spt.setTimeoutEvent(t);
    periodicCallbacks.put(t.getTimeoutId(), callback);
    proxy.trigger(spt, timer);
    return t.getTimeoutId();
  }

  @Override
  public void cancelPeriodicTimer(UUID timeoutId) {
    CancelPeriodicTimeout cpt = new CancelPeriodicTimeout(timeoutId);
    proxy.trigger(cpt, timer);
  }

  Handler handleTimeout = new Handler<Timeout>() {
    @Override
    public void handle(Timeout t) {
      Consumer<Boolean> callback = periodicCallbacks.get(t.getTimeoutId());
      if (callback != null) {
        callback.accept(true);
      }
    }
  };

  private static class Timeout extends se.sics.kompics.timer.Timeout {

    public Timeout(SchedulePeriodicTimeout spt) {
      super(spt);
    }

    public Timeout(ScheduleTimeout st) {
      super(st);
    }
  }
}
