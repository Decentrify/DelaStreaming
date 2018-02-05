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
package se.sics.silk.r2torrent.torrent;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.Handler;
import se.sics.kompics.Negative;
import se.sics.kompics.Positive;
import se.sics.kompics.Start;
import se.sics.kompics.timer.CancelPeriodicTimeout;
import se.sics.kompics.timer.SchedulePeriodicTimeout;
import se.sics.kompics.timer.Timeout;
import se.sics.kompics.timer.Timer;
import se.sics.ktoolbox.util.identifiable.overlay.OverlayId;
import se.sics.ktoolbox.util.network.KAddress;
import se.sics.silk.r2torrent.torrent.event.R1TorrentConnEvents;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class R1TorrentConnComp extends ComponentDefinition {

  final Positive<Timer> timer = requires(Timer.class);
  final Negative<R1TorrentConnPort> connPort = provides(R1TorrentConnPort.class);
  private UUID timerId;

  private Map<OverlayId, Set<KAddress>> bootstrap = new HashMap<>();

  public R1TorrentConnComp(Init init) {
    subscribe(handleStart, control);
    subscribe(handleConnTimeout, timer);
    subscribe(handleConnBootstrap, connPort);
  }

  Handler handleStart = new Handler<Start>() {

    @Override
    public void handle(Start event) {
      scheduleConnTimer();
    }
  };
  
  @Override
  public void tearDown() {
    cancelConnTimer();
  }

  Handler handleConnTimeout = new Handler<ConnTimeout>() {

    @Override
    public void handle(ConnTimeout event) {
      for (Map.Entry<OverlayId, Set<KAddress>> e : bootstrap.entrySet()) {
        trigger(new R1TorrentConnEvents.Seeders(e.getKey(), e.getValue()), connPort);
      }
    }
  };

  Handler handleConnBootstrap = new Handler<R1TorrentConnEvents.Bootstrap>() {

    @Override
    public void handle(R1TorrentConnEvents.Bootstrap event) {
      bootstrap.put(event.torrentId, event.boostrap);
      trigger(new R1TorrentConnEvents.Seeders(event.torrentId, event.boostrap), connPort);
    }
  };

  private void scheduleConnTimer() {
    SchedulePeriodicTimeout spt
      = new SchedulePeriodicTimeout(HardCodedConfig.timerPeriod, HardCodedConfig.timerPeriod);
    ConnTimeout ct = new ConnTimeout(spt);
    timerId = ct.getTimeoutId();
    spt.setTimeoutEvent(ct);
    trigger(spt, timer);
  }

  private void cancelConnTimer() {
    if (timerId != null) {
      CancelPeriodicTimeout cpt = new CancelPeriodicTimeout(timerId);
      trigger(cpt, timer);
      timerId = null;
    }
  }

  public static class Init extends se.sics.kompics.Init<R1TorrentConnComp> {
  }

  public static class HardCodedConfig {

    public static long timerPeriod = 1000;
  }

  public static class ConnTimeout extends Timeout {

    public ConnTimeout(SchedulePeriodicTimeout spt) {
      super(spt);
    }
  }
}
