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
package se.sics.cobweb.transfer.handle.event;

import se.sics.cobweb.util.HandleEvent;
import se.sics.cobweb.util.HandleId;
import se.sics.kompics.timer.SchedulePeriodicTimeout;
import se.sics.ktoolbox.util.identifiable.BasicIdentifiers;
import se.sics.ktoolbox.util.identifiable.overlay.OverlayId;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class HandleT {

  public static class Advance extends HandleEvent.Timeout {

    public Advance(SchedulePeriodicTimeout st, OverlayId torrentId, HandleId handleId) {
      super(st, BasicIdentifiers.eventId(), torrentId, handleId);
    }
  }

  public static class Report extends HandleEvent.Timeout {

    public Report(SchedulePeriodicTimeout spt, OverlayId torrentId, HandleId handleId) {
      super(spt, BasicIdentifiers.eventId(), torrentId, handleId);
    }
  }
}
