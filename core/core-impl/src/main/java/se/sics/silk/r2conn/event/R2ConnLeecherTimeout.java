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
package se.sics.silk.r2conn.event;

import se.sics.kompics.timer.SchedulePeriodicTimeout;
import se.sics.kompics.timer.Timeout;
import se.sics.kompics.util.Identifier;
import se.sics.ktoolbox.util.identifiable.BasicIdentifiers;
import se.sics.silk.r2torrent.R2NodeLeecher;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class R2ConnLeecherTimeout extends Timeout implements R2NodeLeecher.Event {
  private final Identifier eventId;
  private final Identifier leecherId;
  public R2ConnLeecherTimeout(SchedulePeriodicTimeout spt, Identifier seederId) {
    super(spt);
    this.eventId = BasicIdentifiers.eventId();
    this.leecherId = seederId;
  }

  @Override
  public Identifier getId() {
    return eventId;
  }
  
  @Override
  public Identifier getConnLeecherFSMId() {
    return leecherId;
  }
}
