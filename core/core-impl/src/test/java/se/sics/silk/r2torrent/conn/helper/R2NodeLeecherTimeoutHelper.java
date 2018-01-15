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
package se.sics.silk.r2torrent.conn.helper;

import com.google.common.base.Predicate;
import se.sics.kompics.Port;
import se.sics.kompics.testing.Direction;
import se.sics.kompics.testing.TestContext;
import se.sics.kompics.timer.CancelPeriodicTimeout;
import se.sics.kompics.timer.SchedulePeriodicTimeout;
import se.sics.ktoolbox.util.network.KAddress;
import se.sics.silk.PredicateHelper.NodeEPredicate;
import se.sics.silk.PredicateHelper.PeriodicTimerPredicate;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class R2NodeLeecherTimeoutHelper {
  public static TestContext nodeLeecherSetTimer(TestContext tc, Port timerP, KAddress leecher) {
    Predicate p = new PeriodicTimerPredicate(new NodeEPredicate(leecher.getId()));
    return tc.expect(SchedulePeriodicTimeout.class, p, timerP, Direction.OUT);
  }
  
  public static TestContext nodeLeecherCancelTimer(TestContext tc, Port timerP) {
    return tc.expect(CancelPeriodicTimeout.class, timerP, Direction.OUT);
  }
}
