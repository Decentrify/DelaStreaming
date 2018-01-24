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
package se.sics.silk.util;

import org.junit.Assert;
import org.junit.Test;
import se.sics.nstream.tracker.ComponentTracker;
import se.sics.nstream.tracker.IncompleteTracker;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class IncompleteTrackerTest {

  @Test
  public void test1() {
    ComponentTracker tracker = IncompleteTracker.create(6, 0);
    Assert.assertEquals(0, tracker.nextComponentMissing(0));
    tracker.addComponent(3);
    tracker.addComponent(4);
    Assert.assertEquals(5, tracker.nextComponentMissing(3));
    tracker.addComponent(0);
    tracker.addComponent(1);
    tracker.addComponent(2);
    Assert.assertEquals(5, tracker.nextComponentMissing(0));
    Assert.assertFalse(tracker.isComplete());
    tracker.addComponent(5);
    Assert.assertEquals(-1, tracker.nextComponentMissing(0));
    Assert.assertTrue(tracker.isComplete());
  }
}
