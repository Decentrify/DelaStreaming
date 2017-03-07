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
package se.sics.cobweb;

import com.google.common.base.Predicate;
import se.sics.fsm.helper.FSMHelper;
import se.sics.kompics.Component;
import se.sics.kompics.KompicsEvent;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class TestHelper {
  public static <K extends KompicsEvent> Predicate<K> anyPredicate(Class<K> msgType) {
    return new Predicate<K>() {
      @Override
      public boolean apply(K t) {
        return true;
      }
    };
  }
  
  public static <K extends KompicsEvent> Predicate<K> anyAndNext(Component driver, Class<K> eventType) {
    return FSMHelper.anyEventAndPredicate(eventType, driverNext(driver));
  } 
  
  public static Predicate<Boolean> driverNext(final Component driver) {
    return new Predicate<Boolean>() {
      @Override
      public boolean apply(Boolean t) {
        ((MockScenarioComp) driver.getComponent()).next();
        return true;
      }
    };
  }
}
