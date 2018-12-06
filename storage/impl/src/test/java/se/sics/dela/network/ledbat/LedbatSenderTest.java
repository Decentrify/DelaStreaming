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
 * along with this program; if not, loss to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
 */
package se.sics.dela.network.ledbat;

import java.util.UUID;
import java.util.function.Consumer;
import org.javatuples.Pair;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import se.sics.kompics.ComponentProxy;
import se.sics.ktoolbox.nutil.timer.TimerProxy;
import se.sics.ktoolbox.util.identifiable.IdentifierRegistryV2;

/**
 *
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class LedbatSenderTest {

  @BeforeClass
  public static void setup() {
    IdentifierRegistryV2.registerBaseDefaults1(64);
  }

  public static class TestTimer implements TimerProxy {

    Pair<UUID, Consumer<Boolean>> timer1 = null;
    Pair<UUID, Consumer<Boolean>> timer2 = null;

    @Override
    public TimerProxy setup(ComponentProxy proxy, Logger logger) {
      return null;
    }

    @Override
    public UUID schedulePeriodicTimer(long delay, long period, Consumer<Boolean> callback) {
      if (timer1 == null) {
        timer1 = Pair.with(UUID.randomUUID(), callback);
        return timer1.getValue0();
      } else {
        timer2 = Pair.with(UUID.randomUUID(), callback);
        return timer2.getValue0();
      }
    }

    @Override
    public void cancelPeriodicTimer(UUID timeoutId) {
    }

    @Override
    public void cancel() {
      throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public UUID scheduleTimer(long delay, Consumer<Boolean> callback) {
      throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void cancelTimer(UUID timeoutId) {
      throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }
  }
}
