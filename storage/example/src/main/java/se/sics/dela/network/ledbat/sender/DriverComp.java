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
package se.sics.dela.network.ledbat.sender;

import java.util.Random;
import se.sics.dela.network.ledbat.LedbatSenderEvent;
import se.sics.dela.network.ledbat.LedbatSenderPort;
import se.sics.dela.network.ledbat.util.LedbatContainer;
import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.Handler;
import se.sics.kompics.Positive;
import se.sics.kompics.Start;
import se.sics.ktoolbox.util.identifiable.BasicIdentifiers;

/**
 *
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class DriverComp extends ComponentDefinition {

  Positive<LedbatSenderPort> ledbatPort = requires(LedbatSenderPort.class);

  private Random rand = new Random(123);
  private int acked = 0;

  public DriverComp() {
    subscribe(handleStart, control);
    subscribe(handleAcked, ledbatPort);
    subscribe(handleTimeout, ledbatPort);
  }

  Handler handleStart = new Handler<Start>() {
    @Override
    public void handle(Start event) {
      trySend();
    }
  };

  Handler handleAcked = new Handler<LedbatSenderEvent.Acked>() {
    @Override
    public void handle(LedbatSenderEvent.Acked event) {
      logger.info("received:", event.req.data.getId());
      acked++;
    }
  };
  
  Handler handleTimeout = new Handler<LedbatSenderEvent.Timeout>() {
    @Override
    public void handle(LedbatSenderEvent.Timeout event) {
      logger.info("timeout:", event.req.data.getId());
    }
  };

  private void trySend() {
    if (acked < 100) {
      byte[] dataBytes = new byte[1024];
      rand.nextBytes(dataBytes);
      LedbatContainer dataContainer = new LedbatContainer(BasicIdentifiers.nodeId(), dataBytes);
      LedbatSenderEvent.Request req = new LedbatSenderEvent.Request(dataContainer);
      trigger(req, ledbatPort);
    }
  }
}
