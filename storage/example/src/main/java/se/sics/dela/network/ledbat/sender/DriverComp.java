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
import se.sics.kompics.util.Identifier;
import se.sics.ktoolbox.util.identifiable.BasicIdentifiers;

/**
 *
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class DriverComp extends ComponentDefinition {

  Positive<LedbatSenderPort> ledbat = requires(LedbatSenderPort.class);

  private Random rand = new Random(123);
  private int acked = 0;

  public DriverComp() {
    subscribe(handleStart, control);
    subscribe(handleAcked, ledbat);
    subscribe(handleTimeout, ledbat);
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
      logger.debug("received:{}", event.req.data.getId());
      acked++;
      if(acked == 100) {
        logger.info("done");
      }
      trySend();
    }
  };

  Handler handleTimeout = new Handler<LedbatSenderEvent.Timeout>() {
    @Override
    public void handle(LedbatSenderEvent.Timeout event) {
      logger.debug("timeout:{}", event.req.data.getId());
      trySend();
    }
  };

  private void trySend() {
    if (acked < 100) {
      byte[] dataBytes = new byte[1024];
      rand.nextBytes(dataBytes);
      send(dataBytes);
    }
  }

  private void send(byte[] data) {
    Identifier dataId = BasicIdentifiers.eventId();
    LedbatContainer dataContainer = new LedbatContainer(dataId, data);
    LedbatSenderEvent.Request req = new LedbatSenderEvent.Request(dataContainer);
    logger.debug("sending:{}", req.data.getId());
    trigger(req, ledbat);
  }
}
