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
package se.sics.dela.network.ledbat.receiver;

import se.sics.dela.network.ledbat.LedbatReceiverEvent;
import se.sics.dela.network.ledbat.LedbatReceiverPort;
import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.Handler;
import se.sics.kompics.Positive;

/**
 *
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class DriverComp extends ComponentDefinition {

  Positive<LedbatReceiverPort> ledbatPort = requires(LedbatReceiverPort.class);

  public DriverComp() {
    subscribe(handleReceived, ledbatPort);
  }

  Handler handleReceived = new Handler<LedbatReceiverEvent.Received>() {
    @Override
    public void handle(LedbatReceiverEvent.Received event) {
      logger.info("received:", event.data.getId());
    }
  };
}
