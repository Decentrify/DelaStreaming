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
package se.sics.dela.workers.ctrl.util;

import se.sics.dela.network.ledbat.LedbatReceiverEvent;
import se.sics.dela.network.ledbat.LedbatReceiverPort;
import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.Handler;
import se.sics.kompics.Positive;
import se.sics.kompics.Start;
import se.sics.kompics.timer.Timer;
import se.sics.kompics.util.Identifier;
import se.sics.ktoolbox.util.network.KAddress;

/**
 *
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class ReceiverTaskComp extends ComponentDefinition {

  private final Positive<Timer> timerPort = requires(Timer.class);
  private final Positive<LedbatReceiverPort> ledbatPort = requires(LedbatReceiverPort.class);

  private final KAddress selfAdr;
  public final KAddress senderAdr;

  public ReceiverTaskComp(Init init) {
    this.selfAdr = init.selfAdr;
    this.senderAdr = init.senderAdr;

    subscribe(handleStart, control);
    subscribe(handleReceived, ledbatPort);
  }

  Handler handleStart = new Handler<Start>() {
    @Override
    public void handle(Start event) {
      logger.info("starting transfer sender:{} receiver:{}", senderAdr, selfAdr);
      
    }
  };

  Handler handleReceived = new Handler<LedbatReceiverEvent.Received>() {
    @Override
    public void handle(LedbatReceiverEvent.Received event) {
      logger.debug("received:", event.data.getId());
    }
  };

  public static class Init extends se.sics.kompics.Init<ReceiverTaskComp> {

    public final KAddress selfAdr;
    public final KAddress senderAdr;
    public final Identifier dataId;
    public final Identifier rivuletId;

    public Init(KAddress selfAdr, KAddress senderAdr, Identifier dataId, Identifier rivuletId) {
      this.selfAdr = selfAdr;
      this.senderAdr = senderAdr;
      this.dataId = dataId;
      this.rivuletId = rivuletId;
    }
  }
}
