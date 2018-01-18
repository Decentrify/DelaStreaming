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
package se.sics.silk.mocktimer;

import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.Handler;
import se.sics.kompics.Negative;
import se.sics.kompics.network.Msg;
import se.sics.kompics.network.Network;
import se.sics.ktoolbox.util.network.KAddress;
import se.sics.ktoolbox.util.network.basic.BasicContentMsg;
import se.sics.nutil.network.bestEffort.event.BestEffortMsg;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class MockNetworkComp extends ComponentDefinition {

  Negative<Network> network = provides(Network.class);
  KAddress adr1;
  KAddress adr2;

  public MockNetworkComp(Init init) {
    this.adr1 = init.adr1;
    this.adr2 = init.adr2;

    subscribe(handleMsg, network);
  }

  Handler handleMsg = new Handler<Msg>() {
    @Override
    public void handle(Msg m) {
      if(!(m instanceof BasicContentMsg)) {
        throw new RuntimeException("bad msg");
      }
      BasicContentMsg msg = (BasicContentMsg)m;
      BasicContentMsg sendMsg;
      if(msg.getContent() instanceof BestEffortMsg.Request) {
        BestEffortMsg.Request wrapper = (BestEffortMsg.Request)msg.getContent();
        sendMsg = new BasicContentMsg(msg.getHeader(), wrapper.content);
      } else {
        sendMsg = msg;
      }
      trigger(sendMsg, network);
    }
  };

  public static class Init extends se.sics.kompics.Init<MockNetworkComp> {

    KAddress adr1;
    KAddress adr2;

    public Init(KAddress adr1, KAddress adr2) {
      this.adr1 = adr1;
      this.adr2 = adr2;
    }
  }
}
