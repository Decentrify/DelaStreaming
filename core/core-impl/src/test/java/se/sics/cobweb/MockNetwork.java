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

import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.Handler;
import se.sics.kompics.Negative;
import se.sics.kompics.network.Msg;
import se.sics.kompics.network.Network;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class MockNetwork extends ComponentDefinition {

  private final Negative<Network> network = provides(Network.class);
  private MockNetwork pair;
  
  public MockNetwork() {
    subscribe(handleMsg, network);
  }

  public void setPair(MockNetwork pair) {
    this.pair = pair;
  }
  
  Handler handleMsg = new Handler<Msg>() {
    @Override
    public void handle(Msg msg) {
      trigger(msg, pair.network);
    }
  };
}