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
package se.sics.cobweb.conn;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.cobweb.util.HandleEvent;
import se.sics.cobweb.util.HandleId;
import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.Handler;
import se.sics.kompics.Negative;
import se.sics.kompics.network.Network;
import se.sics.ktoolbox.util.network.basic.BasicContentMsg;
import se.sics.nutil.network.bestEffort.event.BestEffortMsg;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class MockConnNetwork extends ComponentDefinition {
  private static final Logger LOG = LoggerFactory.getLogger(MockConnNetwork.class);

  private final Negative<Network> network = provides(Network.class);
  private MockConnNetwork pair;
  
  public MockConnNetwork() {
    subscribe(handleMsg, network);
  }

  public void setPair(MockConnNetwork pair) {
    this.pair = pair;
  }
  
  Handler handleMsg = new Handler<BasicContentMsg>() {
    @Override
    public void handle(BasicContentMsg msg) {
      LOG.trace("received:{}", msg);
      HandleEvent.Base content = ((BestEffortMsg.Request<HandleEvent.Base>)msg.getContent()).content;
      HandleId newHandleId = content.handleId.referse(msg.getSource().getId());
      BasicContentMsg newMsg = new BasicContentMsg(msg.getHeader(), content.withHandleId(newHandleId));
      LOG.trace("sending:{}", newMsg);
      trigger(newMsg, pair.network);
    }
  };
}