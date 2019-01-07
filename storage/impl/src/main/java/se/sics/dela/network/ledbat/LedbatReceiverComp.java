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

import java.util.Optional;
import se.sics.kompics.ClassMatchedHandler;
import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.Handler;
import se.sics.kompics.Negative;
import se.sics.kompics.Positive;
import se.sics.kompics.Start;
import se.sics.kompics.network.Network;
import se.sics.kompics.util.Identifier;
import se.sics.ktoolbox.util.identifiable.BasicIdentifiers;
import se.sics.ktoolbox.util.identifiable.IdentifierFactory;
import se.sics.ktoolbox.util.identifiable.IdentifierRegistryV2;
import se.sics.ktoolbox.util.network.KAddress;
import se.sics.ktoolbox.util.network.KContentMsg;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class LedbatReceiverComp extends ComponentDefinition {

  Negative<LedbatReceiverPort> appPort = provides(LedbatReceiverPort.class);
  Positive<Network> networkPort = requires(Network.class);

  private final KAddress selfAdr;
  private final KAddress srcAdr;
  private final Identifier connId;
  
  private final IdentifierFactory eventIds;

  public LedbatReceiverComp(Init init) {
    this.selfAdr = init.selfAdr;
    this.srcAdr = init.srcAdr;
    this.connId = init.connId;
    loggingCtxPutAlways("dstId", init.selfAdr.getId().toString());
    loggingCtxPutAlways("srcId", init.srcAdr.getId().toString());
    this.eventIds = IdentifierRegistryV2.instance(BasicIdentifiers.Values.EVENT, Optional.of(1234l));
    subscribe(handleStart, control);
    subscribe(handleData, networkPort);
  }

  Handler handleStart = new Handler<Start>() {
    @Override
    public void handle(Start event) {
    }
  };

  @Override
  public void tearDown() {
  }
  
  ClassMatchedHandler handleData
    = new ClassMatchedHandler<LedbatMsg.Data, KContentMsg<?, ?, LedbatMsg.Data>>() {

      @Override
      public void handle(LedbatMsg.Data content, KContentMsg<?, ?, LedbatMsg.Data> msg) {
        answerNetwork(msg);
        answerApp(content);
      }
    };
  
  private void answerNetwork(KContentMsg<?, ?, LedbatMsg.Data> msg) {
    KContentMsg<?, ?, LedbatMsg.Ack> resp = msg.answer(msg.getContent().answer());
    trigger(resp, networkPort);
  }
  
  private void answerApp(LedbatMsg.Data content) {
    trigger(new LedbatReceiverEvent.Received(eventIds.randomId(), connId, content.data), appPort);
  }

  public static class Init extends se.sics.kompics.Init<LedbatReceiverComp> {
    public final KAddress selfAdr;
    public final KAddress srcAdr;
    public final Identifier connId;

    public Init(KAddress selfAdr, KAddress srcAdr, Identifier connId) {
      this.selfAdr = selfAdr;
      this.srcAdr = srcAdr;
      this.connId = connId;
    }
  }
}
