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
package se.sics.dela.conn.filepeer.pos;

import java.util.Optional;
import java.util.function.Consumer;
import org.javatuples.Pair;
import se.sics.dela.conn.filepeer.neg.ctrl.FilePeerConnCtrlEvents;
import se.sics.dela.conn.filepeer.neg.ctrl.FilePeerConnCtrlPort;
import se.sics.kompics.ComponentProxy;
import se.sics.kompics.Handler;
import se.sics.kompics.Positive;
import se.sics.kompics.util.Identifier;
import se.sics.ktoolbox.util.identifiable.IdentifierFactory;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class KFilePeerConnPosProxy implements FilePeerConnPosProxy {

  private final Identifier fileId;
  private final Identifier peerId;
  private final IdentifierFactory eventIds;
  
  private ComponentProxy proxy;
  private Positive<FilePeerConnCtrlPort> port;
  
  private Pair<Integer, Integer> provisionedSlots = Pair.with(0, 0);
  private Optional<Consumer<Boolean>> provisionAck = Optional.empty();

  public KFilePeerConnPosProxy(Identifier fileId, Identifier peerId, IdentifierFactory eventIds) {
    this.fileId = fileId;
    this.peerId = peerId;
    this.eventIds = eventIds;
  }

  public void setup(ComponentProxy proxy) {
    this.proxy = proxy;
    port = proxy.getNegative(FilePeerConnCtrlPort.class).getPair();
    proxy.subscribe(handleProvisionAck, port);
  }

  @Override
  public void provision(int slots, Consumer<Boolean> callback) {
    provisionedSlots.setAt1(slots);
    provisionAck = Optional.of(callback);
    proxy.trigger(new FilePeerConnCtrlEvents.Provision(eventIds.randomId(), fileId, peerId, slots), port);
  }

  @Override
  public boolean waitingProvisionAck() {
    return provisionAck.isPresent();
  }
  
  @Override
  public int provisioned() {
    return Math.max(provisionedSlots.getValue0(), provisionedSlots.getValue1());
  }
  
  Handler handleProvisionAck = new Handler<FilePeerConnCtrlEvents.ProvisionAck>() {
    @Override
    public void handle(FilePeerConnCtrlEvents.ProvisionAck resp) {
      provisionedSlots.setAt0(resp.req.slots);
      provisionAck.get().accept(true);
      provisionAck = Optional.empty();
    }
  };

}
