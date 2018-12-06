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
package se.sics.dela.conn.filepeer.neg.ctrl;

import java.util.function.Consumer;
import se.sics.dela.conn.filepeer.neg.KFilePeerConnView;
import se.sics.kompics.ComponentProxy;
import se.sics.kompics.Handler;
import se.sics.kompics.Negative;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class KFilePeerConnCtrlNegProxy implements FilePeerConnCtrlNegProxy {

  private ComponentProxy proxy;
  private Negative<FilePeerConnCtrlPort> port;

  private final KFilePeerConnView fp;
  private boolean waitingProvisionAck = false;

  public KFilePeerConnCtrlNegProxy(KFilePeerConnView fp) {
    this.fp = fp;
  }

  public void setup(ComponentProxy proxy) {
    this.proxy = proxy;
    port = proxy.getPositive(FilePeerConnCtrlPort.class).getPair();
    proxy.subscribe(handleProvision, port);
  }

  Handler handleProvision = new Handler<FilePeerConnCtrlEvents.Provision>() {
    @Override
    public void handle(FilePeerConnCtrlEvents.Provision req) {
      waitingProvisionAck = true;
      fp.updateProvisionedSlots(req.slots, provisionAck(req));
    }
  };

  private Consumer<Boolean> provisionAck(FilePeerConnCtrlEvents.Provision req) {
    return (_ignore) -> {
      waitingProvisionAck = false;
      proxy.answer(req, req.ack());
    };
  }
}
