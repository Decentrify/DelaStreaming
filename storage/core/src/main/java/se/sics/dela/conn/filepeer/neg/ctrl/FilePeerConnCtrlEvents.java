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

import se.sics.kompics.Direct;
import se.sics.kompics.util.Identifiable;
import se.sics.kompics.util.Identifier;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class FilePeerConnCtrlEvents {

  public static class Provision extends Direct.Request implements Identifiable {

    public final Identifier eventId;
    public final Identifier fileId;
    public final Identifier peerId;
    public final int slots;

    public Provision(Identifier eventId, Identifier fileId, Identifier peerId, int slots) {
      super();
      this.eventId = eventId;
      this.fileId = fileId;
      this.peerId = peerId;
      this.slots = slots;
    }

    @Override
    public Identifier getId() {
      return eventId;
    }

    @Override
    public String toString() {
      return "Provision{" + "eventId=" + eventId + ", fileId=" + fileId + ", peerId=" + peerId + '}';
    }
    
    public ProvisionAck ack() {
      return new ProvisionAck(this);
    }
  }

  public static class ProvisionAck implements Direct.Response, Identifiable {
    public final Provision req;
    
    public ProvisionAck(Provision req) {
      this.req = req;
    }

    @Override
    public Identifier getId() {
      return req.getId();
    }

    @Override
    public String toString() {
      return "Provision{" + "eventId=" + req.eventId + ", fileId=" + req.fileId + ", peerId=" + req.peerId + '}';
    }
  }
}
