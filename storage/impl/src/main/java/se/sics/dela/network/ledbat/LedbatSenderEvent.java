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

import se.sics.kompics.Direct;
import se.sics.kompics.util.Identifiable;
import se.sics.kompics.util.Identifier;
import se.sics.ktoolbox.util.identifiable.BasicIdentifiers;

/**
 *
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class LedbatSenderEvent {

  public static class Request<D extends Identifiable> extends Direct.Request<Indication> implements Identifiable {

    public final Identifier eventId;
    public final D data;

    public Request(D data) {
      this.eventId = BasicIdentifiers.eventId();
      this.data = data;
    }

    @Override
    public Identifier getId() {
      return eventId;
    }

    public Acked ack() {
      return new Acked(this);
    }
    
    public Timeout timeout() {
      return new Timeout(this);
    }
  }

  public static class Indication<D extends Identifiable> implements Direct.Response, Identifiable {
    public final Request<D> req;
    
    public Indication(Request<D> req) {
      this.req = req;
    }
    
    @Override
    public Identifier getId() {
      return req.getId();
    }
  }

  public static class Acked<D extends Identifiable> extends Indication<D> {

    public Acked(Request<D> req) {
      super(req);
    }
  }

  public static class Timeout<D extends Identifiable> extends Indication<D> {

    public Timeout(Request<D> req) {
      super(req);
    }
  }
}
