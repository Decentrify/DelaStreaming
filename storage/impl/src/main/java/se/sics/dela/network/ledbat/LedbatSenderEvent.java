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

import se.sics.dela.network.util.DatumId;
import se.sics.kompics.Direct;
import se.sics.kompics.util.Identifiable;
import se.sics.kompics.util.Identifier;

/**
 *
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class LedbatSenderEvent {

  public static class Request<D extends Identifiable<DatumId>>
    extends Direct.Request<Indication> implements LedbatEvent {

    public final Identifier eventId;
    public final Identifier rivuletId;
    public final D datum;

    public Request(Identifier eventId, Identifier rivuletId, D datum) {
      this.eventId = eventId;
      this.rivuletId = rivuletId;
      this.datum = datum;
    }

    @Override
    public Identifier getId() {
      return eventId;
    }

    public Acked ack(int maxInFlight) {
      return new Acked(this, maxInFlight);
    }

    public Timeout timeout(int maxInFlight) {
      return new Timeout(this, maxInFlight);
    }

    @Override
    public String toString() {
      return "LedbatSenderRequest{" + "data=" + datum.getId() + "," + rivuletId.toString() +'}';
    }

    @Override
    public Identifier rivuletId() {
      return rivuletId;
    }

    @Override
    public Identifier dataId() {
      return datum.getId().dataId();
    }

    @Override
    public String eventType() {
      return LedbatEvent.EVENT_TYPE;
    }
  }

  public static class Indication<D extends Identifiable<DatumId>> 
    extends LedbatEvent.Basic implements Direct.Response {

    public final Request<D> req;
    public final int maxInFlight;

    public Indication(Request<D> req, int maxInFlight) {
      super(req.eventId, req.rivuletId, req.dataId());
      this.req = req;
      this.maxInFlight = maxInFlight;
    }
  }

  public static class Acked<D extends Identifiable<DatumId>> extends Indication<D> {

    public Acked(Request<D> req, int maxInFlight) {
      super(req, maxInFlight);
    }

    @Override
    public String toString() {
      return "LedbatSenderAck{" + "data=" + req.datum.getId() + '}';
    }
  }

  public static class Timeout<D extends Identifiable<DatumId>> extends Indication<D> {

    public Timeout(Request<D> req, int maxInFlight) {
      super(req, maxInFlight);
    }

    @Override
    public String toString() {
      return "LedbatSenderTimeout{" + "data=" + req.datum.getId() + '}';
    }
  }

  public static class Overloaded<D extends Identifiable<DatumId>> extends Indication<D> {

    public Overloaded(Request<D> req, int maxInFlight) {
      super(req, maxInFlight);
    }

    @Override
    public String toString() {
      return "LedbatSenderOverloaded{" + "data=" + req.datum.getId() + '}';
    }
  }
}
