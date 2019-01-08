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

import se.sics.dela.network.ledbat.util.OneWayDelay;
import se.sics.dela.network.util.DatumId;
import se.sics.kompics.util.Identifiable;
import se.sics.kompics.util.Identifier;
import se.sics.ktoolbox.nutil.network.portsv2.SelectableMsgV2;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public interface LedbatMsg extends Identifiable, SelectableMsgV2 {

  public static final String MSG_TYPE = "LEDBAT_MSG";

  public static abstract class Basic implements LedbatMsg {

    public final Identifier id;

    public Basic(Identifier id) {
      this.id = id;
    }

    @Override
    public Identifier getId() {
      return id;
    }

    @Override
    public String eventType() {
      return MSG_TYPE;
    }
  }

  public static class Datum<D extends Identifiable<DatumId>> extends Basic {

    public final D datum;
    public final OneWayDelay dataDelay;

    public Datum(Identifier msgId, D datum) {
      this(msgId, datum, new OneWayDelay());
    }

    public Datum(Identifier msgId, D datum, OneWayDelay dataDelay) {
      super(msgId);
      this.datum = datum;
      this.dataDelay = dataDelay;
    }

    public Ack answer() {
      return new Ack(id, datum.getId(), dataDelay);
    }

    @Override
    public String toString() {
      return "LedbatMsgDatum{" + "datumId=" + datum.getId() + '}';
    }
  }

  public static class Ack extends Basic {

    public final DatumId datumId;
    public final OneWayDelay dataDelay;
    public final OneWayDelay ackDelay;

    public Ack(Identifier msgId, DatumId datumId, OneWayDelay dataDelay) {
      this(msgId, datumId, dataDelay, new OneWayDelay());
    }

    public Ack(Identifier msgId, DatumId datumId, OneWayDelay dataDelay, OneWayDelay ackDelay) {
      super(msgId);
      this.datumId = datumId;
      this.dataDelay = dataDelay;
      this.ackDelay = ackDelay;
    }

    @Override
    public String toString() {
      return "LedbatMsgAck{" + "datum=" + datumId + '}';
    }
  }
}
