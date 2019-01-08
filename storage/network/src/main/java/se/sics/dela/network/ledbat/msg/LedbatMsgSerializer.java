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
package se.sics.dela.network.ledbat.msg;

import com.google.common.base.Optional;
import io.netty.buffer.ByteBuf;
import se.sics.dela.network.ledbat.LedbatMsg;
import se.sics.dela.network.ledbat.util.OneWayDelay;
import se.sics.dela.network.util.DatumId;
import se.sics.kompics.network.netty.serialization.Serializer;
import se.sics.kompics.network.netty.serialization.Serializers;
import se.sics.kompics.util.Identifiable;
import se.sics.kompics.util.Identifier;
import se.sics.ktoolbox.util.identifiable.BasicIdentifiers;
import se.sics.ktoolbox.util.identifiable.IdentifierRegistryV2;

/**
 *
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class LedbatMsgSerializer {

  public static class Datum implements Serializer {

    private final int id;
    private final Class msgIdType;

    public Datum(int id) {
      this.id = id;
      this.msgIdType = IdentifierRegistryV2.idType(BasicIdentifiers.Values.MSG);
    }

    @Override
    public int identifier() {
      return id;
    }

    @Override
    public void toBinary(Object o, ByteBuf buf) {
      LedbatMsg.Datum obj = (LedbatMsg.Datum) o;
      Serializers.lookupSerializer(msgIdType).toBinary(obj.getId(), buf);
      Serializers.toBinary(obj.datum, buf);
      long sendTime = System.currentTimeMillis();
      buf.writeLong(sendTime);
    }

    @Override
    public LedbatMsg.Datum fromBinary(ByteBuf buf, Optional<Object> hint) {
      Identifier msgId = (Identifier) Serializers.lookupSerializer(msgIdType).fromBinary(buf, hint);
      Identifiable data = (Identifiable) Serializers.fromBinary(buf, hint);
      long sendTime = buf.readLong();
      long receiveTime = System.currentTimeMillis();
      OneWayDelay dataDelay = new OneWayDelay(sendTime, receiveTime);
      return new LedbatMsg.Datum(msgId, data, dataDelay);
    }
  }

  public static class Ack implements Serializer {

    private final int id;
    private final Class msgIdType;

    public Ack(int id) {
      this.id = id;
      this.msgIdType = IdentifierRegistryV2.idType(BasicIdentifiers.Values.MSG);
    }

    @Override
    public int identifier() {
      return id;
    }

    @Override
    public void toBinary(Object o, ByteBuf buf) {
      LedbatMsg.Ack obj = (LedbatMsg.Ack) o;
      Serializers.lookupSerializer(msgIdType).toBinary(obj.getId(), buf);
      Serializers.lookupSerializer(DatumId.class).toBinary(obj.datumId, buf);
      buf.writeLong(obj.dataDelay.send);
      buf.writeLong(obj.dataDelay.receive);
      long sendAckTime = System.currentTimeMillis();
      buf.writeLong(sendAckTime);
    }

    @Override
    public LedbatMsg.Ack fromBinary(ByteBuf buf, Optional<Object> hint) {
      Identifier msgId = (Identifier) Serializers.lookupSerializer(msgIdType).fromBinary(buf, hint);
      DatumId datumId = (DatumId) Serializers.lookupSerializer(DatumId.class).fromBinary(buf, hint);
      long sendTime = buf.readLong();
      long receiveTime = buf.readLong();
      OneWayDelay dataDelay = new OneWayDelay(sendTime, receiveTime);
      long sendAckTime = buf.readLong();
      long receiveAckTime = System.currentTimeMillis();
      OneWayDelay ackDelay = new OneWayDelay(sendAckTime, receiveAckTime);
      return new LedbatMsg.Ack(msgId, datumId, dataDelay, ackDelay);
    }
  }
}
