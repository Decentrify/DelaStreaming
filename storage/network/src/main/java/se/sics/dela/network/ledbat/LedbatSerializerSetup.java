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

import se.sics.dela.network.ledbat.msg.LedbatMsgSerializer;
import se.sics.kompics.network.netty.serialization.Serializers;
import se.sics.ktoolbox.util.setup.BasicSerializerSetup;

/**
 *
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class LedbatSerializerSetup {

  public static int maxSerializers = 5;
  public static int serializerIds = 2;

  public static enum LedbatSerializers {
    LedbatMsgData(LedbatMsg.Data.class, "delaLedbatMsgData"),
    LedbatMsgAck(LedbatMsg.Ack.class, "delaLedbatMsgAck");

    public final Class serializedClass;
    public final String serializerName;

    private LedbatSerializers(Class serializedClass, String serializerName) {
      this.serializedClass = serializedClass;
      this.serializerName = serializerName;
    }
  }

  public static boolean checkSetup() {
    for (LedbatSerializers ls : LedbatSerializers.values()) {
      if (Serializers.lookupSerializer(ls.serializedClass) == null) {
        return false;
      }
    }
    if (!BasicSerializerSetup.checkSetup()) {
      return false;
    }
    return true;
  }

  public static int registerSerializers(int startingId) {
    int currentId = startingId;

    LedbatMsgSerializer.Data ledbatMsgDataSerializer = new LedbatMsgSerializer.Data(currentId++);
    Serializers.register(ledbatMsgDataSerializer, LedbatSerializers.LedbatMsgData.serializerName);
    Serializers.register(LedbatSerializers.LedbatMsgData.serializedClass,
      LedbatSerializers.LedbatMsgData.serializerName);

    LedbatMsgSerializer.Ack ledbatMsgAckSerializer = new LedbatMsgSerializer.Ack(currentId++);
    Serializers.register(ledbatMsgAckSerializer, LedbatSerializers.LedbatMsgAck.serializerName);
    Serializers.register(LedbatSerializers.LedbatMsgAck.serializedClass,
      LedbatSerializers.LedbatMsgAck.serializerName);

    assert startingId + serializerIds == currentId;
    assert serializerIds <= maxSerializers;
    return startingId + maxSerializers;
  }
}
