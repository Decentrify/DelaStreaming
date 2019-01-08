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
package se.sics.dela.network;

import se.sics.dela.network.ledbat.LedbatMsg;
import se.sics.dela.network.ledbat.LedbatSerializerSetup;
import static se.sics.dela.network.ledbat.LedbatSerializerSetup.maxSerializers;
import se.sics.dela.network.ledbat.msg.LedbatMsgSerializer;
import se.sics.dela.network.ledbat.util.LedbatContainer;
import se.sics.dela.network.util.DatumId;
import se.sics.dela.network.util.DatumIdSerializer;
import se.sics.kompics.network.netty.serialization.Serializers;
import se.sics.ktoolbox.util.setup.BasicSerializerSetup;

/**
 *
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class DelaStorageSerializerSetup {

  public static int maxSerializers = 10;
  public static int serializerIds = 1;

  public static enum DelaSerializers {
    DatumId(DatumId.class, "delaDatumId");

    public final Class serializedClass;
    public final String serializerName;

    private DelaSerializers(Class serializedClass, String serializerName) {
      this.serializedClass = serializedClass;
      this.serializerName = serializerName;
    }
  }

  public static boolean checkSetup() {
    if (!BasicSerializerSetup.checkSetup()) {
      return false;
    }
    for (DelaSerializers ds : DelaSerializers.values()) {
      if (Serializers.lookupSerializer(ds.serializedClass) == null) {
        return false;
      }
    }
    if (!LedbatSerializerSetup.checkSetup()) {
      return false;
    }
    return true;
  }

  public static int registerSerializers(int startingId) {
    int currentId = startingId;

    Serializers.register(new DatumIdSerializer(currentId++), DelaSerializers.DatumId.serializerName);
    Serializers.register(DelaSerializers.DatumId.serializedClass, DelaSerializers.DatumId.serializerName);

    currentId += LedbatSerializerSetup.registerSerializers(currentId);
    assert startingId + maxSerializers == currentId;
    assert serializerIds + LedbatSerializerSetup.maxSerializers <= maxSerializers;
    return startingId + maxSerializers;
  }
}
